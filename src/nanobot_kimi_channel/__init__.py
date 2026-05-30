"""Kimi (Moonshot) channel plugin for nanobot.

Inbound: IM RPC Subscribe (HTTP/1.1 streaming, Connect protocol)
Outbound: SendMessageStream (WebSocket to /im/send-message/ws) for streaming,
          SendMessage (unary HTTP POST) as fallback.
Auth: X-Kimi-Bot-Token header on all requests.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import httpx
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel

try:
    from nanobot.config.schema import Base as ConfigBase
except ImportError:
    ConfigBase = None

try:
    import websockets as _websockets
except ImportError:
    _websockets = None

_HAS_WS = _websockets is not None

_AUTH_HEADER = "X-Kimi-Bot-Token"
_VERSION_HEADER = "X-Kimi-Claw-Version"

_DEFAULT_KIMIAPI_HOST = "https://www.kimi.com/api-claw"

_IM_SERVICE_PATHS = {
    "subscribe": "/kimi.gateway.im.v1.IMService/Subscribe",
    "sendMessage": "/kimi.gateway.im.v1.IMService/SendMessage",
    "listMessages": "/kimi.gateway.im.v1.IMService/ListMessages",
}

_CHAT_MESSAGE_STATUS_COMPLETED = 2
_RECONNECT_BASE = 1.0
_RECONNECT_MAX = 60.0
_PING_TIMEOUT = 30.0


def _resolve_im_rpc_base(kimiapi_host: str) -> str:
    base = kimiapi_host.rstrip("/")
    if "/api-claw" in base:
        return base.replace("/api-claw", "/api-ws", 1)
    return base


def _extract_text_from_blocks(value: dict[str, Any]) -> str:
    blocks = value.get("blocks") or []
    parts: list[str] = []
    for block in blocks:
        if "text" in block:
            text_val = block["text"]
            if isinstance(text_val, dict):
                content = text_val.get("content", "")
                if isinstance(content, str) and content:
                    parts.append(content)
            elif isinstance(text_val, str) and text_val:
                parts.append(text_val)
    return "\n".join(parts).strip()


class _TextState:
    __slots__ = ("block_id", "snapshot")

    def __init__(self):
        self.block_id = "0"
        self.snapshot = ""


class _ReasoningState:
    __slots__ = ("block_id", "accumulated")

    def __init__(self):
        self.block_id = "0"
        self.accumulated = ""


class _ToolState:
    __slots__ = ("tool_call_id", "name", "args", "status", "summary", "resource_links", "block_id")

    def __init__(self, tool_call_id: str, name: str, args: str = ""):
        self.tool_call_id = tool_call_id
        self.name = name
        self.args = args
        self.status = "running"
        self.summary = ""
        self.resource_links: list[dict[str, str]] = []
        self.block_id = tool_call_id or "0"


class _StreamState:
    __slots__ = ("ws", "text", "lock", "reasoning", "started", "tool_states", "resource_link_uris", "reasoning_rotate_pending")

    def __init__(self):
        self.ws = None
        self.text = _TextState()
        self.lock = asyncio.Lock()
        self.reasoning = None
        self.started = False
        self.tool_states: dict[str, _ToolState] = {}
        self.resource_link_uris: set[str] = set()
        self.reasoning_rotate_pending = False


class _FallbackConfigBase:
    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)


_ConfigBase = ConfigBase or _FallbackConfigBase


class KimiChannelConfig(_ConfigBase):
    enabled: bool = False
    bot_token: str = ""
    kimiapi_host: str = _DEFAULT_KIMIAPI_HOST
    allow_from: list[str] = ["*"]
    streaming: bool = True
    stream_reasoning: bool = True

    def __init__(self, **kwargs: Any):
        values = {
            "enabled": False,
            "bot_token": "",
            "kimiapi_host": _DEFAULT_KIMIAPI_HOST,
            "allow_from": ["*"],
            "streaming": True,
            "stream_reasoning": True,
        }
        values.update(kwargs)
        super().__init__(**values)


class KimiChannel(BaseChannel):
    name = "kimi"
    display_name = "Kimi"
    send_progress = False
    send_tool_hints = False
    show_reasoning = False

    def __init__(self, config: Any, bus: MessageBus):
        normalized_config = self._normalize_config(config)
        super().__init__(normalized_config, bus)
        self._bot_token = normalized_config.bot_token
        self._kimiapi_host = normalized_config.kimiapi_host
        self._im_rpc_base = _resolve_im_rpc_base(self._kimiapi_host)
        self._stream_reasoning = bool(getattr(normalized_config, "stream_reasoning", True))
        self._running = False
        self._chat_map: dict[str, str] = {}
        self._subscribe_task: asyncio.Task | None = None
        self._http: httpx.AsyncClient | None = None
        self._last_event_id: str = ""
        self._ping_task: asyncio.Task | None = None
        self._streams: dict[str, _StreamState] = {}

    @staticmethod
    def _normalize_config(config: Any) -> KimiChannelConfig:
        if isinstance(config, KimiChannelConfig):
            return config
        if isinstance(config, dict):
            data = dict(config)
            if "allowFrom" in data and "allow_from" not in data:
                data["allow_from"] = data["allowFrom"]
            if "botToken" in data and "bot_token" not in data:
                data["bot_token"] = data["botToken"]
            if "kimiapiHost" in data and "kimiapi_host" not in data:
                data["kimiapi_host"] = data["kimiapiHost"]
            if "streamReasoning" in data and "stream_reasoning" not in data:
                data["stream_reasoning"] = data["streamReasoning"]
            return KimiChannelConfig(**data)

        values = {
            "enabled": getattr(config, "enabled", False),
            "bot_token": getattr(config, "bot_token", getattr(config, "botToken", "")),
            "kimiapi_host": getattr(config, "kimiapi_host", getattr(config, "kimiapiHost", _DEFAULT_KIMIAPI_HOST)),
            "allow_from": getattr(config, "allow_from", getattr(config, "allowFrom", ["*"])),
            "streaming": getattr(config, "streaming", True),
            "stream_reasoning": getattr(config, "stream_reasoning", getattr(config, "streamReasoning", True)),
        }
        return KimiChannelConfig(**values)

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return {
            "enabled": False,
            "bot_token": "",
            "kimiapi_host": _DEFAULT_KIMIAPI_HOST,
            "allow_from": ["*"],
            "streaming": True,
            "stream_reasoning": True,
        }

    async def start(self) -> None:
        if not self._bot_token:
            self.logger.error("bot_token is empty – cannot start")
            return
        self._running = True
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0),
            http2=False,
            headers={"user-agent": "nanobot-kimi/0.1.0"},
        )
        self._subscribe_task = asyncio.create_task(self._subscribe_loop())
        self.logger.info("Kimi channel started (IM RPC)")
        try:
            await self._subscribe_task
        finally:
            self._running = False

    async def stop(self) -> None:
        self._running = False
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        if self._subscribe_task and not self._subscribe_task.done():
            self._subscribe_task.cancel()
        for ss in self._streams.values():
            if ss.ws:
                try:
                    await ss.ws.close()
                except Exception:
                    pass
        self._streams.clear()
        if self._http:
            try:
                await self._http.aclose()
            except Exception:
                pass
            self._http = None
        self.logger.info("Kimi channel stopped")

    async def send(self, msg: OutboundMessage) -> None:
        chat_id = msg.chat_id
        kimi_chat_id = self._chat_map.get(chat_id)
        if not kimi_chat_id:
            self.logger.warning("no Kimi chat mapping for {}", chat_id)
            return
        await self._send_im_message(kimi_chat_id, msg.content)

    async def send_delta(self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None) -> None:
        self.logger.info("send_delta called chat={} delta_len={}", chat_id, len(delta))
        kimi_chat_id = self._chat_map.get(chat_id)
        if not kimi_chat_id:
            return
        ss = self._streams.get(kimi_chat_id)
        if not ss or not ss.ws:
            ss = await self._open_stream(kimi_chat_id)
            if not ss:
                return
        async with ss.lock:
            if not ss.started:
                await self._start_stream(ss, kimi_chat_id)
            if metadata:
                await self._emit_metadata_blocks(ss, metadata)
            next_snapshot = f"{ss.text.snapshot}{delta}"
            frame = self._build_progress_frame("text", ss.text.block_id, ss.text.snapshot, next_snapshot)
            if not frame:
                return
            try:
                await ss.ws.send(json.dumps(frame))
                ss.text.snapshot = next_snapshot
            except Exception as e:
                self.logger.warning("send_delta ws error: {}", e)

    async def send_reasoning_delta(self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None) -> None:
        if not self._stream_reasoning:
            return
        kimi_chat_id = self._chat_map.get(chat_id)
        if not kimi_chat_id:
            return
        ss = self._streams.get(kimi_chat_id)
        if not ss or not ss.ws:
            ss = await self._open_stream(kimi_chat_id)
            if not ss:
                return
        async with ss.lock:
            if not ss.started:
                await self._start_stream(ss, kimi_chat_id)
            if metadata:
                await self._emit_metadata_blocks(ss, metadata)
            reasoning = self._ensure_reasoning_state(ss)
            if ss.reasoning_rotate_pending and not reasoning.accumulated:
                reasoning.block_id = self._next_block_id("thinking")
                ss.reasoning_rotate_pending = False
            next_snapshot = f"{reasoning.accumulated}{delta}"
            frame = self._build_progress_frame("think", reasoning.block_id, reasoning.accumulated, next_snapshot)
            if not frame:
                return
            try:
                await ss.ws.send(json.dumps(frame))
                reasoning.accumulated = next_snapshot
            except Exception as e:
                self.logger.warning("send_reasoning_delta ws error: {}", e)

    async def send_reasoning_end(self, chat_id: str, metadata: dict[str, Any] | None = None) -> None:
        if not self._stream_reasoning:
            return
        kimi_chat_id = self._chat_map.get(chat_id)
        if not kimi_chat_id:
            return
        ss = self._streams.get(kimi_chat_id)
        if not ss or not ss.ws:
            return
        async with ss.lock:
            if hasattr(ss, "reasoning"):
                ss.reasoning = None
                ss.reasoning_rotate_pending = True

    def _im_headers(self, content_type: str = "application/json; charset=utf-8") -> dict[str, str]:
        return {
            _AUTH_HEADER: self._bot_token,
            _VERSION_HEADER: "nanobot-kimi-0.1.0",
            "content-type": content_type,
        }

    # -- Subscribe loop -------------------------------------------------------

    async def _subscribe_loop(self) -> None:
        backoff = _RECONNECT_BASE
        while self._running:
            try:
                await self._run_subscribe()
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.logger.error("subscribe error: {} – reconnecting in {:.0f}s", e, backoff)
            if not self._running:
                return
            self.logger.info("subscribe reconnecting in {:.0f}s", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _RECONNECT_MAX)

    async def _run_subscribe(self) -> None:
        url = f"{self._im_rpc_base}{_IM_SERVICE_PATHS['subscribe']}"
        headers = self._im_headers("application/connect+json; charset=utf-8")
        headers["accept"] = "application/connect+json"
        headers["connect-protocol-version"] = "1"

        request_body: dict[str, Any] = {}
        if self._last_event_id:
            request_body["sinceId"] = self._last_event_id

        body = self._encode_connect_envelope(request_body)
        self.logger.info("IM subscribe connecting to {}", url)

        async with self._http.stream("POST", url, headers=headers, content=body) as resp:
            if resp.status_code != 200:
                text = await resp.aread()
                raise RuntimeError(f"subscribe failed status={resp.status_code} body={text.decode()[:300]}")

            default_chat = resp.headers.get("x-kimi-claw-default-chat")
            if default_chat:
                self.logger.info("default chat from subscribe: {}", default_chat)

            await self._read_subscribe_stream(resp)

    async def _read_subscribe_stream(self, resp: httpx.Response) -> None:
        buf = b""

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        self._ping_task = asyncio.create_task(self._ping_watchdog())

        chunk_count = 0
        async for chunk in resp.aiter_bytes():
            chunk_count += 1
            if chunk_count <= 5:
                self.logger.info("subscribe chunk #{} len={}", chunk_count, len(chunk))
            buf += chunk
            while len(buf) >= 5:
                flags = buf[0]
                length = int.from_bytes(buf[1:5], "big")
                if len(buf) < 5 + length:
                    break
                payload_bytes = buf[5:5 + length]
                buf = buf[5 + length:]

                if flags & 2:
                    payload_str = payload_bytes[:200].decode("utf-8", errors="replace")
                    self.logger.info("subscribe end-of-stream: {}", payload_str)
                    try:
                        err = json.loads(payload_str)
                        if isinstance(err, dict) and "error" in err:
                            self._last_event_id = ""
                    except json.JSONDecodeError:
                        pass
                    return

                try:
                    event = json.loads(payload_bytes)
                except json.JSONDecodeError:
                    self.logger.debug("subscribe non-json flags={} raw={}", flags, payload_bytes[:100])
                    continue

                event_keys = list(event.keys())
                if "chatMessage" in event:
                    self.logger.info("subscribe chatMessage raw={}", payload_bytes[:500].decode("utf-8", "replace"))

                event_id = event.get("id", "")
                if event_id:
                    self._last_event_id = event_id

                if "ping" in event:
                    if self._ping_task and not self._ping_task.done():
                        self._ping_task.cancel()
                        self._ping_task = asyncio.create_task(self._ping_watchdog())
                    continue

                if "reconnect" in event:
                    self.logger.info("subscribe received reconnect event")
                    return

                if "chatMessage" in event:
                    value = event["chatMessage"]
                    if not isinstance(value, dict):
                        continue
                    status = value.get("status")
                    if status not in (_CHAT_MESSAGE_STATUS_COMPLETED, "STATUS_COMPLETED", "COMPLETED"):
                        continue
                    await self._handle_chat_message(value)
                else:
                    self.logger.debug("subscribe unknown event keys={}", event_keys)

    async def _ping_watchdog(self) -> None:
        await asyncio.sleep(_PING_TIMEOUT)
        self.logger.warning("subscribe ping timeout – forcing reconnect")

    async def _handle_chat_message(self, value: dict[str, Any]) -> None:
        chat_id = value.get("chatId", "")
        message_id = value.get("messageId", "")
        sender_id = value.get("senderId", "") or "kimi-user"
        room_id = value.get("roomId", "")
        summary = value.get("summary", "")

        text = _extract_text_from_blocks(value) or summary
        if not text:
            text = await self._fetch_message_text(chat_id, message_id)
        if not text:
            return

        nanobot_chat_id = chat_id or f"kimi-{uuid.uuid4().hex[:8]}"
        if chat_id:
            self._chat_map[nanobot_chat_id] = chat_id

        session_key = f"kimi:{nanobot_chat_id}"
        self.logger.info("inbound from Kimi chat={} sender={} text_len={}", nanobot_chat_id, sender_id, len(text))

        await self._handle_message(
            sender_id=sender_id,
            chat_id=nanobot_chat_id,
            content=text,
            metadata={"kimi_chat_id": chat_id, "message_id": message_id, "room_id": room_id},
            session_key=session_key,
            is_dm=True,
        )

    async def _fetch_message_text(self, chat_id: str, message_id: str) -> str:
        url = f"{self._im_rpc_base}{_IM_SERVICE_PATHS['listMessages']}"
        headers = self._im_headers()
        payload = {"chatId": chat_id, "direction": 1, "pageSize": 10}
        try:
            resp = await self._http.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                self.logger.warning("listMessages failed status={}", resp.status_code)
                return ""
            data = resp.json()
            for item in data.get("messages", []):
                msg = item.get("message", {})
                if msg.get("id") == message_id:
                    return _extract_text_from_blocks(msg)
        except Exception as e:
            self.logger.warning("listMessages error: {}", e)
        return ""

    # -- Outbound: SendMessageStream (WebSocket) ------------------------------

    def _stream_ws_url(self) -> str:
        return f"{self._im_rpc_base}/im/send-message/ws".replace("https://", "wss://").replace("http://", "ws://")

    async def _open_stream(self, kimi_chat_id: str) -> _StreamState | None:
        if not _HAS_WS:
            return None
        ss = _StreamState()
        url = self._stream_ws_url()
        try:
            ws = await _websockets.connect(
                url,
                additional_headers={_AUTH_HEADER: self._bot_token},
                close_timeout=5,
            )
            ss.ws = ws
            self._streams[kimi_chat_id] = ss
            self.logger.info("opened SendMessageStream ws for chat={}", kimi_chat_id)
            return ss
        except Exception as e:
            self.logger.warning("open SendMessageStream ws failed: {}", e)
            return None

    async def _close_stream(self, kimi_chat_id: str) -> None:
        ss = self._streams.pop(kimi_chat_id, None)
        if not ss or not ss.ws:
            return
        try:
            if ss.started:
                await ss.ws.send(json.dumps(self._build_end_frame()))
            await asyncio.wait_for(ss.ws.wait_closed(), timeout=3)
        except Exception:
            pass
        try:
            await ss.ws.close()
        except Exception:
            pass
        self.logger.info("closed SendMessageStream ws for chat={}", kimi_chat_id)

    # -- Outbound: unary SendMessage ------------------------------------------

    async def _send_im_message(self, chat_id: str, text: str) -> None:
        url = f"{self._im_rpc_base}{_IM_SERVICE_PATHS['sendMessage']}"
        headers = self._im_headers()
        payload = {
            "chatId": chat_id,
            "blocks": [{"id": f"im_outbound_text_{uuid.uuid4().hex[:8]}_0", "text": {"content": text}}],
        }
        try:
            resp = await self._http.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                self.logger.error("sendMessage failed status={} body={}", resp.status_code, resp.text[:200])
            else:
                self.logger.info("sent reply to Kimi chat={} len={}", chat_id, len(text))
        except Exception as e:
            self.logger.error("sendMessage error: {}", e)
            raise

    # -- Connect protocol framing ---------------------------------------------

    @staticmethod
    def _encode_connect_envelope(payload: dict[str, Any]) -> bytes:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        header = bytearray(5)
        header[0] = 0
        header[1:5] = len(body).to_bytes(4, "big")
        return bytes(header) + body

    @staticmethod
    def _build_stream_start_frame(chat_id: str) -> dict[str, Any]:
        return {"chatId": chat_id}

    @staticmethod
    def _build_end_frame() -> dict[str, Any]:
        return {"end": {}}

    @staticmethod
    def _build_text_block_frame(block_id: str, content: str, append: bool) -> dict[str, Any]:
        return {
            "blockUpdate": {
                "op": 2 if append else 1,
                "mask": {"paths": ["block.text.content"]},
                "block": {
                    "id": block_id,
                    "text": {"content": content},
                },
            }
        }

    @staticmethod
    def _build_thinking_block_frame(block_id: str, content: str, append: bool) -> dict[str, Any]:
        return {
            "blockUpdate": {
                "op": 2 if append else 1,
                "mask": {"paths": ["block.think.content"]},
                "block": {
                    "id": block_id,
                    "think": {"content": content},
                },
            }
        }

    @staticmethod
    def _ensure_reasoning_state(ss: _StreamState) -> _ReasoningState:
        reasoning = getattr(ss, "reasoning", None)
        if reasoning is None:
            reasoning = _ReasoningState()
            ss.reasoning = reasoning
        return reasoning

    async def _emit_metadata_blocks(self, ss: _StreamState, metadata: dict[str, Any]) -> None:
        tool = metadata.get("tool") if isinstance(metadata, dict) else None
        resource_links = metadata.get("resource_links") if isinstance(metadata, dict) else None
        if isinstance(tool, dict):
            await self._emit_tool_block(ss, tool)
        if isinstance(resource_links, list):
            await self._emit_resource_link_blocks(ss, resource_links)

    async def _emit_tool_block(self, ss: _StreamState, tool: dict[str, Any]) -> None:
        if not ss.ws:
            return
        tool_call_id = str(tool.get("id") or tool.get("tool_call_id") or self._next_block_id("tool"))
        name = str(tool.get("name") or "tool")
        args_obj = tool.get("args") or tool.get("arguments") or {}
        args = args_obj if isinstance(args_obj, str) else json.dumps(args_obj, ensure_ascii=False)
        state = ss.tool_states.get(tool_call_id)
        if state is None:
            state = _ToolState(tool_call_id=tool_call_id, name=name, args=args)
            state.block_id = self._next_block_id("tool")
            ss.tool_states[tool_call_id] = state
        state.name = name
        state.args = args
        state.status = str(tool.get("status") or state.status or "running")
        if "summary" in tool and isinstance(tool.get("summary"), str):
            state.summary = tool["summary"]
        frame = self._build_tool_block_frame(state)
        await ss.ws.send(json.dumps(frame))

    async def _emit_resource_link_blocks(self, ss: _StreamState, resource_links: list[Any]) -> None:
        if not ss.ws:
            return
        for item in resource_links:
            if not isinstance(item, dict):
                continue
            uri = str(item.get("uri") or item.get("url") or item.get("downloadUrl") or "").strip()
            if not uri or uri in ss.resource_link_uris:
                continue
            ss.resource_link_uris.add(uri)
            title = str(item.get("title") or item.get("name") or uri.rsplit("/", 1)[-1] or uri)
            frame = self._build_resource_link_block_frame(self._next_block_id("resource"), title, uri)
            await ss.ws.send(json.dumps(frame))

    def _build_tool_block_frame(self, tool: _ToolState) -> dict[str, Any]:
        is_error = tool.status == "failed"
        contents = []
        if tool.status != "running":
            contents = [{"content": {"text": {"content": tool.summary or ("Tool failed." if is_error else "Tool completed successfully.")}}}]
        return {
            "blockUpdate": {
                "op": 1,
                "block": {
                    "id": tool.block_id,
                    "tool": {
                        "toolCallId": tool.tool_call_id,
                        "name": tool.name,
                        "args": tool.args,
                        "isError": is_error,
                        "contents": contents,
                        "status": 1 if tool.status == "running" else 2,
                    },
                },
            }
        }

    @staticmethod
    def _build_resource_link_block_frame(block_id: str, title: str, uri: str) -> dict[str, Any]:
        return {
            "blockUpdate": {
                "op": 1,
                "block": {
                    "id": block_id,
                    "resourceLink": {
                        "title": title,
                        "uri": uri,
                        "downloadUrl": uri,
                        "etag": "",
                        "sizeBytes": 0,
                    },
                },
            }
        }

    @staticmethod
    def _next_block_id(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    async def _start_stream(self, ss: _StreamState, kimi_chat_id: str) -> None:
        if ss.started or not ss.ws:
            return
        await ss.ws.send(json.dumps(self._build_stream_start_frame(kimi_chat_id)))
        await ss.ws.send(json.dumps({"ping": {}}))
        ss.started = True

    def _build_progress_frame(
        self,
        lane: str,
        block_id: str,
        previous_snapshot: str,
        next_snapshot: str,
    ) -> dict[str, Any] | None:
        if not next_snapshot or next_snapshot == previous_snapshot:
            return None
        append = bool(previous_snapshot) and next_snapshot.startswith(previous_snapshot)
        payload = next_snapshot[len(previous_snapshot):] if append else next_snapshot
        if lane == "think":
            return self._build_thinking_block_frame(block_id, payload, append=append)
        return self._build_text_block_frame(block_id, payload, append=append)
