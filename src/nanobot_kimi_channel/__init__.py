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


class _StreamState:
    __slots__ = ("ws", "block_id", "accumulated", "lock")

    def __init__(self):
        self.ws = None
        self.block_id = "0"
        self.accumulated = ""
        self.lock = asyncio.Lock()


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

    def __init__(self, **kwargs: Any):
        values = {
            "enabled": False,
            "bot_token": "",
            "kimiapi_host": _DEFAULT_KIMIAPI_HOST,
            "allow_from": ["*"],
            "streaming": True,
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
            return KimiChannelConfig(**data)

        values = {
            "enabled": getattr(config, "enabled", False),
            "bot_token": getattr(config, "bot_token", getattr(config, "botToken", "")),
            "kimiapi_host": getattr(config, "kimiapi_host", getattr(config, "kimiapiHost", _DEFAULT_KIMIAPI_HOST)),
            "allow_from": getattr(config, "allow_from", getattr(config, "allowFrom", ["*"])),
            "streaming": getattr(config, "streaming", True),
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
            ss.accumulated += delta
            frame = {
                "blockUpdate": {
                    "op": 1,
                    "block": {
                        "id": ss.block_id,
                        "text": {"content": ss.accumulated},
                    },
                }
            }
            try:
                await ss.ws.send(json.dumps(frame))
            except Exception as e:
                self.logger.warning("send_delta ws error: {}", e)

    async def send_reasoning_delta(self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None) -> None:
        pass

    async def send_reasoning_end(self, chat_id: str, metadata: dict[str, Any] | None = None) -> None:
        pass

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
            await ws.send(json.dumps({"chatId": kimi_chat_id}))
            await ws.send(json.dumps({"ping": {}}))
            ss.ws = ws
            ss.block_id = "0"
            ss.accumulated = ""
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
            await ss.ws.send(json.dumps({"end": {}}))
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
