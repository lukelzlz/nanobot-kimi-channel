from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
from pathlib import Path


def _install_nanobot_stubs() -> None:
    nanobot = types.ModuleType("nanobot")
    bus = types.ModuleType("nanobot.bus")
    bus_events = types.ModuleType("nanobot.bus.events")
    bus_queue = types.ModuleType("nanobot.bus.queue")
    channels = types.ModuleType("nanobot.channels")
    channels_base = types.ModuleType("nanobot.channels.base")
    config = types.ModuleType("nanobot.config")
    config_schema = types.ModuleType("nanobot.config.schema")

    class OutboundMessage:
        def __init__(self, channel: str, chat_id: str, content: str, metadata: dict | None = None):
            self.channel = channel
            self.chat_id = chat_id
            self.content = content
            self.metadata = metadata or {}

    class MessageBus:
        pass

    class BaseChannel:
        def __init__(self, config, bus):
            self.config = config
            self.bus = bus

            class _Logger:
                def info(self, *args, **kwargs):
                    pass

                def warning(self, *args, **kwargs):
                    pass

                def error(self, *args, **kwargs):
                    pass

            self.logger = _Logger()

    class ConfigBase:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    bus_events.OutboundMessage = OutboundMessage
    bus_queue.MessageBus = MessageBus
    channels_base.BaseChannel = BaseChannel
    config_schema.Base = ConfigBase

    sys.modules.setdefault("nanobot", nanobot)
    sys.modules.setdefault("nanobot.bus", bus)
    sys.modules.setdefault("nanobot.bus.events", bus_events)
    sys.modules.setdefault("nanobot.bus.queue", bus_queue)
    sys.modules.setdefault("nanobot.channels", channels)
    sys.modules.setdefault("nanobot.channels.base", channels_base)
    sys.modules.setdefault("nanobot.config", config)
    sys.modules.setdefault("nanobot.config.schema", config_schema)


def _install_loguru_stub() -> None:
    loguru = types.ModuleType("loguru")

    class _Logger:
        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

        def error(self, *args, **kwargs):
            pass

        def bind(self, **kwargs):
            return self

    loguru.logger = _Logger()
    sys.modules.setdefault("loguru", loguru)


def _load_plugin_module():
    _install_nanobot_stubs()
    _install_loguru_stub()

    module_path = Path(__file__).resolve().parents[1] / "src" / "nanobot_kimi_channel" / "__init__.py"
    spec = importlib.util.spec_from_file_location("nanobot_kimi_channel_verify", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class FakeWs:
    def __init__(self):
        self.frames: list[dict] = []

    async def send(self, payload: str):
        self.frames.append(json.loads(payload))

    async def close(self):
        return None

    async def wait_closed(self):
        return None


async def main() -> None:
    module = _load_plugin_module()
    channel = module.KimiChannel({"bot_token": "demo", "stream_reasoning": True}, object())

    stream = module._StreamState()
    stream.ws = FakeWs()
    channel._chat_map["chat-local"] = "chat-kimi"
    channel._streams["chat-kimi"] = stream

    await channel.send_delta(
        "chat-local",
        "Hello",
        {
            "tool": {
                "id": "call_1",
                "name": "search",
                "args": {"query": "nanobot"},
                "status": "running",
            },
            "resource_links": [{"uri": "https://example.com/a", "title": "A"}],
        },
    )
    await channel.send_reasoning_delta("chat-local", "Thinking 1")
    await channel.send_reasoning_end("chat-local")
    await channel.send_reasoning_delta(
        "chat-local",
        "Thinking 2",
        {
            "tool": {
                "id": "call_1",
                "name": "search",
                "args": {"query": "nanobot"},
                "status": "done",
                "summary": "done",
            }
        },
    )
    await channel.send_delta("chat-local", " world")
    await channel._close_stream("chat-kimi")

    for idx, frame in enumerate(stream.ws.frames, start=1):
        print(f"FRAME {idx}")
        print(json.dumps(frame, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
