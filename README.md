# nanobot-kimi-channel

Kimi (Moonshot) channel plugin for Nanobot.

This plugin connects Nanobot to Kimi's IM RPC endpoints and supports:

- inbound message subscription over Connect-style HTTP streaming
- outbound replies over IM RPC
- optional streaming replies over Kimi's WebSocket message channel
- optional streaming reasoning blocks over the same IM stream

## Status

This repository is structured as an external Nanobot channel plugin and registers the `kimi` channel through the `nanobot.channels` entry-point group.

## Installation

```bash
pip install .
```

## Configuration

Add the `kimi` channel to your Nanobot config:

```json
{
  "channels": {
    "kimi": {
      "enabled": true,
      "bot_token": "your-kimi-bot-token",
      "kimiapi_host": "https://www.kimi.com/api-claw",
      "allow_from": ["*"],
      "streaming": true,
      "stream_reasoning": true
    }
  }
}
```

Supported aliases handled by the plugin constructor:

- `botToken` -> `bot_token`
- `kimiapiHost` -> `kimiapi_host`
- `allowFrom` -> `allow_from`
- `streamReasoning` -> `stream_reasoning`

## Streaming behavior

- `send_delta()` now emits text stream frames with official-style `set`/`append` semantics based on snapshot growth
- `send_reasoning_delta()` emits separate `think` stream frames when `stream_reasoning` is enabled
- the stream now starts with a `chatId` frame and closes with an explicit `end` frame
- `send_reasoning_end()` closes the current reasoning lane so the next reasoning stream starts a fresh block
- if `metadata.tool` is provided, the plugin emits a dedicated tool block with running/done/failed status
- if `metadata.resource_links` is provided, the plugin emits de-duplicated resource link blocks
- reasoning lane rotation is armed by `send_reasoning_end()` and applied when the next reasoning snapshot begins

### Metadata contract for extended stream blocks

The plugin now accepts extra streaming metadata for parity-oriented block emission:

```python
metadata = {
    "tool": {
        "id": "tool_call_1",
        "name": "search",
        "args": {"query": "nanobot"},
        "status": "running"  # or "done" / "failed"
        "summary": "Tool completed successfully."
    },
    "resource_links": [
        {"uri": "https://example.com/result", "title": "result"}
    ]
}
```

These metadata fields are optional. If the upper layer does not provide them, the plugin keeps streaming text/reasoning only.

## Packaging

- distribution name: `nanobot-kimi-channel`
- Python package: `nanobot_kimi_channel`
- entry point: `kimi = nanobot_kimi_channel:KimiChannel`

## Development

```bash
python3 -m build
```

## Notes

- Requires `nanobot-ai>=0.2.0`
- The plugin expects a valid Kimi bot token passed as `bot_token`
