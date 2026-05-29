# nanobot-kimi-channel

Kimi (Moonshot) channel plugin for Nanobot.

This plugin connects Nanobot to Kimi's IM RPC endpoints and supports:

- inbound message subscription over Connect-style HTTP streaming
- outbound replies over IM RPC
- optional streaming replies over Kimi's WebSocket message channel

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
      "streaming": true
    }
  }
}
```

Supported aliases handled by the plugin constructor:

- `botToken` -> `bot_token`
- `kimiapiHost` -> `kimiapi_host`
- `allowFrom` -> `allow_from`

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
