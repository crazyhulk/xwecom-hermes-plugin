# xwecom-hermes-plugin

WeCom (企业微信) platform adapter for [Hermes Agent](https://hermes-agent.nousresearch.com), using the official WeCom Python SDK.

## Why

The built-in Hermes WeCom adapter has 28+ open issues (WebSocket reconnection instability, message loss, media handling failures) that are deprioritized as p2/p3. This plugin replaces it with an implementation based on the official SDK maintained by the Tencent WeCom team.

## Installation

```bash
# Clone into Hermes plugins directory
git clone git@github.com:crazyhulk/xwecom-hermes-plugin.git ~/.hermes/plugins/xwecom

# Install dependencies
pip install -r ~/.hermes/plugins/xwecom/requirements.txt
```

## Configuration

Set environment variables in `~/.hermes/.env`:

```bash
XWECOM_BOT_ID=bot_xxxxx
XWECOM_SECRET=your_secret_here
XWECOM_HOME_CHANNEL=chat_id_for_cron  # optional
```

Optional self-built app callback mode:

```bash
XWECOM_CALLBACK_ENABLED=true
XWECOM_CALLBACK_PORT=8645
XWECOM_CALLBACK_PATH=/wecom/callback
XWECOM_CORP_ID=wwxxxxxxxx
XWECOM_CORP_SECRET=corp_secret_for_replies
XWECOM_AGENT_ID=1000002
XWECOM_CALLBACK_TOKEN=callback_token
XWECOM_ENCODING_AES_KEY=43_char_encoding_aes_key
```

Or configure in `config.yaml`:

```yaml
gateway:
  platforms:
    xwecom:
      enabled: true
      extra:
        bot_id: "bot_xxxxx"
        secret: "your_secret"
        dm_policy: "open"
        group_policy: "open"
        callback_enabled: false
```

## Migrating from built-in wecom

Disable the old adapter and enable xwecom:

```yaml
gateway:
  platforms:
    wecom:
      enabled: false
    xwecom:
      enabled: true
```

## Features

- ✅ Official WeCom Python SDK (stable WebSocket, proper reconnection)
- ✅ Streaming replies with sentence-aligned block chunking
- ✅ Stream keepalive and proactive rotation before WeCom's 6-minute limit
- ✅ Full-content fallback for stream replies that exceed WeCom frame limits
- ✅ Stream expiry detection (errcode 846608) with graceful fallback
- ✅ Media upload/download with AES decryption
- ✅ Optional self-built app HTTP callback listener with crypto/XML verification
- ✅ Callback inbound MediaId download/cache for image, voice, file, and video
- ✅ Callback-mode proactive replies through WeCom Agent API
- ✅ DM and Group access control policies
- ✅ Cron delivery support (standalone sender)
- ✅ Token lock (multi-profile safety)
- ✅ Message deduplication
- ✅ Rapid plain-text message batching for WeCom client-side splits

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v
```

## Architecture

See [PLAN.md](./PLAN.md) for the full technical design document.
See [docs/migration-status.md](./docs/migration-status.md) for the current
OpenClaw/Python SDK migration coverage and verification log.

## License

MIT
