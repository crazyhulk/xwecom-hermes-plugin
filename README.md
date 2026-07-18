# xwecom-hermes-plugin

WeCom (企业微信) platform adapter for [Hermes Agent](https://hermes-agent.nousresearch.com), using the official WeCom Python SDK.

## Why

Heremes Agent 的 wecom 适配器实在是一言难尽。

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
- ✅ Passive replies bound to the inbound WeCom `req_id` (no active-send quota for normal replies)
- ✅ Native WeCom streaming on compatible Hermes runtimes, with passive final-reply fallback
- ✅ Hermes-compatible non-editing fallback (no partial/duplicate preview messages)
- ✅ WeCom thinking/typing placeholder finalized by the passive reply
- ✅ UTF-8 byte-safe text chunking without silent truncation
- ✅ Media upload/download with AES decryption
- ✅ Native Hermes image, document, voice, and video delivery methods
- ✅ Inbound image/file/video caching with MIME and quote context preservation
- ✅ Optional self-built app HTTP callback listener with crypto/XML verification
- ✅ Callback inbound MediaId download/cache for image, voice, file, and video
- ✅ Callback-mode proactive replies through WeCom Agent API
- ✅ Bot-first, Agent HTTP fallback for text and media delivery
- ✅ DM and Group access control policies
- ✅ Hermes-owned DM pairing flow
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

OpenClaw's official plugin owns both LLM dispatch and the original WeCom frame,
so it can drive `replyStream` for every model delta. Hermes owns LLM dispatch in
the gateway. Hermes runtimes with the native-stream seam call this adapter's
`send_stream_frame()` for the full turn. Older runtimes do not call that method;
because message editing is disabled, they skip partial previews and deliver the
final response through `send(reply_to=message_id)`, which sends a passive WeCom
stream reply (`finish=true`) correlated to the inbound frame.

See [PLAN.md](./PLAN.md) for the full technical design document.
See [docs/migration-status.md](./docs/migration-status.md) for the current
OpenClaw/Python SDK migration coverage and verification log.

## License

MIT
