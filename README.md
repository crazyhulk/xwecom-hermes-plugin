# xwecom-hermes-plugin

WeCom (企业微信) AI-bot adapter for [Hermes Agent](https://hermes-agent.nousresearch.com), shipped as a
standalone plugin. It presents WeCom to Hermes as a **fully editable IM**, so Hermes uses the same
delivery path it uses for Telegram/Slack — while every edit is rendered on WeCom's reply-stream bubbles.

## Why

WeCom cannot edit a sent message. The built-in adapter works around that with a WeCom-specific
"native streaming" path inside Hermes core, which is rarely exercised and hard to change upstream.
This plugin takes the opposite approach and emulates the missing capabilities at the edge:

| Hermes sees | How xwecom renders it on WeCom |
|---|---|
| `send(..., expect_edits)` returns an editable message | a live `msgtype: stream` bubble |
| `edit_message(id, text)` | the bubble's content is replaced |
| `edit_message(..., finalize=True)` | the bubble is closed (`finish: true`) |
| a message that lives longer than 10 minutes | sealed at 9 min and continued in a fresh bubble, same id |
| a message longer than one frame (20 KB) | spills over into further bubbles, no repeated text |
| an edit after the message was closed | continues in a new bubble (WeCom would silently drop it) |
| tool progress | an editable progress bubble with a live `⠹ terminal 37s` timer |
| exec approval buttons | a clickable `button_interaction` card, updated in place after a click |

Measured against the live WeCom API (Nov 2026): several streams can be open at once on one inbound
message; a stream has no frame-count cap (120 frames at 2/s were accepted); a stream expires 600 s after
its first frame (`846608`), after which even the closing frame is rejected; updates to a finished stream
are acknowledged with `errcode 0` but never rendered.

## Install

```bash
git clone -b v2 git@github.com:crazyhulk/xwecom-hermes-plugin.git /tmp/xwecom-src
cp -r /tmp/xwecom-src/xwecom ~/.hermes/plugins/xwecom      # or <profile>/plugins/xwecom
hermes plugins enable xwecom
```

Requires `aiohttp` and `httpx` in the Hermes environment (both ship with Hermes' messaging extra).

## Configure

Credentials are read as `XWECOM_*` first, then the built-in adapter's `WECOM_*`, so an existing WeCom
profile works without re-entering anything:

```bash
XWECOM_BOT_ID=...            # or WECOM_BOT_ID
XWECOM_SECRET=...            # or WECOM_SECRET
XWECOM_HOME_CHANNEL=...      # optional: cron / notification target
XWECOM_DM_POLICY=pairing     # open | allowlist | pairing | disabled
XWECOM_ALLOWED_USERS=...     # optional allowlist
```

```yaml
gateway:
  platforms:
    wecom:
      enabled: false   # one bot = one WebSocket: a second connection kicks the first
    xwecom:
      enabled: true
```

### Switching from the built-in `wecom` adapter

- Disable `wecom` first — WeCom allows one WebSocket per bot, and the two adapters would kick each other.
- Sessions are keyed by platform name, so chats start fresh conversations under `xwecom`.
- Cron jobs delivering to `wecom` must be changed to `deliver=xwecom`.

## Layout

```
xwecom/
├── adapter.py      # WebSocket connection, inbound parsing, access policy, register()
├── emulation.py    # the editable-IM surface Hermes talks to (+ tool timer, approval cards)
├── messages.py     # virtual message → chain of WeCom stream bubbles (rotation, spill, overlay)
├── streaming.py    # one stream frame on the wire, ack tracking, coalescing, final-frame retry
├── hooks.py        # pre/post_tool_call → tool timer of the right chat
├── media.py        # inbound media decryption, outbound uploads
├── send_queue.py   # per-chat FIFO + 30 msgs/min token bucket
├── gateway_env.py  # env → config seeding (XWECOM_* with WECOM_* fallback)
└── compat.py       # local stand-ins for core internals the plugin API does not publish
```

Only names listed in Hermes' published plugin API (`hermes_cli/data/plugin_api.json`) are imported
directly; everything else goes through `compat.py`, which degrades instead of failing when core moves.

## Tests

```bash
python -m pytest tests          # emulation core against a fake WeCom that models the measured API
```
