# xwecom-hermes-plugin 实现方案

## 概述

xwecom 是一个 Hermes Agent 第三方 platform adapter 插件，用企业微信（WeCom）官方 Python SDK 替代 Hermes 内置的 wecom adapter，解决其 WebSocket 重连不稳定、消息丢失、媒体处理断裂等 28+ 已知 issues。

## 背景

| 维度 | Hermes 内置 wecom adapter | OpenClaw 官方插件 | 官方 Python SDK |
|------|--------------------------|-----------------|----------------|
| 语言 | Python (3344 行单文件) | TypeScript (~5000+ 行) | Python (~1400 行) |
| 维护方 | Hermes 社区 (issues 被标 p2/p3) | 腾讯企微官方团队 | 腾讯企微官方 |
| 架构 | 裸写 aiohttp/httpx WebSocket | 基于 `@wecom/aibot-node-sdk` | asyncio + websockets |
| 稳定性 | WS 重连/消息丢失/媒体断裂 | 生产级 | 基本可用但有若干 bug |

## 技术方案

### 核心思路

用官方 Python SDK（fork 后修复已知 bug）作为 WebSocket 通信层，在上面写一个薄的 Hermes `BasePlatformAdapter` 包装。对照 OpenClaw TypeScript 实现补全 SDK 缺失的功能（媒体上传、流式回复降级等）。

### 为什么不用 Node sidecar

- Hermes 插件系统是纯 Python，`ctx.register_platform()` 要求返回 Python 对象
- IPC 桥接增加故障面（进程管理、通信协议、序列化）
- 官方已有 Python SDK，bug 可修，逻辑可对照 OpenClaw TS 实现补全

## 架构

```
┌─────────────────────────────────────────────────────────┐
│  ~/.hermes/plugins/xwecom/                               │
│                                                          │
│  plugin.yaml              # kind: platform               │
│  __init__.py              # from .adapter import register│
│  adapter.py               # XWeComAdapter                │
│  sdk/                     # fork of official Python SDK  │
│    ├── __init__.py                                       │
│    ├── client.py          # WSClient (patched)           │
│    ├── ws.py              # WsConnectionManager          │
│    ├── message_handler.py                                │
│    ├── api.py             # HTTP file download           │
│    ├── crypto_utils.py    # AES-256-CBC decryption       │
│    ├── types.py           # Type definitions             │
│    └── logger.py          # Logging                      │
│  media.py                 # 媒体上传/下载/类型检测        │
│  stream.py                # 流式回复 + BlockChunker       │
│  policy.py                # DM/Group 访问控制             │
│  constants.py             # 常量定义                      │
└─────────────────────────────────────────────────────────┘
         │
         ▼ extends
┌──────────────────────────────────────┐
│ BasePlatformAdapter (Hermes Gateway) │
│  connect() / disconnect()            │
│  send() / handle_message()           │
│  send_typing() / get_chat_info()     │
└──────────────────────────────────────┘
```

## SDK 修复清单

| # | 问题 | 修复 | 参照 |
|---|------|------|------|
| 1 | `dotenv` vs `python-dotenv` 依赖错写 | pyproject.toml 改为 `python-dotenv>=1.0` | — |
| 2 | `asyncio.get_event_loop()` 弃用 | 改为 `asyncio.get_running_loop()` | — |
| 3 | 重复 websockets import | 删除冗余 try/except | — |
| 4 | ACK 超时硬编码 5s 不可配 | 加入 `WSClientOptions.reply_ack_timeout` | TS SDK |
| 5 | 无 `send_message` 连接状态预检 | 加 `if not self._ws` 守卫 | OpenClaw |
| 6 | ACK 失败缺重试 | 补完 reply queue 重试逻辑 | OpenClaw `replyStreamNonBlocking` |
| 7 | 缺 stream expired 降级 | 捕获 errcode 846608 → `send_message` | OpenClaw monitor.ts |
| 8 | 缺 media upload（分块上传） | 新增实现 | OpenClaw `media-uploader.ts` |
| 9 | `disconnect()` 在非 async 上下文可能失败 | 加 loop 检测守卫 | — |

## 功能对照 (vs OpenClaw 官方插件)

| 功能 | OpenClaw TS | xwecom 实现 |
|------|------------|-------------|
| WebSocket 连接 | `@wecom/aibot-node-sdk` | 官方 Python SDK (patched) |
| 认证 (aibot_subscribe) | SDK 内部 | SDK 内部 ✓ |
| 心跳 (30s + 2-miss 判死) | SDK 内置 | SDK 内置 ✓ |
| 自动重连 (指数退避) | max 10 | max=-1 (无限) |
| 消息接收 | `on("message")` | SDK 事件 ✓ |
| 流式回复 (reply_stream) | `replyStream(frame, streamId, text, finish)` | SDK `reply_stream()` ✓ |
| Block 分句发送 | 120-360 字符 + 句末断点 | `stream.py` BlockChunker |
| Stream 过期降级 (846608) | → `sendMessage` | → `self.send()` |
| ACK 串行队列 | 100 帧上限, 5s 超时 | SDK `_process_reply_queue` |
| Proactive 发送 | `sendMessage(chatId, body)` | SDK `send_message()` ✓ |
| 媒体下载 + AES 解密 | `downloadFile(url, aesKey)` | SDK `download_file()` ✓ |
| 媒体上传（分块） | `uploadMedia(buffer, opts)` | `media.py` 新增实现 |
| 媒体发送 | `sendMediaMessage()` | `send_message` + media_id |
| DM 策略 | open/pairing/allowlist/disabled | `policy.py` |
| Group 策略 | open/allowlist/disabled + per-group | `policy.py` |
| Template Card | 发送 + 事件更新 | Phase 2 迭代 |
| Token Lock | — | `acquire_scoped_lock` |
| Cron Delivery | — | `standalone_sender_fn` |
| 消息去重 | 有 | `MessageDeduplicator` |

## Hermes 插件集成点

遵循 Hermes [Adding Platform Adapters](https://hermes-agent.nousresearch.com/docs/developer-guide/adding-platform-adapters) Plugin Path：

1. **`plugin.yaml`** — `kind: platform`, 声明环境变量
2. **`register(ctx)`** — `ctx.register_platform()` 完整参数
3. **`env_enablement_fn`** — 环境变量自动配置
4. **`cron_deliver_env_var`** — cron 投递支持
5. **`standalone_sender_fn`** — out-of-process cron 发送
6. **Token Lock** — `acquire_scoped_lock("xwecom", bot_id)` 防多 profile 冲突
7. **`platform_hint`** — LLM 系统提示注入
8. **`max_message_length=4000`** — 消息智能分块

## 实施阶段

| 阶段 | 内容 | 交付物 |
|------|------|--------|
| Phase 1 | SDK Fork & Fix | `sdk/` 目录，修复 9 个已知问题 |
| Phase 2 | Core Adapter | `adapter.py` 能连接、收发消息 |
| Phase 3 | Stream Reply | `stream.py` BlockChunker + 流式 + 降级 |
| Phase 4 | Media | `media.py` 分块上传 + 类型检测 + 大小限制 |
| Phase 5 | Policy & Integration | `policy.py` + standalone_sender + token lock |
| Phase 6 | Tests | `tests/` 完整测试覆盖 |

## 配置方式

### 环境变量
```bash
XWECOM_BOT_ID=bot_xxx
XWECOM_SECRET=secret_xxx
XWECOM_WEBSOCKET_URL=wss://openws.work.weixin.qq.com  # optional
XWECOM_HOME_CHANNEL=chat_id_xxx                        # optional, for cron
XWECOM_ALLOWED_USERS=user1,user2                       # optional
XWECOM_ALLOW_ALL_USERS=true                            # optional
```

### config.yaml
```yaml
gateway:
  platforms:
    xwecom:
      enabled: true
      extra:
        bot_id: "bot_xxx"
        secret: "secret_xxx"
        dm_policy: "open"          # open | allowlist | disabled | pairing
        allow_from: ["*"]
        group_policy: "open"       # open | allowlist | disabled
        group_allow_from: []
```

## 与现有 wecom 并存

两个插件使用不同的 Platform name（`wecom` vs `xwecom`），可以在 config.yaml 中共存：

```yaml
gateway:
  platforms:
    wecom:
      enabled: false   # 禁用旧 adapter
    xwecom:
      enabled: true    # 启用新 adapter
```

切换只需改 enabled 标志即可回滚。

## 依赖

- Python >= 3.10
- websockets >= 14.0
- aiohttp >= 3.9
- pyee >= 11.0
- cryptography >= 42.0
- certifi >= 2023.0

## 参考实现

- OpenClaw WeCom Plugin: `~/.hermes/wecom-openclaw-plugin/src/`
- Official Python SDK: `~/.hermes/wecom-aibot-python-sdk-async/`
- Hermes IRC Plugin: `~/.hermes/hermes-agent/plugins/platforms/irc/`
- Hermes WeCom Adapter: `~/.hermes/hermes-agent/plugins/platforms/wecom/`
