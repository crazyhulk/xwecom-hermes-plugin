# xwecom 迁移状态记录

更新时间：2026-07-02

## 参考来源

- `/Users/bilibili/.hermes/wecom-openclaw-plugin`：官方 OpenClaw TypeScript 插件，主要参考 `src/monitor.ts`、`src/message-parser.ts`、`src/message-sender.ts`、`src/media-uploader.ts`、`src/state-manager.ts`、`src/template-card-manager.ts`。
- `/Users/bilibili/.hermes/openclaw-plugin-wecom`：个人二开版本，主要参考 `wecom/ws-monitor.js` 的 keepalive、stream rotation、callback/event 修复，以及测试集中覆盖的 issue fixes。
- `/Users/bilibili/.hermes/wecom-aibot-python-sdk-async`：官方 Python SDK，作为 `sdk/` WebSocket、API、AES 下载解密、基础类型定义的迁移来源。

## 本轮补齐

- `adapter.py` 接入 `state_manager`：连接成功后登记 WSClient 和连接状态；入站消息记录 `chat_id -> req_id`，供流式回复复用；断开时清理连接状态。
- `adapter.py` 接入 `monitor`：普通消息通过 `run_with_message_timeout` 做处理超时保护，并用 `SessionRecorder` 标记 processing/finished/failed/timeout。
- `adapter.py` 接入事件回调：`enter_chat` 使用 `reply_welcome`/`send_message` 发送欢迎语；`disconnected_event` 主动断开并标记 displaced；`template_card_event` 尝试更新缓存卡片；`auth_change_event` 和 `template_card_event` 的文本化内容会继续投递给 Hermes。
- `adapter.py` 接入出站 Template Card：主动 `send()` 和流式 finalize 会提取 LLM 输出中的 template card JSON，先发卡片，再把剩余文本作为 markdown/stream final；流式中间帧会遮罩 card JSON。
- `adapter.py` 接入 `MEDIA:/FILE:` 回复指令：主动 `send()` 和流式 finalize 会提取指令行，调用 `upload_and_send_media()` 主动发送文件，并从可见文本中移除指令。
- `adapter.py` 接入 `NonBlockingStreamGate`：中间流式帧在上一帧 ACK pending 时跳过，避免 per-reqId 队列积压；seed/final 帧仍强制发送。
- `adapter.py` 接入长流式 keepalive / rotation：4 分钟 keepalive 重发最后非空内容，5 分钟主动 finish 当前 stream 并用新 `stream_id` 继续，避免企业微信 6 分钟硬限制。
- `adapter.py` 接入超长完整内容兜底：final stream 因 20KB 单帧限制被截断时，会通过主动 `send_message` 按 UTF-8 安全分块补发完整回复。
- `adapter.py` 接入同会话纯文本防抖聚合：短时间连续文本消息按 session 合并后投递，接近 OpenClaw webhook 500ms debounce 和 Hermes 内置 wecom 的 text batching；媒体消息即时投递。
- 新增 `callback.py`：迁移自建应用 HTTP callback 的签名校验、AES-256-CBC 解密、outer XML `<Encrypt>` 提取、解密后消息 XML 解析。
- `adapter.py` 新增可选自建应用 HTTP callback listener：支持 GET URL 验证、POST 签名/时间戳/AES 解密、MsgId 去重、投递 Hermes message pipeline。
- `adapter.py` 新增 callback MediaId 下载：image/voice/file/video 回调会通过企业微信 Agent API `media/get` 拉取内容，并缓存为 Hermes `media_urls` / `media_types`。
- `adapter.py` 新增 callback-mode 主动回复：按 `corp_id:user_id` 作用域记录 app，使用企业微信 Agent API `message/send` 回复，缓存 access token，并在 token 过期错误时刷新重试。
- 新增 `tests/test_adapter_events.py`，覆盖 auth event 投递、enter_chat 欢迎语和 disconnected_event displaced 状态。
- 扩展 `tests/test_streaming.py`，覆盖 ACK pending 时中间帧跳过、final 帧仍发送、keepalive、stream rotation 和超长完整内容兜底。
- 新增 `tests/test_callback.py`，覆盖 callback signature、timestamp tolerance、AES 解密、padding 错误、组合校验/解密/解析和 text/image/voice/file/video XML 解析。
- 扩展 `tests/test_adapter_events.py`，覆盖 `MEDIA:/FILE:` 指令提取、主动发送和流式 final 失败提示。

## 已有迁移覆盖

- 消息解析：支持 text/image/voice/file/video/mixed/quote/location/link、mentions、template_card_event、auth_change_event。
- 流式回复：`<think></think>` seed、120-360 字符句子分块、250ms idle flush、4 分钟 keepalive、5 分钟 stream rotation、85 个中间帧上限、20KB UTF-8 尾部截断、超长完整内容主动补发、846608 stream expired 标记和 fallback。
- 媒体：下载解密、URL/本地路径解析、MIME 类型检测、大小限制与降级、分块上传、主动媒体发送。
- 回复媒体指令：识别 `MEDIA:/path` / `FILE:/path` 指令行，发送后从可见文本移除，失败时返回可见错误摘要。
- Template Card：LLM 输出 JSON 检测、字段规整、主动/流式出站发送、缓存、template_card_event 后更新禁用态/选中态。
- 全局状态：WSClient registry、message state TTL/cap、reqId store、session chat info、account cleanup。
- 文本批处理：session-scoped rapid text batching、长分片更长等待、媒体消息不延迟。
- Callback 入站通道：签名校验、时间戳窗口、AES 解密、`Encrypt` 提取、回调消息 XML 解析、可选 aiohttp listener、Hermes dispatch、MsgId 去重、MediaId 下载缓存。
- Callback 出站回复：`corp_id:user_id` 会话作用域、access-token 缓存、Agent API `message/send` 主动文本回复。

## 尚未完全迁移的 OpenClaw 专属能力

- OpenClaw Core 的动态 agent routing、command allowlist、runtime telemetry、workspace template 不属于 Hermes platform adapter 的原生接口，本插件没有照搬。
- OpenClaw 的群聊 callback 细分字段如果企业微信后续扩展 XML schema，本插件目前仍按现有 self-built app 用户消息 schema 解析；AI Bot WebSocket 群聊仍完整走 WS 通道。
- OpenClaw 的独立 `dmContent` buffer 没有原样照搬；Hermes 版用 AI Bot WS `send_message` 分块补发完整内容。

## 验证

```bash
python3 -m py_compile adapter.py tests/test_streaming.py tests/test_adapter_events.py
python3 -m pytest tests/ -q
```

当前结果：`236 passed in 1.58s`。
