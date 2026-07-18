# xwecom 迁移状态记录

更新时间：2026-07-18

## 参考来源

- `/Users/bilibili/.hermes/workspace/wecom-openclaw-plugin`：官方 OpenClaw TypeScript 插件，主要参考 `src/monitor.ts`、`src/message-parser.ts`、`src/message-sender.ts`、`src/media-uploader.ts`、`src/state-manager.ts`、`src/template-card-manager.ts`。
- `/Users/bilibili/.hermes/openclaw-plugin-wecom`：个人二开版本，主要参考 `wecom/ws-monitor.js` 的 keepalive、stream rotation、callback/event 修复，以及测试集中覆盖的 issue fixes。
- `/Users/bilibili/.hermes/wecom-aibot-python-sdk-async`：官方 Python SDK，作为 `sdk/` WebSocket、API、AES 下载解密、基础类型定义的迁移来源。

## 本轮补齐

- 明确 OpenClaw 与 Hermes 网关边界：OpenClaw 插件拥有 LLM dispatch，通过 buffered dispatcher 直接调用同一 `streamId` 的 `replyStream`；官方插件没有 `SUPPORTS_MESSAGE_EDITING`，该能力标记属于 Hermes platform adapter。
- 新增 Hermes 适配层：Bot WS 在线时动态声明 `SUPPORTS_MESSAGE_EDITING=True` 和 `REQUIRES_EDIT_FINALIZE=True`；`send(metadata.expect_edits=true)` 创建企微 stream 并返回官方语义的 `streamId`，`edit_message()` 将 Hermes 累计编辑映射为 `replyStream(finish=false/true)`。
- Agent HTTP callback 或 Bot WS 不可用时动态关闭编辑能力；混合模式下 callback chat 会拒绝 editable preview，让 Hermes 只发送一次最终回复，避免 partial/duplicate 消息。
- 建立 `message_id -> 原始 frame/req_id` 映射；Hermes 最终回复携带 `reply_to=message_id` 时使用 SDK `reply_stream(..., finish=True)` 被动回复。没有入站锚点的 cron/通知才使用 `aibot_send_msg` 主动发送。
- 补齐 Hermes 实际调用的 `send_image`、`send_image_file`、`send_document`、`send_voice`、`send_video`，支持被动媒体回复与 Agent HTTP callback 路径。
- 入站 WS 图片、文件、视频统一下载缓存，保留 MIME、`raw_message`、`reply_to_text` 和引用消息 ID。
- WebSocket 连接等待 `authenticated` ACK 后才标记 ready；认证错误和超时会让连接失败并上报 fatal error。
- `dm_policy=pairing` 不再在 adapter 内按 allowlist 提前丢弃，而是放行到 Hermes 核心生成/校验配对码；同时声明 adapter 自己执行 allowlist，避免 Hermes 对 config allowlist 二次拒绝。
- Callback/Agent 文本按 UTF-8 字节边界分块，不再使用 `content[:2048]` 静默截断。
- Bot WS 文本或媒体发送不可用时，自动降级到已配置的 Agent HTTP API；Agent 群聊目标使用 `appchat/send`。
- Hermes `send_typing()` 使用企微 thinking stream 显示输入状态；首个 editable send 会接管同一 `stream_id`，最终 `edit_message(finalize=true)` 关闭占位。

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
- 流式/被动回复：Hermes Gateway 仍使用通用 `GatewayStreamConsumer`；adapter 将其 `send/edit_message` 契约转换为官方 `replyStream` 生命周期。非流式最终文本继续绑定入站 `req_id` 被动回复，两条路径均按 UTF-8 字节限制处理内容。
- 媒体：下载解密、URL/本地路径解析、MIME 类型检测、大小限制与降级、分块上传、主动媒体发送。
- 回复媒体指令：识别 `MEDIA:/path` / `FILE:/path` 指令行，发送后从可见文本移除，失败时返回可见错误摘要。
- Template Card：LLM 输出 JSON 检测、字段规整、主动/流式出站发送、缓存、template_card_event 后更新禁用态/选中态。
- 全局状态：WSClient registry、message state TTL/cap、reqId store、session chat info、account cleanup。
- 文本批处理：session-scoped rapid text batching、长分片更长等待、媒体消息不延迟。
- Callback 入站通道：签名校验、时间戳窗口、AES 解密、`Encrypt` 提取、回调消息 XML 解析、可选 aiohttp listener、Hermes dispatch、MsgId 去重、MediaId 下载缓存。
- Callback 出站回复：`corp_id:user_id` 会话作用域、access-token 缓存、Agent API `message/send` 主动文本回复。

## 尚未完全迁移的 OpenClaw 专属能力

- 官方 Bot HTTP webhook（加密 JSON、`stream_refresh` 轮询状态机）尚未移植；当前 HTTP callback 是自建应用 Agent 的加密 XML 通道。Hermes 若需要该模式，应实现独立 transport，并让 adapter `send()` 更新 webhook stream store。
- 官方 `network.timeoutMs/retries/retryDelayMs/egressProxyUrl` 配置尚未移植；当前 WebSocket/HTTP 网络行为由 Python SDK、aiohttp 和进程环境决定。
- Hermes 工具进度轨道没有 finalize 信号，不能安全复用长期未完成的企微 stream；当前 `edit_message` 只编辑由 `expect_edits` 创建的 assistant stream，普通工具进度在编辑失败后按 Hermes 规则降级为独立完成消息。
- 多账号路由按当前需求暂缓；本轮只保留现有单账号和 callback app 配置兼容性。
- OpenClaw Core 的动态 agent routing、command allowlist、runtime telemetry、workspace template 不属于 Hermes platform adapter 的原生接口，本插件没有照搬。
- OpenClaw 的群聊 callback 细分字段如果企业微信后续扩展 XML schema，本插件目前仍按现有 self-built app 用户消息 schema 解析；AI Bot WebSocket 群聊仍完整走 WS 通道。
- OpenClaw 的独立 `dmContent` buffer 没有原样照搬；Hermes 版用 AI Bot WS `send_message` 分块补发完整内容。

## 验证

```bash
python3 -m py_compile adapter.py tests/test_streaming.py tests/test_adapter_events.py
python3 -m pytest tests/ -q
```

当前结果：`266 passed`，并已使用 Jarvis 实际安装版 Hermes 的
`GatewayStreamConsumer` 完成进程内 `send -> edit_message -> finish=true`
集成验证。
