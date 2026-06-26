# xwecom Phase 2: 对齐 OpenClaw 官方插件

## 目标

将 xwecom-hermes-plugin 的实现完全对齐 `/Users/bilibili/.hermes/wecom-openclaw-plugin/src/` 中的官方 TypeScript 实现。

## 参考源码位置

- **OpenClaw 官方插件**: `/Users/bilibili/.hermes/wecom-openclaw-plugin/src/`
- **官方 Python SDK**: `/Users/bilibili/.hermes/wecom-aibot-python-sdk-async/aibot/`
- **Hermes 内置 WeCom adapter (参考)**: `/Users/bilibili/.hermes/hermes-agent/plugins/platforms/wecom/adapter.py`
- **当前 xwecom 代码**: `/Users/bilibili/xwecom-hermes-plugin/`

## 需要完成的工作

### 1. 完善 message-parser (对标 `message-parser.ts` 317行)

当前 `adapter.py` 中的 `_parse_message_content` 过于简单。需要：

- 支持 quote 引用消息解析（提取被引用内容）
- 支持 file attachment 完整解析（filename, size, url, aes_key）
- 支持 voice 消息解析
- 支持 @mention 提取（被@的用户列表）
- 支持 location 消息
- 支持 link 消息
- 支持 mixed 消息的完整结构（多文本段+多图片+引用混合）

参考: `src/message-parser.ts` 的 `parseWeComMessageContent()` 函数

### 2. 完善 message-sender / stream (对标 `message-sender.ts` 172行 + `monitor.ts` stream部分)

当前 `stream.py` 只有 BlockChunker 和 session 管理。需要：

- **replyStreamNonBlocking 语义**: pending ACK 时跳过中间帧（不排队等待），只确保 final frame 必达
- **6分钟 stream 超时主动检测**: 启动定时器，超时后主动切换到 proactive `send_message`
- **stream errcode 处理**: 846608(expired) → fallback send, 846609(not subscribed) → reconnect
- **thinking message**: 首帧发送 `<think></think>` 占位符（对齐 OpenClaw 的 THINKING_MESSAGE 行为）
- **cumulative content 正确性**: 每帧都是全量内容（不是增量），与 WeCom 协议一致

参考: `src/message-sender.ts` 的 `sendWeComReply()` 和 `src/monitor.ts` 的 `replyStream` 调用逻辑

### 3. 完善 media-uploader (对标 `media-uploader.ts` 495行)

当前 `media.py` 的 upload 是推测实现。需要：

- 对照 OpenClaw `src/media-uploader.ts` 验证分块上传协议的正确字段名和流程
- 实现 `resolveMediaFile`: 从 URL 或本地路径加载媒体
- 实现 `applyFileSizeLimits`: 完整的大小限制+降级逻辑（image>10MB→file 等）
- 实现通过 SDK 的 `uploadMedia` / `sendMediaMessage` 正确发送
- voice 格式验证（只接受 AMR）
- 错误处理和重试

参考: `src/media-uploader.ts` 的完整流程

### 4. 新增 template-card-manager (对标 `template-card-manager.ts` 295行 + `template-card-parser.ts` 731行)

当前完全缺失。需要：

- Template Card 检测（从 LLM 输出中识别 card JSON）
- Template Card 发送（text_notice, button_interaction, news_notice 等类型）
- Template Card 事件更新（用户点击按钮后更新卡片内容）
- Template Card 在 reply_stream 中的处理（stream 结束后追加 card）

参考: `src/template-card-manager.ts` 和 `src/template-card-parser.ts`

### 5. 完善 monitor 核心循环 (对标 `monitor.ts` 1176行)

当前 `adapter.py` 的 `_on_message` 太简单。需要对齐 monitor.ts 的完整流程：

- **buffered block dispatcher**: 收到消息后，不立即处理，先 buffer 一段时间看是否有后续消息（防抖）
- **enter_chat 欢迎消息**: 用户首次进入聊天时发送欢迎语
- **session recording**: 记录消息处理状态（用于重连后恢复）
- **消息超时保护**: 消息处理超过阈值告警
- **disconnect event 处理**: 收到 `disconnected_event`（被新连接踢下线）时的正确行为

参考: `src/monitor.ts` 的 `monitorWeComProvider()` 和 `processWeComMessageNow()`

### 6. 新增 state-manager (对标 `state-manager.ts` 369行)

- 全局消息状态跟踪（哪些消息正在处理、已完成、超时）
- reqId 分配和去重
- stream session 状态（映射 chatId → active stream）
- 连接状态跟踪

### 7. 测试更新

每个新增/修改的模块都需要对应测试。保持当前 48 个测试全部通过的前提下新增。

## 约束

1. **保持 Hermes 插件接口不变** — `register(ctx)` 和 `XWeComAdapter` 的公开接口不能破坏
2. **保持现有 48 个测试通过**
3. **Python 3.10+** 语法
4. **不引入新的第三方依赖**（websockets/aiohttp/pyee/cryptography 已够用）
5. **所有和 OpenClaw 对齐的逻辑需要在代码注释中标注** `# Aligned with OpenClaw: <file>:<function>`

## 验证

完成后运行:
```bash
cd /Users/bilibili/xwecom-hermes-plugin && python3 -m pytest tests/ -v
```

所有测试必须通过。
