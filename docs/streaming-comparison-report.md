# WeCom 流式输出实现对比报告

## 背景

xwecom-hermes-plugin 的 BlockChunker 曾在 finalize 路径缺少可用的 drain API，导致消息输出一半就断。当前实现已经改成显式 `drain(cumulative_text)`，并补齐 idle flush、尾部截断、ACK pending 跳帧等对齐项。本报告保留差异分析，并标注当前仍未移植的能力。

---

## 一、三份实现的核心架构差异

| 维度 | openclaw 官方 TS (Webhook 模式) | openclaw 官方 TS (WS 模式) | 官方 Python SDK | xwecom-hermes-plugin |
|------|------|------|------|------|
| **连接方式** | HTTP 长轮询 (response_url + stream_refresh) | WebSocket 长连接 | WebSocket 长连接 | WebSocket 长连接 |
| **流式分块策略** | 由 OpenClaw Core 的 `dispatchReplyWithBufferedBlockDispatcher` 托管 | 同左 | 无（SDK 只提供 `reply_stream` 原语） | 插件层 BlockChunker 自行实现 |
| **coalesce 配置** | `blockStreamingCoalesce: { minChars: 120, maxChars: 360, idleMs: 250 }` | 同左 | N/A | `BLOCK_STREAM_MIN_CHARS=120, MAX_CHARS=360, IDLE_FLUSH=0.25s` |
| **chunk 配置** | `blockStreamingChunk: { minChars: 120, maxChars: 360, breakPreference: "sentence" }` | 同左 | N/A | 句子分隔符检查（尾部 20 chars 扫描） |
| **帧计数限制** | 无显式 frame cap（Core 内部管理） | 无显式 frame cap | 无 | MAX_INTERMEDIATE_FRAMES=85 |
| **内容长度限制** | STREAM_MAX_BYTES=20,480 (UTF-8 字节) | 同左 | 无限制（SDK 不做截断） | MAX_STREAM_CONTENT_LENGTH=20,480 |
| **finalize 方式** | streamStore.markFinished + pushFinalStreamReplyNow (response_url POST) | sendWeComReply(finish=true) via replyStream | reply_stream(finish=True) | _send_stream_reply_frame(finish=True) |
| **stream expired 处理** | fallbackMode=timeout → Agent 私信兜底 | StreamExpiredError → wsClient.sendMessage 主动发送 | 无处理 | turn.expired=True → 回退到 send() |
| **idle flush** | Core 内置 (blockStreamingCoalesce.idleMs=250) | 同左 | 无 | 250ms call_later → _idle_flush_send |
| **thinking 消息** | `<think></think>` placeholder | 同左 | 无内置 | `<think></think>` 对齐 |
| **内容累积模式** | 非累积（每次 deliver 追加 `\n\n` 到 `content`） | 非累积（`state.accumulatedText += payload.text`） | 无内置 | 累积（consumer 传入 cumulative text） |

---

## 二、每个方案的优缺点

### A. openclaw 官方 TS 实现

**优点：**
- ✅ 分块/合并逻辑由 OpenClaw Core 的 `dispatchReplyWithBufferedBlockDispatcher` 统一托管，插件只需提供 deliver 回调
- ✅ 有完善的超时兜底机制（6 分钟窗口检测 + Agent 私信降级）
- ✅ blockStreamingCoalesce 配置化（idleMs、minChars、maxChars 可通过 config 覆盖）
- ✅ Webhook 模式下有 response_url 主动推送 + stream_refresh 长轮询双通道
- ✅ 消息防抖聚合（同会话 500ms 内连发合并为一批）
- ✅ template_card 模式感知（流式输出中遮罩 JSON）
- ✅ dmContent 独立于 content 限制（超长回复可通过私信兜底全量投递）

**缺点：**
- ❌ 复杂度高（1366 行 monitor.ts + 900 行 helpers.ts + 541 行 state.ts）
- ❌ 强依赖 OpenClaw Core SDK（不可独立复用）
- ❌ Webhook 模式无法做真正的流式中间帧推送（依赖客户端 stream_refresh 轮询）

### B. 官方 Python SDK (wecom-aibot-python-sdk-async)

**优点：**
- ✅ 极简 API：`reply_stream(frame, stream_id, content, finish)` 一行搞定
- ✅ 无外部依赖（纯 asyncio + aiohttp + pyee）
- ✅ 易理解，学习成本低

**缺点：**
- ❌ **无任何分块/合并逻辑** — 只是 WebSocket 帧的薄封装
- ❌ 无 idle timeout、无 sentence boundary 检测
- ❌ 无 stream expired 处理
- ❌ 无 frame cap 限制
- ❌ 无内容长度截断（依赖调用方）
- ❌ 不维护，无法作为生产参考

### C. xwecom-hermes-plugin（我们的实现）

**优点：**
- ✅ BlockChunker 独立于 Core，可在任何 Python 环境使用
- ✅ 参数对齐官方（min=120, max=360, idle=250ms, sentence break）
- ✅ 有 idle flush 机制（250ms 强制刷出 partial buffer）
- ✅ 有 frame cap 保护（85 帧上限）
- ✅ 有 UTF-8 字节尾部截断（20KB 限制，对齐官方保留最新内容）
- ✅ 有 stream expired 检测和回退
- ✅ finalize 路径有 `chunker.drain(cumulative_text)`
- ✅ 中间流帧有 ACK pending 跳帧，final 帧强制发送
- ✅ 有 4 分钟 keepalive 和 5 分钟 stream rotation，主动避开企业微信 6 分钟硬限制
- ✅ final 内容超过 20KB 时主动分块补发完整回复

**差异：**
- 没有原样照搬 OpenClaw 的 dmContent buffer；Hermes 版通过 AI Bot WS 主动补发完整内容。
- 同会话纯文本防抖聚合已迁移为 adapter 内 session-scoped text batching；媒体消息不延迟。

---

## 三、Finalize 路径具体对比

### openclaw 官方 TS (WS 模式)

```
finishThinkingStream(ctx):
  1. 计算 finishText（优先级：visibleText > 卡片提示 > 媒体提示）
  2. 如果 streamExpired=false:
       try: sendWeComReply(frame, text, finish=true, streamId)
       catch StreamExpiredError → expired = true
  3. 如果 expired=true:
       wsClient.sendMessage(chatId, markdown) // 主动发送降级
```

**特点：** Core 的 buffered block dispatcher 已经确保所有文本在 deliver 中累积完毕，finalize 时 `state.accumulatedText` 就是完整内容，直接发 finish=true。

### openclaw 官方 TS (Webhook 模式)

```
startAgentForStream 结束时:
  1. streamStore.markFinished(streamId) — 标记 finished=true
  2. pushFinalStreamReplyNow(streamId):
       buildStreamReplyFromState(state, STREAM_MAX_BYTES)
       POST JSON 到 response_url
  3. 客户端下次 stream_refresh 拉到 finish=true + 最终内容
```

**特点：** Webhook 模式不存在"中间帧"概念 — content 是单次覆盖写入，最终一次性 POST。

### xwecom-hermes-plugin（我们的实现）

```
Hermes GatewayStreamConsumer:
  send(metadata.expect_edits=true)
    → 创建 native turn，返回 WeCom streamId 作为 Hermes message_id
  edit_message(message_id=streamId, cumulative_text, finalize=...)
    → send_stream_frame(...)

send_stream_frame(finalize=True):
  1. _cancel_idle_flush(turn)
  2. if turn.chunker is not None:
       drained = turn.chunker.drain(text)
       if drained is not None: text = drained
  3. 防重复：如果 final_text == turn.last_sent_content → 追加 ZWS
  4. _send_stream_reply_frame(turn, final_text, finish=True)
  5. turn.finalized = True; cleanup
```

**问题分析：**
- consumer 在 finalize 时传入的是完整累积文本，`drain(text)` 只负责判断是否还有未发尾部，并推进 emitted length。
- 如果 consumer 传入的 `text` 比 chunker 已经 emitted 的内容少（异常场景），`drain()` 返回 None，final_text 保持传入值，这是正确的 fallback。

---

## 四、Idle Flush / Coalesce 定时器对比

| 维度 | openclaw 官方 | xwecom-hermes-plugin |
|------|------|------|
| **机制** | Core 内部 idleMs 定时器（每次收到 token 重置计时） | 250ms `call_later`，每次 not-ready 都重置 |
| **触发条件** | 收到 token 后 250ms 无新 token → force-emit | chunker.should_emit 返回 False 时 arm → 250ms 后 fire |
| **重置行为** | 每次新 token 重置计时器 | 每次取消旧 timer 并重新 arm |
| **效果差异** | LLM 持续出 token（间隔<250ms）时不会触发 | 一旦 arm，250ms 后必定触发一次，之后如果还没到 min_chars 又不会重新 arm |

当前语义已经对齐官方：只有最后一次 not-ready 更新后持续 250ms 没有新内容，才 force emit `turn.pending_cumulative`。

---

## 五、Frame Cap & Content Length Limit 处理差异

| 维度 | openclaw 官方 (WS) | openclaw 官方 (Webhook) | xwecom-hermes-plugin |
|------|------|------|------|
| **Frame Cap** | 无显式限制（Core dispatcher 内部管理） | 无（Webhook 只有最终一帧） | 85 帧硬上限 (MAX_INTERMEDIATE_FRAMES) |
| **超帧后行为** | N/A | N/A | 静默积累，最终 finalize 帧发送全部内容 |
| **Content 字节限制** | 20,480 bytes (helpers.ts:STREAM_MAX_BYTES) | 同左 | 20,480 bytes (MAX_STREAM_CONTENT_LENGTH) |
| **截断方式** | `truncateUtf8Bytes` — 保留尾部，截断头部 | 同左 | `_truncate_to_bytes` — 保留尾部，截断头部 |
| **截断方向差异** | 保留最后 N 字节（用户看到最新内容） | 同左 | 已对齐 |

**状态：** 截断方向已对齐。
- 官方：`buf.subarray(buf.length - maxBytes)` — 保留尾部
- 我们：取 `encoded[len(encoded) - max_bytes:]`，再跳过 UTF-8 continuation bytes。

在流式场景下，因为每帧发送的是 **累积内容**（cumulative），且后面的内容是最新的，官方选择截断头部、保留尾部是合理的 — 用户看到的气泡始终显示最新内容。

但在实际场景中，**20KB 限制极少被触发**（120-360 chars per block × 85 frames ≈ 最多 30,600 chars ≈ ~90KB UTF-8 — 但这是累积的，每帧只发一次全量，最后一帧才可能接近 20KB）。而且流式场景每帧的累积 content 就是用户最终看到的全部内容，所以截断方向对用户体验影响较大。

---

## 六、推荐：BlockChunker 应该采用哪种设计模式

### 推荐方案：保持现有 BlockChunker 设计，但修复关键细节

**理由：**

1. **不应该依赖 Core dispatcher** — 官方 TS 实现能用 `dispatchReplyWithBufferedBlockDispatcher` 是因为它是 OpenClaw 生态的一部分。我们的插件是独立的 Hermes plugin，没有也不应该依赖 OpenClaw Core。

2. **BlockChunker 的"无状态 cumulative"模式是正确的** — consumer 每次传入完整的累积文本，chunker 只负责决定"是否该 emit"和"mark emitted 位置"。这比维护内部 buffer 更简单可靠。

3. **idle timer 已改为重置型** — 参考官方 Core 的 coalesce.idleMs 语义：每次收到新 token 时重置，只有真正"沉默 250ms"才 flush。

4. **finalize 路径已经正确** — `drain(cumulative_text)` 确保尾部内容不丢失。

---

## 七、具体改进建议

### 高优先级（影响正确性）

#### 1. UTF-8 截断方向

已修复为保留尾部。超长 cumulative content 在 20KB 限制时，用户看到的是最新输出而非开头。

#### 2. idle flush 重置语义

已修复为每次 cancel 旧 timer 并重新 arm。

### 中优先级（提升鲁棒性）

#### 3. 6 分钟主动超时检测

已按二开版策略实现：4 分钟 keepalive，5 分钟主动 finish old stream 并换新 `stream_id` 继续，避免依赖 846608 errcode 的被动检测。

#### 4. BlockChunker API

已统一为显式参数：

```python
def drain(self, cumulative_text: str) -> Optional[str]:
    """Return cumulative_text if there's pending content."""
    if len(cumulative_text) <= self._emitted_len:
        return None
    self._emitted_len = len(cumulative_text)
    return cumulative_text
```

finalize 路径直接 `drain(text)`。

### 低优先级（对齐最佳实践）

#### 5. 消息防抖聚合

已迁移为同 session 纯文本批处理：短时间连续文字合并后投递，媒体消息保持即时处理。

#### 6. 超长完整内容兜底

对于超长回复（如代码生成），20KB 限制可能导致 stream final 只显示尾部。当前 Hermes 版会在 final 被截断时，通过 AI Bot WS `send_message` 按 UTF-8 安全分块主动补发完整内容，达到 OpenClaw dmContent 兜底的同类效果。

---

## 总结

| 评价 | 结论 |
|------|------|
| 官方 TS 最佳 | 是，但强依赖 OpenClaw Core，不可直接移植 |
| Python SDK 可参考 | 否，太简陋，无分块逻辑 |
| 我们的设计方向 | ✅ 正确 — 独立 BlockChunker + idle flush + frame cap |
| 需要修复 | 已知高优先级细节已修复：UTF-8 尾部截断、idle flush 重置、BlockChunker 显式 drain、ACK pending 跳帧 |
| 需要增强 | 暂无已确认的流式正确性缺口 |

**核心结论：** xwecom-hermes-plugin 的流式架构设计方向是对的，参数和主要正确性细节已对齐官方。HTTP callback inbound 已在 adapter 内通过可选 aiohttp listener 接入；同会话短时间纯文本消息防抖聚合也已补齐。
