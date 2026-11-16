# xwecom 实现说明

本文描述 v2 分支当前的实现：插件如何把企业微信 AI 机器人「伪装」成一个支持编辑的 IM，让 Hermes 走通用的编辑式流式路径，同时在企微上呈现为「一轮对话一个气泡」。

## 1. 背景与目标

企微机器人没有「编辑已发消息」的接口，只有回复流（`aibot_respond_msg` + `msgtype: stream`）：同一个 `stream.id` 的内容可以被反复替换，直到发出 `finish: true` 或者满 10 分钟。

Hermes 内置的 wecom 适配器为此在 core 里维护了一条 WeCom 专用的「原生流式」路径。我们之前的 13 个 core 补丁（工具计时、10 分钟换泡、ACK 重发）也都改在那里，上游难以合入。

v2 的思路是**在插件里补齐能力，不改 core**：
- 对 Hermes 声明 `SUPPORTS_MESSAGE_EDITING = True` 和 `REQUIRES_EDIT_FINALIZE = True`，Hermes 就走 Telegram/Slack 共用的编辑路径（`send` → `edit_message` → finalize）。
- 插件内部把每一次 send/edit 画到企微的流式气泡上。

验收标准与迁移前（core 补丁版）一致：
- 工具调用不单独成泡；
- 气泡永远不会卡住；
- 一轮对话的思考、工具、正文都在同一个气泡里。

## 2. 实测的企微行为（2026-11 探测）

| 行为 | 结论 |
|---|---|
| 同一条入站消息（同一 req_id）下同时开多个流 | 可以，交替更新、分别关闭都正常 |
| 单个流的帧数 | 无上限（2 帧/秒连发 120 帧全部成功） |
| 流的寿命 | 首帧后 600 秒报 `846608`，到期后连 `finish` 帧也会被拒 |
| 更新已 finish 的流 | 返回 `errcode 0`，但**不会显示** |
| 单帧大小 | 20480 字节 |
| ACK | 每一帧（包括非结束帧）都有一个只带 req_id 的 ACK；并发下是否严格按发送顺序，**未验证** |
| 模板卡片按钮 | 点击产生 `aibot_event_callback` / `template_card_event`，带 `event_key` 和 `task_id` |
| 连接 | 一个 bot 只能有一条长连接，第二条会把第一条踢下线（`disconnected_event`） |

## 3. 模块结构

```
xwecom/
├── adapter.py      连接、心跳、入站解析、准入策略、register()
├── emulation.py    Hermes 看到的「可编辑 IM」表面：send / edit_message / delete_message / 审批卡片
├── turn_bubble.py  本轮气泡：开/关、send 的归属判断、状态行、补发
├── turn.py         本轮气泡的纯状态（无 I/O）：正文分段、工具、思考、通知、渲染规则
├── messages.py     虚拟消息 → 企微流式气泡链（10 分钟换泡、超长分泡、overlay 不入封）
├── streaming.py    单帧发送与 ACK 跟踪（按 req_id 的 FIFO）
├── rescue.py       连接断开时未送达回复的暂存（按 bot 存，跨适配器实例）
├── hooks.py        pre_tool_call / post_tool_call / pre_api_request → 对应会话的本轮气泡
├── media.py        入站媒体解密、出站上传
├── send_queue.py   每个会话的 FIFO + 30 条/分钟令牌桶
├── gateway_env.py  环境变量 → 配置（XWECOM_* 优先，回退 WECOM_*）
└── compat.py       插件 API 未公开的 core 内部名字的本地替代/带回退的访问
```

## 4. 虚拟消息与气泡链（messages.py）

一个 Hermes 消息 id 对应一个 `VirtualMessage`，背后是一串企微气泡。只有最后一个气泡可以是活的。

- **编辑**：用正文中属于当前气泡的部分替换它，即上一个气泡封口点之后的内容。
- **10 分钟换泡**：气泡活到 540 秒时，由 ticker 封口（加 `⏬`），再接一个新气泡。对 Hermes 来说还是同一个 id。
- **超长分泡**：正文超过约 19000 字节时，封掉满的部分，接着在新气泡里写。
- **关闭后再编辑**：在新气泡里继续，不会被企微静默丢弃。如果改动了已封口的文字，新气泡显示完整的新文本。
- **overlay**：状态区以 overlay 的形式画在正文下方（`\n\n---\n` 分隔），**永远不会被封进旧气泡**。

## 5. 本轮气泡（turn_bubble.py + turn.py）

### 5.1 生命周期

**打开**：取以下最先发生者，并且打开是幂等的。
- `on_processing_start(event)`；
- 第一次 `pre_api_request` / `pre_tool_call`；
- 该轮第一条带标记（`notify` 或 `expect_edits`）的回复。

例外：`reply_expected is False` 或内部唤醒的消息不开气泡。没有画任何东西的轮次不发帧，不会留下空气泡或「✅」。

**换手**：只有 `on_processing_start`（同一会话的后续消息真正开始）会冻结或关闭上一轮：
- 已送达答案或什么都没画 → 立即关闭；
- 正在输出答案 → 冻结到它自己的 `on_processing_complete`。

由回复或 hook 触发的兜底打开**永远不会**关闭别的轮次。

**关闭**：任一条件触发即关闭。
- `on_processing_complete`；
- 最终回复（`notify`）发出后 3 秒内没有新的模型调用、工具或正文（`SETTLE_AFTER_FINAL_SECONDS`）；
- 回复已经稳定且空闲 20 秒；
- 无事件、无运行中工具超过 40 分钟（运行中的工具算活动）；
- 适配器断开连接（见 §7）。

### 5.2 状态区与渲染（turn.py）

状态区画在正文下方，每秒重画一次：

```
✓ 已完成的工具（最多 5 个）
⠹ 运行中的工具 (Ns)               ← 标签优先用 Hermes 工具进度行的文字，如「🔍 Searching the web for X」
⠹ 💭 Thinking (Ns)               ← 没有运行中工具时显示，计时从最近一次 pre_api_request 起，不显示模型名
状态通知（最多 3 条）              ← 心跳、忙碌提示、Hindsight 等
```

**正文到达即清空工具行**：
- 正文变长时，**之前各轮模型调用**的工具行（运行中和已完成的都算）全部隐藏。
- 写了一段正文后再调工具，这一轮的工具会出现在正文下方；正文继续时再清空。
- 分轮清理的原因：stream consumer 在独立任务里送正文，同一次模型调用的正文可能比它自己的第一个工具晚一点点到达。若一律全清，会误删这一轮刚开始的工具。

**最终气泡**：只有正文，加上「持久」通知（目前只有审批超时）。没有 ✓ 工具列表。

### 5.3 send 的归属（`_send_kind`）

原则是保守：只有能证明属于本轮的 send 才并入，拿不准就按旧路径单独发。多一个气泡无害，内容并错、被压成临时行或丢失才是问题。

| 来源 | 识别方式 | 去向 |
|---|---|---|
| stream consumer 的正文 / 基础适配器的最终回复 | 锚定在本轮开场消息或已并入的消息上，且未标 `_interim_send` | 正文分段 |
| Hermes 工具进度通道 | 调用协程是 `TurnRunner.send_progress_messages`（`compat.core_send_lane`） | 运行中工具的标签 |
| agent 状态（如「👁️ Hindsight — recalled N memories」） | `send_or_update_status`，或调用协程是 `_send_or_update_status_coro` | 一条状态行，按 status_key 原地更新 |
| 心跳、卡住警告、审批超时、排空通知 | `_interim_send` 且文本完整匹配 core 对应 i18n 模板（字段按类型约束，如分钟数只能是数字） | 临时状态行 |
| 忙碌提示（↪ Redirected / ⏳ Queued / steer / interrupt） | 未标记、锚定在同一会话的新入站消息上，且文本匹配忙碌提示模板 | 临时状态行，同时把新消息并入本轮（重定向后的答案落在同一气泡） |
| 其他 `_interim_send`（澄清前的说明、审批卡前的完整命令、插件推送、答案尾巴） | — | 单独消息，不会被丢 |
| 审批提示、`force_proactive_send`、无锚定（cron / send_message）、锚定在其他会话或未知消息、锚定在别处的带标记 send（忙时的 `/status`） | — | 单独消息 |

**工具进度行与工具的匹配**（F2）：
- 每条进度消息首次发送时，记录它从第几个工具开始覆盖。`__reset__` 之后的新消息只覆盖后续工具。
- 一行只匹配以该工具自己前缀开头的调用，前缀为 `{emoji} {工具名}` 或 `{emoji} {动作词}`，和 core `_progress_build_message` 的拼法一致。
- `(×N)` 覆盖接下来 N 个同类调用。
- 匹配不上的行（思考文字、`/verbose` 提示、new 模式的空缺）不作标签；匹配不上的工具保留默认的 `🔧 工具名`，宁缺不错。

**群聊**：
- 入站时记录每条消息 → 会话键（`compat.source_session_key`，群聊里按发送者区分）。
- 同一群里有多个会话的轮次同时进行时，不猜，无锚定的 send 走单独消息。

## 6. 发送与 ACK（streaming.py）

- **中间帧一律即发即走**：状态 tick、正文、完整回复、finalize 都是。不等 ACK，也不会因为 ACK 状态被跳过或延后，所以 ACK 慢或丢都不会让气泡停住。
- **只有结束帧（finish）等 ACK**：15 秒超时后原样重发一次，仍无 ACK 记为「不确定」，按已送达处理（与迁移前一致）。
- **每个 req_id 一个 FIFO 等待队列**：所有发出的帧（流式帧、被动 markdown、被动媒体、审批卡片）都排队，每个 ACK 解决最老的那个等待者。
  - 超时的等待者留在队列里，迟到的 ACK 只会解决它自己，顺序不错位。
  - 超过 120 秒无 ACK 的队头会被丢弃。
  - 这只影响「是否送达」的判断，不影响画面。
- **没有「污染/拉黑」机制**：任何 ACK 问题都不会让中间帧停发。

## 7. 送达失败与补发（turn_bubble.py + rescue.py）

失败只从两处判断：本轮结束帧的 ACK，以及明确的错误码。

| 情况 | 处理 |
|---|---|
| 结束帧被拒：846604 / 846609 / 其他 errcode，或因连接断开没发出去 | 把没被已确认结束帧证明上屏的部分（`body[confirmed_upto:]`）用主动 markdown 补发 |
| 846608（流过期） | 在新气泡里接着显示 |
| 结束帧无 ACK（不确定） | 视为已送达 |
| 连接已断开 | 补发内容存入 `rescue.py`（按 bot id，每个 bot 最多 50 条），下一次连接成功后按顺序发出。Hermes 重连会新建适配器对象，所以不存在适配器上 |
| 适配器断开（`disconnect()` / 监听循环出错） | 关闭所有进行中的轮次，未证实上屏的内容进暂存 |

已知取舍：
- **可能重复一小段前文**：中间帧不确认，回复上下文在流式中途失效时，补发可能重复已经显示过的一段前文。宁可重复，不可丢失。
- **暂存只在内存里**：进程内重连能保住，网关进程重启会丢失，因此也不会在重启后突然冒出旧答案。

## 8. 审批卡片（emulation.py）

`send_exec_approval` 渲染为 `button_interaction` 模板卡片，按钮的 `key` 是 `once` / `session` / `always` / `deny`。

点击后：
1. 收到 `template_card_event`，通过公开 API `tools.approval.resolve_gateway_approval_card` 解决对应的请求（带点击人身份校验）；
2. 用 `aibot_respond_update_msg` 把卡片原地改成「✓ 选择」。

## 9. 配置与部署

- **插件名 `xwecom`**，独立于内置的 `wecom`。同一个 bot 只能被一个适配器连接，启用 xwecom 前必须关闭 wecom。
- **凭证**：先读 `XWECOM_*`，没有则回退到 `WECOM_*`。
- **display 档位**：`display_tier="low"` + `display_overrides={"streaming": True}`，与内置 wecom 相同。profile 顶层的 `display.*`（如 `tool_progress: all`）会覆盖档位，工具进度和状态消息会并入本轮气泡。
- **用户插件需要显式启用**：`plugins.enabled: [xwecom]`。
- **部署注意**：插件目录里**不要留同名备份**，例如 `plugins/xwecom.prev-*`。它的 `plugin.yaml` 也叫 `xwecom`，会覆盖新插件被加载（踩过这个坑）。

## 10. 对 core 的依赖

直接 import 的只有 `hermes_cli/data/plugin_api.json` 里公开的名字。其他 core 内部名字都经过 `compat.py`，失效时退化为安全的旧行为：

| 依赖 | 用途 | 失效后果 |
|---|---|---|
| `send_progress_messages` / `_send_or_update_status_coro`（协程名） | 识别工具进度、状态通道 | 这些消息回到单独气泡，不丢内容 |
| `agent.i18n.t` 的状态/忙碌模板 | 识别心跳、忙碌提示 | 回退到本地英文副本；仍不匹配则单独成泡 |
| `BasePlatformAdapter._source_session_key` | 群聊按发送者区分会话 | 回退为 `chat` / `chat:user` |
| `agent.display.get_tool_verb` | 工具进度行的动作词前缀 | 动作词形式的行不作标签，工具保留默认标签 |

## 11. 测试

```bash
PY=<hermes test-environment python>
cd ~/xwecom-hermes-plugin && $PY -m pytest -q tests                 # 96 个单元测试
for f in load_check consumer_e2e hooks_approval_e2e turn_bubble_e2e turn_bubble_zh_e2e authority_e2e; do
  HERMES_DISABLE_LAZY_INSTALLS=1 PYTHONPATH=~/.hermes/hermes-agent $PY tests/e2e/$f.py
done
```

- **单元测试**：基于一个按实测行为建模的假企微。
- **`authority_e2e.py`**：用真实的 `GatewayRunner` 加会话权威通道（真实网关走的路径）跑完整一轮，只替换模型。覆盖：
  - 工具进度并入本轮气泡；
  - ACK 迟到 20 秒时气泡仍在刷新；
  - 重定向和排队；
  - 结束帧被 846609 拒绝时补发；
  - 断线重连后补发；
  - 两轮工具与正文交替。
- **`turn_bubble_zh_e2e.py`**：中文界面下的状态模板匹配。
- **`HERMES_DISABLE_LAZY_INSTALLS=1`**：必需，否则 core 会为临时 `HERMES_HOME` 重新拉起运行时，测试挂住。

## 12. 已知限制 / 待办

- **H1**：快速工具前的一句短开场白如果晚到，可能触发 3 秒封泡，答案接到第二个气泡（不丢内容）。
- **H3**：群聊里断线暂存的回复，重连后可能因为没有可用的被动回复上下文而发不出去，会一直留在暂存里（上限 50 条）。
- **ACK 顺序**：并发下企微是否严格按发送顺序回 ACK 未验证；即使乱序，也只影响送达判断。
- **同名工具并行**：同一条进度消息里两个同名工具按开始顺序贴标签，并行时预览可能互换。
- **已知缺口**：流式气泡链尚未关闭时，如果编辑改动了已经封口的前缀（超长分泡或换泡之后），显示可能错乱。正常的 Hermes 流只追加，不会触发（`messages.py` 中标注 `KNOWN GAP`）。
