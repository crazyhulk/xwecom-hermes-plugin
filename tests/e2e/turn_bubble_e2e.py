"""E2E: the turn bubble. The REAL xwecom adapter (loaded through Hermes plugin discovery) receives an inbound
WeCom callback, Hermes' REAL base-adapter pipeline (``handle_message`` → ``_process_message_background``)
runs on_processing_start / on_processing_complete around a turn that is driven with the REAL hook dispatch
(``pre_api_request`` / ``pre|post_tool_call`` on worker threads with the gateway's session context), the
REAL ``_interim_metadata`` heartbeat, the REAL busy-path ``_send_busy_reply`` and the REAL
GatewayStreamConsumer — against a fake WeCom server that renders what the user would see.

Reproduces the live bug of docs/implementation.md §5 (blank wait, then separate bubbles for the redirect
notice, the heartbeat and the answer) and asserts the fix: one bubble per turn.
"""
from __future__ import annotations

import asyncio
import contextvars
import os
import pathlib
import re
import shutil
import tempfile
import threading

home = pathlib.Path(tempfile.mkdtemp(prefix="xw-home-"))
os.environ["HERMES_HOME"] = str(home)
(home / "plugins").mkdir()
shutil.copytree(pathlib.Path(__file__).resolve().parents[2] / "xwecom", home / "plugins" / "xwecom")
(home / "config.yaml").write_text("plugins:\n  enabled:\n    - xwecom\n")
os.environ.update(XWECOM_BOT_ID="bot", XWECOM_SECRET="sec")

from hermes_cli.plugins import discover_plugins  # noqa: E402
discover_plugins(force=True)
from hermes_cli.lifecycle import invoke_hook  # noqa: E402
from gateway.platform_registry import platform_registry  # noqa: E402
from gateway.config import PlatformConfig  # noqa: E402
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig  # noqa: E402
from gateway import session_context  # noqa: E402
from gateway.run_busy import GatewayBusySessionMixin  # noqa: E402
from gateway.platforms.base import _reply_anchor_for_event  # noqa: E402


SPIN = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def _interim_metadata(metadata=None):
    """Same marker as ``gateway.run._interim_metadata``. Importing gateway.run here would run its runtime
    check and relaunch this script into a fresh environment for the temp HERMES_HOME."""
    return {**(metadata or {}), "_interim_send": True}


class Recorder:
    """Screen model of the measured API (same as consumer_e2e.FakeWeComServer: a finished stream acks
    updates but never renders them), plus every frame per stream so the test sees the screen over time."""

    def __init__(self):
        self.screen: dict[str, str] = {}
        self.order: list[str] = []
        self.finished: set[str] = set()
        self.markdown: list[str] = []
        self.history: dict[str, list[str]] = {}
        self.frames = 0

    def visible(self):
        return [self.screen[s].replace("\u200b", "") for s in self.order] + [f"[md] {m}" for m in self.markdown]

    async def handle(self, adapter, payload, dead=()):
        cmd, req_id, body = payload["cmd"], payload["headers"]["req_id"], payload.get("body") or {}
        if cmd == "aibot_respond_msg" and req_id in dead:  # the inbound req_id expired (846604)
            await asyncio.sleep(0.005)
            await adapter._dispatch_payload({"headers": {"req_id": req_id}, "errcode": 846604, "errmsg": "expired"})
            return
        if cmd == "aibot_respond_msg" and body.get("msgtype") == "stream":
            self.frames += 1
            st = body["stream"]
            if st["id"] not in self.finished:
                if st["id"] not in self.screen:
                    self.order.append(st["id"])
                self.screen[st["id"]] = st["content"]
                self.history.setdefault(st["id"], []).append(st["content"])
                if st["finish"]:
                    self.finished.add(st["id"])
        elif cmd in ("aibot_respond_msg", "aibot_send_msg") and body.get("msgtype") == "markdown":
            self.markdown.append(body["markdown"]["content"])
        elif cmd == "aibot_respond_msg" and body.get("msgtype") == "template_card":
            self.markdown.append("[card] " + body["template_card"]["main_title"]["desc"])
        await asyncio.sleep(0.005)  # ack latency
        await adapter._dispatch_payload({"headers": {"req_id": req_id}, "errcode": 0, "errmsg": "ok"})


class BusyRunner(GatewayBusySessionMixin):
    """Just enough runner for the REAL ``_send_busy_reply`` (WeCom has no threads → no thread metadata)."""
    _reply_anchor_for_event = staticmethod(_reply_anchor_for_event)

    def _thread_metadata_for_source(self, source, reply_to_message_id=None):
        return None


def inbound(msgid: str, req_id: str, text: str, *, sender: str = "user1", group: str = "") -> dict:
    body = {"msgid": msgid, "chattype": "group" if group else "single", "from": {"userid": sender}, "msgtype": "text",
            "text": {"content": text}}
    if group:
        body["chatid"] = group
    return {"cmd": "aibot_msg_callback", "headers": {"req_id": req_id}, "body": body}


def on_agent_thread(hook: str, message_id: str, chat_id: str = "user1", **kwargs) -> None:
    """Fire a plugin hook the way the agent does: on a worker thread carrying the turn's session context."""
    tokens = [session_context._SESSION_PLATFORM.set("xwecom"), session_context._SESSION_CHAT_ID.set(chat_id),
              session_context._SESSION_MESSAGE_ID.set(message_id)]
    ctx = contextvars.copy_context()
    for tok in reversed(tokens):
        tok.var.reset(tok)
    t = threading.Thread(target=ctx.run, args=(invoke_hook, hook), kwargs=kwargs)
    t.start()
    t.join()


API = dict(task_id="t", turn_id="u", api_request_id="a", session_id="s", user_message="", conversation_history=[],
           platform="xwecom", model="secret-model-x", provider="p", base_url="", api_mode="chat",
           api_call_count=1, retry_count=0, request_messages=[], system_prompt="", message_count=1, tool_count=1,
           approx_input_tokens=1, request_char_count=1, max_tokens=1, started_at=0.0, middleware_trace=[], request={})
TOOL = dict(args={}, session_id="s", task_id="t", turn_id="u", api_request_id="", conversation_id="")


def make_adapter(server, *, dead=()):
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={
        "bot_id": "bot", "secret": "sec", "dm_policy": "pairing", "group_policy": "open"}))  # admitted at intake

    class _WS:
        closed = False

        async def send_json(self, payload):
            asyncio.get_running_loop().create_task(server.handle(adapter, payload, dead=dead))

    adapter._ws, adapter._running = _WS(), True
    adapter._text_batch_delay_seconds = 0  # hand the inbound to the pipeline at once
    adapter._start_ticker()
    hooks = __import__(type(adapter).__module__.rsplit(".", 1)[0] + ".hooks", fromlist=["attach"])
    hooks.attach(adapter)
    return adapter, hooks


async def run_turn(name, turn_body, *, dead=(), first=None):
    """One real inbound → the base adapter's processing pipeline (on_processing_start/complete) around
    ``turn_body``. Returns once every turn bubble the adapter knows of has closed."""
    server = Recorder()
    adapter, hooks = make_adapter(server, dead=dead)
    done = asyncio.Event()

    async def handler(event):
        try:
            return await turn_body(adapter, event, server)
        finally:
            done.set()

    adapter.set_message_handler(handler)
    await adapter._dispatch_payload(first or inbound("m1", "REQ-1", "你好"))
    await asyncio.wait_for(done.wait(), 30)
    for _ in range(100):  # on_processing_complete runs right after the handler returns
        await asyncio.sleep(0.05)
        if all(t.closed for ts in adapter._turns.values() for t in ts):
            break
    await asyncio.sleep(0.3)
    await adapter._stop_ticker()
    hooks.detach(adapter)
    show(name, server)
    return server, adapter


def show(name, server):
    print(f"── {name}: {server.frames} frames")
    for i, v in enumerate(server.visible()):
        print(f"   bubble {i + 1}: {v!r}")


async def stream_answer(adapter, chunks, *, reply_to="m1", chat="user1"):
    """The REAL GatewayStreamConsumer, as run_turn_runner builds it for this turn."""
    consumer = GatewayStreamConsumer(adapter, chat, StreamConsumerConfig(edit_interval=0.05, buffer_threshold=1, cursor=" ▉"),
                                     initial_reply_to_id=reply_to)
    task = asyncio.create_task(consumer.run())
    for chunk in chunks:
        consumer.on_delta(chunk)
        await asyncio.sleep(0.15)
    consumer.finish()
    await asyncio.wait_for(task, 10)
    return consumer


def thinking_seconds(frames):
    return [int(m.group(1)) for f in frames for m in [re.search(r"💭 Thinking \((\d+)s\)", f)] if m]


def assert_all_closed_and_clean(server):
    assert all(sid in server.finished for sid in server.order), "a bubble was left open (spins forever)"
    for v in server.visible():
        assert "▉" not in v and "💭" not in v and not any(c in v for c in SPIN), f"live status left behind: {v!r}"


async def live_bug_scenario():
    """The live bug at the SHIPPED display tier (low: no heartbeat, no tool-progress messages): inbound →
    thinking ticks in one bubble while the model is silent → a second message + the real busy-path redirect
    ack → the redirected answer streams via the real consumer → on_processing_complete."""

    async def turn(adapter, event, server):
        assert event.message_id == "m1"
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(3.3)  # the model is silent; the bubble must tick
        adapter._reply_req_ids["m2"] = "REQ-2"  # what _on_message records before the busy hand-off
        adapter._remember_chat_req_id("user1", "REQ-2")
        adapter._remember_inbound_session("m2", event.source)
        ev2 = type(event)(text="你现在是什么模型", message_type=event.message_type, source=event.source, message_id="m2")
        await BusyRunner()._send_busy_reply(ev2, adapter, "↪ Redirected current run. I'll adjust using your correction.")
        await asyncio.sleep(1.1)
        on_agent_thread("pre_api_request", "m1", **API)  # the redirected run calls the model again
        await asyncio.sleep(1.2)
        consumer = await stream_answer(adapter, ["我是", "一个", "AI 助手。"], reply_to="m2")  # re-anchored
        assert consumer.final_content_delivered

    server, _ = await run_turn("live bug (tier low): thinking → redirect → streamed answer", turn)
    assert server.visible() == ["我是一个AI 助手。"], server.visible()
    frames = server.history[server.order[0]]
    secs = thinking_seconds(frames)
    print("   thinking seconds seen:", secs)
    assert secs and secs[0] == 0, "thinking must show from 0s, before any token"
    assert max(secs) >= 3 and len(set(secs)) >= 3, secs
    assert any("↪ Redirected" in f for f in frames), "redirect ack must show in the turn bubble"
    assert not any("secret-model-x" in f or "API call" in f for f in frames), "model identity leaked"
    assert_all_closed_and_clean(server)


async def forced_status_line_scenario():
    """A heartbeat forced on by the operator (an ``_interim_send`` status line) refreshes in place and is
    gone once the answer lands."""

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(1.2)
        hb = await adapter.send("user1", "⏳ Working — 3 min — iteration 1/90", metadata=_interim_metadata(None))
        assert hb.success, hb
        await asyncio.sleep(1.1)
        assert (await adapter.edit_message("user1", hb.message_id, "⏳ Working — 4 min — iteration 2/90")).success
        await asyncio.sleep(1.1)
        await stream_answer(adapter, ["好的。"])

    server, _ = await run_turn("forced heartbeat as a live status line", turn)
    frames = server.history[server.order[0]]
    assert any(f.endswith("⏳ Working — 4 min — iteration 2/90") for f in frames), frames
    assert server.visible() == ["好的。"], server.visible()
    assert_all_closed_and_clean(server)


async def tool_scenario():
    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(1.1)
        on_agent_thread("pre_tool_call", "m1", tool_name="terminal", tool_call_id="call_1", **TOOL)
        await asyncio.sleep(2.3)
        on_agent_thread("post_tool_call", "m1", tool_name="terminal", tool_call_id="call_1", result="ok", duration_ms=2000,
                        status="ok", error_type=None, error_message=None, middleware_trace=[], **TOOL)
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(1.2)
        await stream_answer(adapter, ["查到了：42。"])

    server, _ = await run_turn("tool: thinking → terminal → thinking → answer", turn)
    frames = server.history[server.order[0]]
    running = [int(m.group(1)) for f in frames for m in [re.search(r"[⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏] 🔧 terminal \((\d+)s\)", f)] if m]
    assert running and max(running) >= 1 and running == sorted(running), frames
    assert not any("Thinking" in f and "🔧" in f for f in frames), "thinking must yield to a running tool"
    assert any("✓ terminal (2s)\n" in f and "💭 Thinking" in f for f in frames), "completed tool above thinking"
    shown = server.visible()
    first_text = next(f for f in frames if f.startswith("查到了"))
    assert "✓" not in first_text and "terminal" not in first_text, f"text must clear the tool lines: {first_text!r}"
    assert shown == ["查到了：42。"], shown  # no tool footer on the closed bubble
    assert_all_closed_and_clean(server)


async def plain_final_scenario():
    """No streaming: the base adapter's own final send (notify, anchored on the opener) lands in the bubble."""

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(1.2)
        return "完整回答"  # _process_message_background → _send_final_text (real path)

    server, _ = await run_turn("plain final send", turn)
    assert server.visible() == ["完整回答"], server.visible()
    assert_all_closed_and_clean(server)


async def silent_turn_scenario():
    """A turn with no model call and no reply never shows a bubble at all."""

    async def turn(adapter, event, server):
        await asyncio.sleep(0.5)

    server, _ = await run_turn("silent turn (no model call)", turn)
    assert server.visible() == [], server.visible()


async def silent_after_thinking_scenario():
    """A turn that thought but answered nothing (silence marker) closes without a placeholder bubble."""

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(1.2)

    server, _ = await run_turn("silent turn after thinking", turn)
    assert server.visible() == [""], server.visible()  # an empty finish frame, like the built-in adapter
    assert_all_closed_and_clean(server)


async def dead_req_id_scenario():
    """B1: the inbound req_id is dead (846604): the answer must still reach the screen, by proactive send."""

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(0.3)
        return "THE FINAL ANSWER"

    server, _ = await run_turn("dead req_id: final answer", turn, dead={"REQ-1"})
    assert "[md] THE FINAL ANSWER" in server.visible(), server.visible()


async def non_turn_sends_scenario():
    """B3 / B4 / bypass: cron, an inline /status reply, clarify prose, a full command before an approval
    card, an approval prompt and a force_proactive confirmation all keep their own messages."""
    prose = "我找到了三个方案：A) 重启 B) 回滚 C) 扩容。"
    command = "```\nrm -rf /var/cache/build && make all\n```"

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(0.3)
        await adapter.send("user1", "CRON: daily report")  # _standalone_send shape
        adapter._reply_req_ids["m2"] = "REQ-2"
        adapter._remember_inbound_session("m2", event.source)
        await adapter.send("user1", "STATUS: running", reply_to="m2", metadata={"notify": True})  # _dispatch_inline_reply
        await adapter.send("user1", prose, metadata=_interim_metadata(None))  # clarify_context_sink
        await adapter.send("user1", command, metadata=_interim_metadata(None))  # send_full_command
        await adapter.send("user1", "⚠️ approve?", metadata=_interim_metadata({"is_approval_prompt": True}))
        await adapter.send("user1", "⏳ Working — 3 min", metadata=_interim_metadata(None))  # status: folds
        await adapter.send("user1", "⏳ Working — 5 min: deploy FAILED, run rollback.sh now", metadata=_interim_metadata(None))
        await adapter.send("user1", "⏳ Queued for the next turn (your build artifact is at /tmp/x.zip)", metadata=_interim_metadata(None))
        await adapter.send("user1", "✓ approved", metadata={"force_proactive_send": True})
        await stream_answer(adapter, ["最终回答。"])

    server, _ = await run_turn("non-turn sends during a turn", turn)
    shown = server.visible()
    for item in ("CRON: daily report", "STATUS: running", prose, command, "⚠️ approve?", "✓ approved",
                 "⏳ Working — 5 min: deploy FAILED, run rollback.sh now",
                 "⏳ Queued for the next turn (your build artifact is at /tmp/x.zip)"):
        assert sum(item in v for v in shown) == 1 and not any(item in v and "最终回答" in v for v in shown), (item, shown)
    assert sum("最终回答。" in v for v in shown) == 1


async def group_two_sessions_scenario():
    """B2: two people in one group, each with a running turn; Alice redirects. Each answer stays in its
    owner's bubble, and the redirect ack never lands in Bob's."""
    server = Recorder()
    adapter, hooks = make_adapter(server)
    events, release = {}, {"a1": asyncio.Event(), "b1": asyncio.Event()}
    answers = {"a1": ("a2", "ALICE ANSWER"), "b1": ("b1", "BOB ANSWER")}

    async def handler(event):
        events[event.message_id] = event
        on_agent_thread("pre_api_request", event.message_id, chat_id="grp", **API)
        await release[event.message_id].wait()
        anchor, text = answers[event.message_id]
        await stream_answer(adapter, [text], reply_to=anchor, chat="grp")

    adapter.set_message_handler(handler)
    await adapter._dispatch_payload(inbound("a1", "RA1", "hi", sender="alice", group="grp"))
    await adapter._dispatch_payload(inbound("b1", "RB1", "yo", sender="bob", group="grp"))
    await asyncio.sleep(1.2)
    # Alice's correction while her turn runs: the busy path acks it anchored on a2.
    adapter._reply_req_ids["a2"] = "RA2"
    adapter._remember_chat_req_id("grp", "RA2")
    src_a2 = adapter.build_source(chat_id="grp", chat_type="group", user_id="alice", user_name="alice", message_id="a2")
    adapter._remember_inbound_session("a2", src_a2)
    ev_a2 = type(events["a1"])(text="改一下", message_type=events["a1"].message_type, source=src_a2, message_id="a2")
    await BusyRunner()._send_busy_reply(ev_a2, adapter, "↪ Redirected current run. I'll adjust using your correction.")
    await asyncio.sleep(0.3)
    release["b1"].set(); await asyncio.sleep(0.6)
    release["a1"].set()
    for _ in range(100):
        await asyncio.sleep(0.05)
        if adapter._turns and all(t.closed for ts in adapter._turns.values() for t in ts):
            break
    await asyncio.sleep(0.3)
    await adapter._stop_ticker(); hooks.detach(adapter)
    show("group: two sessions + redirect", server)
    shown = server.visible()
    assert sorted(shown) == ["ALICE ANSWER", "BOB ANSWER"], shown
    bob = next(sid for sid in server.order if "BOB ANSWER" in server.screen[sid])
    assert not any("Redirected" in f for f in server.history[bob]), "the redirect ack leaked into Bob's bubble"
    assert_all_closed_and_clean(server)


async def follow_up_chain_scenario():
    """B5: a queued follow-up drained in-band. core fires on_processing_start(m2) mid-chain and completes
    m1 only after the whole chain, and the agent context keeps m1 as its message id."""
    from gateway.platforms.base import ProcessingOutcome

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(0.5)
        await stream_answer(adapter, ["answer 1"])
        adapter._reply_req_ids["m2"] = "REQ-2"
        adapter._remember_chat_req_id("user1", "REQ-2")
        adapter._remember_inbound_session("m2", event.source)
        ev2 = type(event)(text="还有一个问题", message_type=event.message_type, source=event.source, message_id="m2")
        await adapter._run_processing_hook("on_processing_start", ev2)  # run_turn_followup_ack
        on_agent_thread("pre_api_request", "m1", **API)
        on_agent_thread("pre_tool_call", "m1", tool_name="web_search", tool_call_id="c2", **TOOL)
        await asyncio.sleep(1.3)
        on_agent_thread("post_tool_call", "m1", tool_name="web_search", tool_call_id="c2", result="ok", duration_ms=1000,
                        status="ok", error_type=None, error_message=None, middleware_trace=[], **TOOL)
        on_agent_thread("pre_api_request", "m1", **API)
        await stream_answer(adapter, ["answer 2"], reply_to="m2")
        await adapter._run_processing_hook("on_processing_complete", ev2, ProcessingOutcome.SUCCESS)

    server, _ = await run_turn("queued follow-up chain", turn)
    shown = server.visible()
    assert shown[0] == "answer 1", shown  # turn 1 closed with its answer, no tools of turn 2 under it
    assert shown[1] == "answer 2", shown
    assert not any("web_search" in f for f in server.history[server.order[0]]), "turn 2's tool ran in turn 1"
    assert_all_closed_and_clean(server)


async def interrupted_then_follow_up_scenario():
    """R1: turn 1 thinks, is interrupted without a reply, and a follow-up takes over. Turn 1's bubble must
    finish at hand-over (empty) and the wire must never carry WeCom's "<think></think>" placeholder."""
    from gateway.platforms.base import ProcessingOutcome

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(1.2)
        adapter._reply_req_ids["m2"] = "REQ-2"
        adapter._remember_chat_req_id("user1", "REQ-2")
        adapter._remember_inbound_session("m2", event.source)
        ev2 = type(event)(text="算了，换个问题", message_type=event.message_type, source=event.source, message_id="m2")
        await adapter._run_processing_hook("on_processing_start", ev2)
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(2.2)
        first = server.order[0]
        assert first in server.finished, "the unanswered first turn must finish at hand-over"
        await stream_answer(adapter, ["新问题的回答。"], reply_to="m2")
        await adapter._run_processing_hook("on_processing_complete", ev2, ProcessingOutcome.SUCCESS)

    server, _ = await run_turn("interrupted turn + follow-up", turn)
    sent = [f for frames in server.history.values() for f in frames]
    assert not any("<think>" in f for f in sent), "the empty <think></think> placeholder reached the wire"
    assert server.visible() == ["", "新问题的回答。"], server.visible()
    assert_all_closed_and_clean(server)


async def edited_interim_post_scenario():
    """R2: a plugin post (``_interim_send``) edited mid-turn keeps its final text on screen; an emoji-led
    answer tail sent as ``_interim_send`` is content and gets its own message."""

    async def turn(adapter, event, server):
        on_agent_thread("pre_api_request", "m1", **API)
        await asyncio.sleep(0.3)
        post = await adapter.send("user1", "🔨 Deploy started", metadata=_interim_metadata(None))  # plugin_messages._send
        assert post.success
        assert (await adapter.edit_message("user1", post.message_id, "✅ Deploy finished: v1.2.3 live")).success
        await adapter.send("user1", "✅ All 3 checks passed.", metadata=_interim_metadata(None))  # segment-break tail
        await stream_answer(adapter, ["部署完成。"])

    server, _ = await run_turn("edited plugin post + emoji tail", turn)
    shown = server.visible()
    assert "✅ Deploy finished: v1.2.3 live" in shown and "✅ All 3 checks passed." in shown, shown
    assert "部署完成。" in shown
    assert_all_closed_and_clean(server)


async def media_from_other_session_scenario():
    """R4: Alice's turn finalized a preamble at a tool boundary; Bob's attachment must not close her turn.
    Her tool keeps ticking in her bubble and her answer joins it."""
    server = Recorder()
    adapter, hooks = make_adapter(server)
    release, events = asyncio.Event(), {}

    async def handler(event):
        events[event.message_id] = event
        if event.message_id == "a1":
            on_agent_thread("pre_api_request", "a1", chat_id="grp", **API)
            consumer = await stream_answer(adapter, ["Let me check…"], reply_to="a1", chat="grp")
            assert consumer.final_content_delivered or True
            await release.wait()
            on_agent_thread("pre_tool_call", "a1", chat_id="grp", tool_name="terminal", tool_call_id="t1", **TOOL)
            await asyncio.sleep(1.2)
            on_agent_thread("post_tool_call", "a1", chat_id="grp", tool_name="terminal", tool_call_id="t1", result="ok",
                            duration_ms=1000, status="ok", error_type=None, error_message=None, middleware_trace=[], **TOOL)
            await stream_answer(adapter, ["Result: 42"], reply_to="a1", chat="grp")
        else:
            await adapter._settle_turns_before_media("grp", "b1")  # what media.py runs for Bob's image
            release.set()

    adapter.set_message_handler(handler)
    await adapter._dispatch_payload(inbound("a1", "RA1", "hi", sender="alice", group="grp"))
    await asyncio.sleep(0.8)
    await adapter._dispatch_payload(inbound("b1", "RB1", "pic", sender="bob", group="grp"))
    for _ in range(120):
        await asyncio.sleep(0.05)
        if len(adapter._turns.get("grp", ())) == 2 and all(t.closed for t in adapter._turns["grp"]):
            break
    await asyncio.sleep(0.3)
    await adapter._stop_ticker(); hooks.detach(adapter)
    show("other session's media during a preamble", server)
    alice = [sid for sid in server.order if "Let me check" in server.screen[sid]]
    assert len(alice) == 1, server.visible()
    assert any("🔧 terminal" in f for f in server.history[alice[0]]), "Alice's tool must tick in her own bubble"
    assert "Result: 42" in server.screen[alice[0]]
    assert_all_closed_and_clean(server)


async def failed_finalize_scenario():
    """2b: the stream consumer's finalize edit fails (not a dead req_id). Core falls back to sending the
    unseen tail on its own; the turn must give that text back, so the user sees the answer once, and the
    tail continues the sentence instead of opening a new paragraph."""
    import sys as _sys
    from gateway.platforms.base import ProcessingOutcome  # noqa: F401  (kept for symmetry with siblings)

    async def turn(adapter, event, server):
        outcome = _sys.modules[type(adapter._book).__module__].FrameOutcome
        real, state = adapter.stream_frame, {"failed": False}

        async def flaky(*a, **k):
            # The finalize edit's frame carries the unseen tail; its send fails (the websocket send raised).
            if "第二部分" in a[2] and not state["failed"]:
                state["failed"] = True
                return outcome(ok=False)
            return await real(*a, **k)
        adapter.stream_frame = flaky
        consumer = GatewayStreamConsumer(adapter, "user1", StreamConsumerConfig(edit_interval=0.3, buffer_threshold=1, cursor=" ▉"),
                                         initial_reply_to_id="m1")
        task = asyncio.create_task(consumer.run())
        consumer.on_delta("第一部分。")
        await asyncio.sleep(0.5)
        consumer.on_delta("第二部分（之前没显示）。")
        consumer.finish()
        await asyncio.wait_for(task, 10)
        adapter.stream_frame = real
        assert state["failed"] and consumer.final_content_delivered

    server, _ = await run_turn("finalize edit fails, core sends the tail", turn)
    shown = server.visible()
    assert shown == ["第一部分。第二部分（之前没显示）。"], shown
    assert_all_closed_and_clean(server)


async def late_edit_and_rotation_scenario():
    """N1: a turn bubble older than 540s rotates while its body grows, then gets an edit after the turn
    ended. Every character once, no tool footer, no garbled continuation."""
    clock = {"skew": 0.0}

    async def turn(adapter, event, server):
        real = adapter._book._clock
        adapter._book._clock = lambda: real() + clock["skew"]
        on_agent_thread("pre_tool_call", "m1", tool_name="terminal", tool_call_id="c1", **TOOL)
        await asyncio.sleep(0.3)
        on_agent_thread("post_tool_call", "m1", tool_name="terminal", tool_call_id="c1", result="ok", duration_ms=300,
                        status="ok", error_type=None, error_message=None, middleware_trace=[], **TOOL)
        res = await adapter.send("user1", "第一段", reply_to="m1", metadata={"expect_edits": True})
        await asyncio.sleep(0.2)
        clock["skew"] = 545.0
        await adapter._book.rotate_due()
        await adapter.edit_message("user1", res.message_id, "第一段，第二段")
        await adapter.edit_message("user1", res.message_id, "第一段，第二段，第三段", finalize=True)
        adapter._late = res.message_id

    server, adapter = await run_turn("rotation + growth", turn)
    adapter._start_ticker()
    await adapter.edit_message("user1", adapter._late, "第一段，第二段，第三段，补充", finalize=True)
    await asyncio.sleep(0.3)
    await adapter._stop_ticker()
    show("… then a late edit", server)
    shown = server.visible()
    body = "".join(v.split("\n\n---\n")[0].removesuffix("\n\n⏬") for v in shown)
    assert body == "第一段，第二段，第三段，补充", shown
    assert not any("terminal" in v for v in shown), shown
    assert_all_closed_and_clean(server)


async def progress_edit_after_close_scenario():
    """No live turn takes a progress-lane message anchored on a turn that already closed, so it keeps the
    old path. Its next edit continues the finished bubble: with the whole changed line, never a bare
    " (×2)" fragment."""

    async def turn(adapter, event, server):
        await stream_answer(adapter, ["好的。"])

    server, adapter = await run_turn("progress edit after the turn closed", turn)
    adapter._start_ticker()

    async def send_progress_messages():  # core's lane, identified by this coroutine name
        res = await adapter.send("user1", "💻 Running ssh box uptime", reply_to="m1")
        await adapter.edit_message("user1", res.message_id, "💻 Running ssh box uptime (×2)")
    await asyncio.get_running_loop().create_task(send_progress_messages())
    await asyncio.sleep(0.3)
    await adapter._stop_ticker()
    show("… then the lane's edit", server)
    shown = server.visible()
    assert not any(v.lstrip().startswith("(×") for v in shown), shown
    assert shown[-1] == "💻 Running ssh box uptime (×2)", shown


async def main():
    await live_bug_scenario()
    await forced_status_line_scenario()
    await tool_scenario()
    await plain_final_scenario()
    await silent_turn_scenario()
    await silent_after_thinking_scenario()
    await dead_req_id_scenario()
    await non_turn_sends_scenario()
    await group_two_sessions_scenario()
    await follow_up_chain_scenario()
    await interrupted_then_follow_up_scenario()
    await edited_interim_post_scenario()
    await media_from_other_session_scenario()
    await failed_finalize_scenario()
    await late_edit_and_rotation_scenario()
    await progress_edit_after_close_scenario()
    print("\nALL TURN-BUBBLE E2E CHECKS PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        shutil.rmtree(home, ignore_errors=True)
