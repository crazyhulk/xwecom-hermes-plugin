"""E2E on the REAL session-authority path, end to end.

A real ``GatewayRunner`` with a real ``SessionAuthority`` (``initialize_session_authority``) against a temp
HERMES_HOME; the xwecom plugin loaded through Hermes' plugin discovery; a fake WeCom wire. An inbound
WeCom callback goes through the adapter's own intake → text batch → ``handle_message`` →
``GatewayRunner._handle_message`` → ``_hm_authority_admission`` → ``session_ingress.admit_message`` → the
durable FIFO → ``execute_admission`` → ``TurnRunner.run_sync`` on the executor thread. Only the model is
stubbed: ``_build_fresh_agent`` returns an agent whose ``run_conversation`` fires the REAL plugin hook
dispatch (``pre_api_request`` / ``pre_tool_call`` / ``post_tool_call`` through ``invoke_hook`` and
``_dispatch_pre_tool_call_hooks``) and the REAL callbacks the runner wired on it (tool progress, stream
deltas), sleeping like a slow model.

Asserted on the wire: thinking within ~1s of the inbound message and ticking, the tool in the same
bubble, the answer in the same bubble, the bubble finished, nothing spinning afterwards — and, with the
profile's display overriding the tier (``display.tool_progress: all``, as on the live test profile), the
tool-progress message folded instead of becoming its own bubble.

Run directly: ``HERMES_DISABLE_LAZY_INSTALLS=1 PYTHONPATH=<hermes-agent> python tests/e2e/authority_e2e.py``
(the env var stops core relaunching the script into a fresh runtime for the temp HERMES_HOME).
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import re
import shutil
import tempfile
import time

os.environ.setdefault("HERMES_DISABLE_LAZY_INSTALLS", "1")
home = pathlib.Path(tempfile.mkdtemp(prefix="xw-auth-"))
os.environ["HERMES_HOME"] = str(home)
(home / "plugins").mkdir()
shutil.copytree(pathlib.Path(__file__).resolve().parents[2] / "xwecom", home / "plugins" / "xwecom")
(home / "config.yaml").write_text(
    "plugins:\n  enabled:\n    - xwecom\n"
    "streaming:\n  enabled: true\n"
    "display:\n  tool_progress: all\n  interim_assistant_messages: true\n"  # the live profile's overrides
    # Core appends a one-time onboarding tip to the FIRST busy ack ever on a profile (agent/onboarding.py
    # BUSY_INPUT_FLAG); with it the ack is multi-line content and keeps its own message, by design. A profile
    # that has seen it once — every real one after its first busy message — sends the plain ack tested here.
    "onboarding:\n  seen:\n    busy_input_prompt: true\n")
os.environ.update(XWECOM_BOT_ID="bot", XWECOM_SECRET="sec", GATEWAY_ALLOW_ALL_USERS="true")

from hermes_cli.plugins import discover_plugins  # noqa: E402
discover_plugins(force=True)
from gateway.platform_registry import platform_registry  # noqa: E402
from gateway.config import PlatformConfig  # noqa: E402
from gateway.run import GatewayRunner, load_gateway_config_for_runner  # noqa: E402
from gateway.session_execution import GatewaySessionAgentMixin  # noqa: E402

SPIN = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


class Wire:
    """Screen model of the measured WeCom API, with every frame timestamped. Acks come back in send order
    (one per frame, req_id only). ``late_prefix``: the ack of the finish frame whose content starts with it
    arrives ``late_by`` seconds late — beyond the plugin's ack wait, as seen live (15:17:14: an awaited frame
    of another bubble on the turn's req_id, its ack ~5s after a 15s timeout), which used to poison the req_id
    and freeze the turn bubble."""

    def __init__(self, late_prefix=None, late_by=0.0, reject_finish=0):
        self.late_prefix, self.late_by, self.late_acks = late_prefix, late_by, 0
        self.reject_finish = reject_finish  # errcode acked for every stream FINISH frame (846609: kicked)
        self.down = False  # the socket dropped: the adapter's send_json raises
        self.drop_after = None  # drop the socket right after a non-finish frame containing this text lands
        self.adapter = None
        self._ack_q: asyncio.Queue = asyncio.Queue()
        self._pump = None
        self.t0 = time.monotonic()
        self.screen, self.order, self.finished, self.markdown = {}, [], set(), []
        self.history: dict[str, list[tuple[float, str]]] = {}

    def visible(self):
        return [self.screen[s].replace("​", "") for s in self.order] + [f"[md] {m}" for m in self.markdown]

    async def handle(self, adapter, payload):
        cmd, req_id, body = payload["cmd"], payload["headers"]["req_id"], payload.get("body") or {}
        rejected = bool(self.reject_finish and cmd == "aibot_respond_msg" and (body.get("stream") or {}).get("finish"))
        if cmd == "aibot_respond_msg" and body.get("msgtype") == "stream" and not rejected:
            st = body["stream"]
            if st["id"] not in self.finished:
                if st["id"] not in self.screen:
                    self.order.append(st["id"])
                self.screen[st["id"]] = st["content"]
                self.history.setdefault(st["id"], []).append((time.monotonic() - self.t0, st["content"]))
                if self.drop_after and self.drop_after in st["content"] and not st["finish"]:
                    self.down = True  # the whole answer is on screen; the close frame will not leave
                if st["finish"]:
                    self.finished.add(st["id"])
        elif body.get("msgtype") == "markdown":
            self.markdown.append(body["markdown"]["content"])
        delay = 0.005
        st = body.get("stream") or {}
        late = (self.late_prefix is not None and cmd == "aibot_respond_msg" and st.get("finish")
                and str(st.get("content", "")).startswith(self.late_prefix))
        if late:
            self.late_acks += 1
            delay = self.late_by
        await self._ack_q.put((asyncio.get_running_loop().time() + delay, adapter, req_id,
                               self.reject_finish if rejected else 0))
        if self._pump is None:
            self._pump = asyncio.get_running_loop().create_task(self._deliver())

    async def _deliver(self):
        while True:  # in order: one connection, an ack never overtakes an earlier frame's ack
            due, adapter, req_id, errcode = await self._ack_q.get()
            await asyncio.sleep(max(0.0, due - asyncio.get_running_loop().time()))
            await adapter._dispatch_payload({"headers": {"req_id": req_id}, "errcode": errcode,
                                             "errmsg": "x" if errcode else "ok"})


class SlowModelAgent:
    """The model stub. Everything around it — hook dispatch, the runner's callbacks (tool progress, status,
    stream deltas), the consumer, the progress lane — is real. One turn: recall status → model call (silent)
    → two web tools in parallel → model call → streamed answer."""

    def __init__(self, session_id):
        self.session_id, self.model, self.provider, self.platform = session_id, "stub-model", "stub", "xwecom"
        self.tools, self.max_tokens, self.base_url, self.api_mode = [], 1000, "", "chat"
        self.session_prompt_tokens = self.session_completion_tokens = self.session_output_tokens = 0
        self.tool_progress_callback = self.stream_delta_callback = self.tool_start_callback = None
        self.status_callback = None
        self._interrupt_message = None

    def __getattr__(self, name):  # any other knob the runner reads or sets on a real agent
        if name.startswith("__"):
            raise AttributeError(name)
        return None

    def interrupt(self, message=None, *args, **kwargs):
        self._interrupt_message = message

    def get_activity_summary(self):
        return {"api_call_count": 1, "max_iterations": 90, "current_tool": None, "last_activity_desc": ""}

    def _api_call(self, n):
        from hermes_cli.lifecycle import invoke_hook
        if self.tool_progress_callback:
            self.tool_progress_callback("llm.request_started", "_thinking_timer", f"stub (API call #{n})", None)
        invoke_hook("pre_api_request", task_id=self.session_id, turn_id=getattr(self, "_turn_id", "t"),
                    api_request_id=f"t:api:{n}", session_id=self.session_id,
                    user_message=getattr(self, "_user_message", ""), conversation_history=[], platform="xwecom",
                    model=self.model, provider=self.provider, base_url="", api_mode="chat", api_call_count=n,
                    retry_count=0, request_messages=[], system_prompt="", message_count=1, tool_count=0,
                    approx_input_tokens=1, request_char_count=1, max_tokens=1000, started_at=time.time(),
                    middleware_trace=[], request={})

    def _tool(self, name, call_id, args, preview):
        from hermes_cli.plugins import _dispatch_pre_tool_call_hooks
        if self.tool_progress_callback:
            self.tool_progress_callback("tool.started", name, preview, args, tool_call_id=call_id)
        _dispatch_pre_tool_call_hooks(name, args, middleware_trace=[], task_id=self.session_id,
                                      session_id=self.session_id, tool_call_id=call_id,
                                      turn_id=getattr(self, "_turn_id", "t"),
                                      api_request_id="t:api:1", conversation_id="")

    def _tool_done(self, name, call_id, args, seconds):
        from hermes_cli.lifecycle import invoke_hook
        invoke_hook("post_tool_call", tool_name=name, args=args, result="ok", task_id=self.session_id,
                    session_id=self.session_id, tool_call_id=call_id, turn_id="t", api_request_id="t:api:1",
                    conversation_id="", duration_ms=int(seconds * 1000), status="ok", error_type=None,
                    error_message=None, middleware_trace=[])
        if self.tool_progress_callback:
            self.tool_progress_callback("tool.completed", name, None, None, duration=seconds, tool_call_id=call_id)

    def run_conversation(self, message, **kwargs):
        if self.status_callback:  # the memory-recall indicator (agent._emit_status → status lane)
            self.status_callback("lifecycle", "👁️ Hindsight — recalled 24 memories")
        self._api_call(1)
        time.sleep(2.5)  # the model is silent: the bubble must tick "Thinking"
        search, extract = {"query": "herdr.dev"}, {"urls": ["https://herdr.dev"]}
        self._tool("web_search", "call_1", search, "herdr.dev")
        time.sleep(1.8)  # core's progress lane edits at most every 1.5s
        self._tool("web_extract", "call_2", extract, "https://herdr.dev")
        time.sleep(2.2)
        self._tool_done("web_extract", "call_2", extract, 1.8)
        self._tool_done("web_search", "call_1", search, 2.6)
        self._api_call(2)
        time.sleep(1.2)
        answer = ""
        for chunk in ["herdr", " 是一个", "终端里的", "代理管理器。"]:
            answer += chunk
            if self.stream_delta_callback:
                self.stream_delta_callback(chunk)
            time.sleep(0.3)
        return {"final_response": answer, "messages": [{"role": "user", "content": message},
                                                       {"role": "assistant", "content": answer}],
                "api_calls": 2, "completed": True}


async def run_turn(name, wire, msgid="m1", user="u1", side_send=None, agent_cls=None):
    agent_cls = agent_cls or SlowModelAgent
    GatewaySessionAgentMixin._build_fresh_agent = lambda self, *a, **k: agent_cls(self._ctx.session_id)
    runner = GatewayRunner(load_gateway_config_for_runner())  # the gateway's own config load (streaming on)
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={
        "bot_id": "bot", "secret": "sec", "dm_policy": "open"}))

    class _WS:
        @property
        def closed(self):
            return wire.down

        async def send_json(self, payload):
            if wire.down:
                raise ConnectionError("websocket closed")
            asyncio.get_running_loop().create_task(wire.handle(adapter, payload))

    adapter._ws, adapter._running = _WS(), True
    adapter.gateway_runner = runner
    runner.adapters = {adapter.platform: adapter}
    from gateway.session_authority import initialize_session_authority
    await initialize_session_authority(runner, profile_id="default", instance_id="owner")
    runner._wire_adapter_handlers(adapter)
    adapter._start_ticker()
    hooks = __import__(type(adapter).__module__.rsplit(".", 1)[0] + ".hooks", fromlist=["attach"])
    hooks.attach(adapter)
    wire.adapter = adapter
    assert runner.session_authority is not None, "no session authority: this would test the legacy path"
    sent_at = time.monotonic() - wire.t0
    await adapter._dispatch_payload({"cmd": "aibot_msg_callback", "headers": {"req_id": f"REQ-{msgid}"}, "body": {
        "msgid": msgid, "chattype": "single", "from": {"userid": user}, "msgtype": "text",
        "text": {"content": "帮我调研下 herdr.dev"}}})
    if side_send is not None:
        async def later():
            await asyncio.sleep(side_send[0])
            await adapter.send(user, side_send[1])  # cron / send_message: its own bubble, same req_id
        asyncio.get_running_loop().create_task(later())
    for _ in range(400):
        await asyncio.sleep(0.1)
        turns = [t for ts in adapter._turns.values() for t in ts]
        if turns and all(t.closed for t in turns) and not runner._is_session_running(f"agent:main:xwecom:dm:{user}"):
            break
    await asyncio.sleep(3.0)
    await adapter._stop_ticker()
    hooks.detach(adapter)
    print(f"── {name}")
    for sid in wire.order:
        frames = wire.history[sid]
        print(f"   bubble {sid[-6:]}: {len(frames)} frames, finished={sid in wire.finished}")
        for ts, content in frames:
            print(f"      +{ts - sent_at:5.1f}s {content!r}")
    for m in wire.markdown:
        print(f"   [md] {m!r}")
    return sent_at


def answer_bubble(wire):
    found = [sid for sid in wire.order if any("代理管理器" in c for _, c in wire.history[sid])]
    assert len(found) == 1, f"the answer must land in exactly one bubble: {wire.visible()}"
    return found[0]


def check_turn(wire, sent_at, *, tick_window):
    """What the user must see, whatever happened to acks."""
    bubble = answer_bubble(wire)
    frames = wire.history[bubble]
    thinking = [(ts - sent_at, int(m.group(1))) for ts, c in frames for m in [re.search(r"💭 Thinking \((\d+)s\)", c)] if m]
    assert thinking and thinking[0][0] < 2.0, f"thinking must appear within ~1s of the inbound (+batch delay): {thinking}"
    assert len({s for _, s in thinking}) >= 2, f"thinking must tick: {thinking}"
    # The status keeps redrawing through the tools: the elapsed seconds of a running tool advance.
    running = [int(m.group(1)) for ts, c in frames if tick_window[0] <= ts - sent_at <= tick_window[1]
               for m in [re.search(r"🔍 Searching the web for herdr\.dev \((\d+)s\)", c)] if m]
    assert len(set(running)) >= 2, f"the running tool line must keep ticking: {running}"
    final = wire.screen[bubble].replace("\u200b", "")
    assert final == "herdr 是一个终端里的代理管理器。", f"the closed bubble carries no tool footer: {final!r}"
    before = [c for _, c in frames if not c.startswith("herdr")]
    assert any("✓ Searching the web for herdr.dev (" in c and "✓ Reading https://herdr.dev (" in c for c in before), \
        "completed tools must show while there is no text yet"
    first_text = next(c for _, c in frames if c.startswith("herdr"))
    assert not any(m in first_text for m in ("✓", "Searching", "Reading", "💭")), \
        f"the first frame with text must drop the tool lines: {first_text!r}"
    assert all(s in wire.finished for s in wire.order), "a bubble was left open (spins forever)"
    others = [wire.screen[s] for s in wire.order if s != bubble] + wire.markdown
    assert not any("Searching" in o or "Reading" in o or "Hindsight" in o or "▉" in o for o in others), \
        f"tool progress / recall status must not get their own bubble: {others}"
    for v in wire.visible():
        assert "▉" not in v and "💭" not in v and not any(c in v for c in SPIN), f"live status left behind: {v!r}"
    return bubble


class BusyAgent(SlowModelAgent):
    """A turn long enough for a second message to arrive mid-run. ``redirect()`` is the real contract a
    running agent offers (agent/interrupt_control.py): accept the correction into the current turn."""
    _supports_active_turn_redirect = True

    def __init__(self, session_id):
        super().__init__(session_id)
        self.redirected = None

    def redirect(self, text):
        self.redirected = text
        return True

    def run_conversation(self, message, **kwargs):
        self._api_call(1)
        args = {"command": "sleep 4"}
        self._tool("terminal", "call_t", args, "sleep 4")
        time.sleep(4.0)  # the second message lands here
        self._tool_done("terminal", "call_t", args, 4.0)
        self._api_call(2)
        time.sleep(0.6)
        answer = f"ANSWER to {message.splitlines()[-1][:12]}" + (" (redirected)" if self.redirected else "")
        for chunk in (answer[:6], answer[6:]):
            if self.stream_delta_callback:
                self.stream_delta_callback(chunk)
            time.sleep(0.2)
        return {"final_response": answer, "messages": [{"role": "user", "content": message},
                                                       {"role": "assistant", "content": answer}],
                "api_calls": 2, "completed": True}


class TwoRoundAgent(SlowModelAgent):
    """Text, then a second tool round, then more text: the status area must clear on text, come back below
    the text for the second round (without the first round's tools), and clear again."""

    def _say(self, chunks):
        for chunk in chunks:
            if self.stream_delta_callback:
                self.stream_delta_callback(chunk)
            time.sleep(0.3)

    def run_conversation(self, message, **kwargs):
        self._api_call(1)
        args1, args2 = {"command": "ls"}, {"query": "herdr.dev"}
        self._tool("terminal", "call_a", args1, "ls")
        time.sleep(2.2)
        self._tool_done("terminal", "call_a", args1, 2.2)
        self._api_call(2)
        time.sleep(1.2)
        self._say(["先看", "一下。"])
        if self.stream_delta_callback:
            self.stream_delta_callback(None)  # core's tool boundary (agent/turn_tool_round.py)
        self._tool("web_search", "call_b", args2, "herdr.dev")
        time.sleep(2.6)
        self._tool_done("web_search", "call_b", args2, 2.6)
        self._api_call(3)
        time.sleep(1.2)
        self._say(["herdr", " 是一个", "终端里的", "代理管理器。"])
        answer = "herdr 是一个终端里的代理管理器。"
        return {"final_response": answer, "messages": [{"role": "user", "content": message},
                                                       {"role": "assistant", "content": answer}],
                "api_calls": 3, "completed": True}


def check_two_rounds(wire):
    bubbles = [s for s in wire.order if any("代理管理器" in c or "先看" in c for _, c in wire.history[s])]
    frames = [c.replace("\u200b", "") for s in bubbles for _, c in wire.history[s]]
    shown = [wire.screen[s].replace("\u200b", "") for s in bubbles]
    first = next(i for i, c in enumerate(frames) if c.startswith("先看"))
    assert any("✓ Running ls (" in c for c in frames[:first]), f"completed tool before any text: {frames[:first]}"
    # The text clears round 1. It may already carry round 2's own tool below it: the consumer delivers the
    # text on its own task, so it can land after the tool its model call started (real path, seen here).
    assert "ls" not in frames[first] and "✓" not in frames[first], f"text clears round 1: {frames[first]!r}"
    round2 = [c for c in frames[first:] if "web_search" in c or "Searching the web for herdr.dev" in c]
    assert round2, f"the second round must show below the text: {frames[first:]}"
    assert all(c.startswith("先看一下。\n\n---\n") for c in round2), round2
    assert not any("ls" in c for c in round2), f"round 1 came back: {round2}"
    assert any("✓ Searching the web for herdr.dev (" in c for c in round2), round2
    last_round2 = max(i for i, c in enumerate(frames) if c in round2)
    after = frames[last_round2 + 1:]
    assert after and all("Searching" not in c and "✓" not in c for c in after), after
    assert shown == ["先看一下。\n\nherdr 是一个终端里的代理管理器。"], shown
    assert not any("✓" in v or "Searching" in v for v in wire.visible()), wire.visible()
    assert all(s in wire.finished for s in wire.order), "a bubble was left open"


class QuickAgent(SlowModelAgent):
    def run_conversation(self, message, **kwargs):
        self._api_call(1)
        time.sleep(0.5)
        if self.stream_delta_callback:
            self.stream_delta_callback("好的，已在后台运行。")
        return {"final_response": "好的，已在后台运行。", "messages": [{"role": "user", "content": message},
                {"role": "assistant", "content": "好的，已在后台运行。"}], "api_calls": 1, "completed": True}


class InternalAgent(TwoRoundAgent):
    def _start_review_fork(self):
        import contextvars
        import threading
        ctx = contextvars.copy_context()  # propagate_context_to_thread
        # once while the turn is still settling, once after it closed
        threading.Thread(target=ctx.run, args=(_review_fork_run, self, (0.3, 6.0)), daemon=True).start()


    """The live 02:46 turn: a background-process completion woke the session. The same ssh twice (core's
    progress lane collapses it to "(×2)"), text, a tool boundary, another tool, text."""

    def run_conversation(self, message, **kwargs):
        assert "Background process" in message or "proc_" in message, message[:200]
        # What core's agent passes as pre_api_request(user_message=...): the persisted form of the message
        # (gateway/run_turn_prepare.py strips timestamps only), carrying core's internal-notification footer.
        self._user_message = kwargs.get("persist_user_message") or message
        from gateway.run_notifications import INTERNAL_NOTIFICATION_FOOTER
        assert INTERNAL_NOTIFICATION_FOOTER in self._user_message, "core no longer marks the wake"
        self._turn_id = f"{self.session_id}:internal:{id(self):x}"
        self._api_call(1)
        time.sleep(1.2)
        ssh = {"command": "ssh box uptime"}
        for call in ("call_s1", "call_s2"):
            self._tool("terminal", call, ssh, "ssh box uptime")
            time.sleep(1.0)
            self._tool_done("terminal", call, ssh, 1.0)
        self._api_call(2)
        time.sleep(1.2)
        self._say(["进程", "已完成。"])
        if self.stream_delta_callback:
            self.stream_delta_callback(None)
        args = {"command": "tail -n 5 log"}
        self._tool("terminal", "call_t", args, "tail -n 5 log")
        time.sleep(1.6)
        self._tool_done("terminal", "call_t", args, 1.6)
        self._api_call(3)
        time.sleep(1.0)
        self._say(["日志", "一切正常。"])
        self._start_review_fork()
        answer = "日志一切正常。"
        return {"final_response": answer, "messages": [{"role": "user", "content": message},
                                                       {"role": "assistant", "content": answer}],
                "api_calls": 3, "completed": True}


def _review_fork_run(agent, delays):
    """The background review fork (run_agent: ``propagate_context_to_thread``): the turn's session context
    (message id "", session key), its OWN turn_id, a review prompt; model call, memory tool, model call."""
    from hermes_cli.lifecycle import invoke_hook
    from hermes_cli.plugins import _dispatch_pre_tool_call_hooks
    for n, delay in enumerate(delays):
        time.sleep(delay)
        invoke_hook("pre_api_request", task_id="rv", turn_id=f"review:{n}", api_request_id=f"rv:{n}",
                    session_id=agent.session_id, user_message="Review the conversation above …",
                    conversation_history=[], platform="xwecom", model="m", provider="p", base_url="", api_mode="chat",
                    api_call_count=1, retry_count=0, request_messages=[], system_prompt="", message_count=1,
                    tool_count=0, approx_input_tokens=1, request_char_count=1, max_tokens=1, started_at=time.time(),
                    middleware_trace=[], request={})
        _dispatch_pre_tool_call_hooks("memory", {}, middleware_trace=[], task_id="rv", session_id=agent.session_id,
                                      tool_call_id=f"rv_call_{n}", turn_id=f"review:{n}", api_request_id="rv",
                                      conversation_id="")
        time.sleep(0.5)
        invoke_hook("post_tool_call", tool_name="memory", args={}, result="ok", task_id="rv",
                    session_id=agent.session_id, tool_call_id=f"rv_call_{n}", turn_id=f"review:{n}",
                    api_request_id="rv", conversation_id="", duration_ms=500, status="ok", error_type=None,
                    error_message=None, middleware_trace=[])


async def internal_turn(name, user):
    """A user turn, then core's watch-notification injection (``_inject_watch_notification`` →
    ``admit_process_notice`` on the authority path) of an ``internal`` event with no message id."""
    wire = Wire()
    agents = iter([QuickAgent, InternalAgent])
    GatewaySessionAgentMixin._build_fresh_agent = lambda self, *a, **k: next(agents)(self._ctx.session_id)
    runner = GatewayRunner(load_gateway_config_for_runner())
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={
        "bot_id": "bot", "secret": "sec", "dm_policy": "open"}))

    class _WS:
        closed = False

        async def send_json(self, payload):
            asyncio.get_running_loop().create_task(wire.handle(adapter, payload))

    adapter._ws, adapter._running = _WS(), True
    adapter.gateway_runner = runner
    runner.adapters = {adapter.platform: adapter}
    from gateway.session_authority import initialize_session_authority
    await initialize_session_authority(runner, profile_id="default", instance_id="owner")
    runner._wire_adapter_handlers(adapter)
    adapter._start_ticker()
    hooks = __import__(type(adapter).__module__.rsplit(".", 1)[0] + ".hooks", fromlist=["attach"])
    hooks.attach(adapter)
    key = f"agent:main:xwecom:dm:{user}"
    sent_at = time.monotonic() - wire.t0
    await adapter._dispatch_payload({"cmd": "aibot_msg_callback", "headers": {"req_id": f"REQ-{user}"}, "body": {
        "msgid": f"{user}-1", "chattype": "single", "from": {"userid": user}, "msgtype": "text",
        "text": {"content": "在后台跑一下 ssh 检查"}}})

    async def idle(limit):
        for _ in range(limit):
            await asyncio.sleep(0.1)
            turns = [t for ts in adapter._turns.values() for t in ts]
            if turns and all(t.closed for t in turns) and not runner._is_session_running(key):
                return
    await idle(200)
    user_frames = sum(len(v) for v in wire.history.values())
    evt = {"type": "watch_match", "event_id": f"evt-{user}", "session_id": "proc_4c7ecc8277d9", "session_key": key,
           "platform": "xwecom", "chat_type": "dm", "chat_id": user, "pattern": "done", "output": "done",
           # the process's owning conversation, as the process registry records it at spawn
           "parent_session_id": runner.session_store.lookup_by_session_key(key).session_id}
    text = "[IMPORTANT: Background process proc_4c7ecc8277d9 completed normally (exit code 0)]\ndone"
    accepted = await runner._inject_watch_notification(text, evt)
    assert accepted, f"core did not accept the injected notification: {accepted!r}"
    await asyncio.sleep(1.0)
    for _ in range(600):  # the internal turn has no lifecycle close: it settles on its own
        await asyncio.sleep(0.1)
        internal = [t for ts in adapter._turns.values() for t in ts if getattr(t, "internal", False)]
        if (internal or len(wire.order) > 2) and all(t.closed for t in internal) and not runner._is_session_running(key):
            break
    await asyncio.sleep(8.0)  # the review fork's second round lands after the close
    await adapter._stop_ticker()
    hooks.detach(adapter)
    print(f"── {name}")
    for sid in wire.order:
        frames = wire.history[sid]
        print(f"   bubble {sid[-6:]}: {len(frames)} frames, finished={sid in wire.finished}")
        for ts, content in frames:
            print(f"      +{ts - sent_at:5.1f}s {content!r}")
    for m in wire.markdown:
        print(f"   [md] {m!r}")
    return wire, adapter, user_frames


def check_internal_turn(wire, adapter):
    shown = real_bubbles(wire)
    assert shown[0] == "好的，已在后台运行。", shown
    rest = shown[1:]
    assert rest == ["🔔 后台任务\n\n进程已完成。\n\n日志一切正常。"], f"the internal turn must be ONE bubble: {rest}"
    assert not wire.markdown, wire.markdown
    bubble = next(s for s in wire.order if "日志一切正常" in wire.screen[s])
    frames = [c.replace("\u200b", "") for _, c in wire.history[bubble]]
    assert any("ssh box uptime" in c for c in frames), f"the internal turn's tool lines show: {frames}"
    assert not any(c.strip().startswith("(×") for c in frames), "a bare (×2) fragment"
    assert not frames[0].startswith(("⠋ 💭", "<think>")), f"no thinking placeholder before a tool or text: {frames[0]!r}"
    assert all(s in wire.finished for s in wire.order), "a bubble was left open"
    assert not any("Thinking" in c or "memory" in c for c in frames), f"review fork / thinking in the bubble: {frames}"
    assert not any(not t.closed for ts in adapter._turns.values() for t in ts), "a turn is still live (spinning)"
    internal = [t for ts in adapter._turns.values() for t in ts if getattr(t, "internal", False)]
    users = [t for ts in adapter._turns.values() for t in ts if not getattr(t, "internal", False)]
    assert len(internal) == 1 and internal[0].session == users[0].session, \
        f"the hook session key must match the user turn's: {[t.session for t in internal + users]}"


async def busy_turns(name, mode, user):
    """m1 runs; m2 arrives through the REAL WeCom intake while m1 is busy; core's busy path handles it in
    *mode* (interrupt → redirect, queue → a queued follow-up). Returns the wire."""
    wire = Wire()
    GatewaySessionAgentMixin._build_fresh_agent = lambda self, *a, **k: BusyAgent(self._ctx.session_id)
    runner = GatewayRunner(load_gateway_config_for_runner())
    runner._busy_input_mode = mode
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={
        "bot_id": "bot", "secret": "sec", "dm_policy": "open"}))

    class _WS:
        closed = False

        async def send_json(self, payload):
            asyncio.get_running_loop().create_task(wire.handle(adapter, payload))

    adapter._ws, adapter._running = _WS(), True
    adapter.gateway_runner = runner
    runner.adapters = {adapter.platform: adapter}
    from gateway.session_authority import initialize_session_authority
    await initialize_session_authority(runner, profile_id="default", instance_id="owner")
    runner._wire_adapter_handlers(adapter)
    adapter._start_ticker()
    hooks = __import__(type(adapter).__module__.rsplit(".", 1)[0] + ".hooks", fromlist=["attach"])
    hooks.attach(adapter)
    sent_at = time.monotonic() - wire.t0

    def inbound(msgid, text):
        return {"cmd": "aibot_msg_callback", "headers": {"req_id": f"REQ-{msgid}"}, "body": {
            "msgid": msgid, "chattype": "single", "from": {"userid": user}, "msgtype": "text", "text": {"content": text}}}
    await adapter._dispatch_payload(inbound(f"{user}-1", "first question"))
    await asyncio.sleep(3.0)  # m1 is in its tool
    await adapter._dispatch_payload(inbound(f"{user}-2", "second question"))
    key = f"agent:main:xwecom:dm:{user}"
    for _ in range(300):
        await asyncio.sleep(0.1)
        turns = [t for ts in adapter._turns.values() for t in ts]
        # queue mode: the follow-up's turn starts ~0.1s after the first closes; wait for both
        if len(turns) >= (2 if mode == "queue" else 1) and all(t.closed for t in turns) and not runner._is_session_running(key) \
                and not runner._pending_messages.get(key) if hasattr(runner, "_pending_messages") else True:
            if all(t.closed for t in turns) and not runner._is_session_running(key):
                break
    await asyncio.sleep(1.0)
    await adapter._stop_ticker()
    hooks.detach(adapter)
    print(f"── {name}")
    for sid in wire.order:
        frames = wire.history[sid]
        print(f"   bubble {sid[-6:]}: {len(frames)} frames, finished={sid in wire.finished}")
        for ts, content in frames:
            print(f"      +{ts - sent_at:5.1f}s {content!r}")
    for m in wire.markdown:
        print(f"   [md] {m!r}")
    return wire


def real_bubbles(wire):
    """The bubbles a user sees, minus core's one-off home-channel onboarding notice."""
    return [wire.screen[s].replace("\u200b", "") for s in wire.order if "No home channel" not in wire.screen[s]]


async def main():
    wire = Wire()
    sent_at = await run_turn("real authority path: recall + tools (display.tool_progress: all) + answer", wire)
    bubble = check_turn(wire, sent_at, tick_window=(3.0, 7.0))
    assert any("👁️ Hindsight — recalled 24 memories" in c for _, c in wire.history[bubble]), "recall status line"

    # The live 15:17 failure: an ack arrives long after the plugin stopped waiting for it. The bubble must
    # keep drawing (the old wire poisoned the req_id here and skipped every later frame).
    late = Wire(late_prefix="CRON", late_by=20.0)
    sent_at = await run_turn("same turn + a cron message mid-turn whose ack is 20s late", late, msgid="m2", user="u2",
                             side_send=(3.2, "CRON: daily report"))
    print(f"   late acks injected: {late.late_acks}")
    assert late.late_acks >= 1, "the scenario must actually delay an ack"  # its timed-out re-send is late too
    check_turn(late, sent_at, tick_window=(3.0, 7.0))
    assert "CRON: daily report" in late.visible(), "the cron message keeps its own bubble"
    # Tool round → text → tool round → text: tool lines clear on text, reappear below it, clear again.
    two = Wire()
    await run_turn("two tool rounds around text", two, msgid="m7", user="u7", agent_cls=TwoRoundAgent)
    check_two_rounds(two)

    # The live 02:46 bug: a background-process completion injected as an internal turn (no message id,
    # authority path, no adapter lifecycle) must render in ONE bubble, like a user turn.
    wire, adapter, _ = await internal_turn("injected internal turn (watch notification)", "u8")
    check_internal_turn(wire, adapter)

    # F1 redirect: the second message redirects the running turn; ONE bubble, the ack transient.
    wire = await busy_turns("busy: second message redirects the running turn", "interrupt", "u3")
    shown = real_bubbles(wire)
    assert len(shown) == 1 and shown[0].startswith("ANSWER to") and "(redirected)" in shown[0], shown
    assert "Redirected" not in shown[0], f"the redirect ack must be transient: {shown}"
    first = next(s for s in wire.order if "No home channel" not in wire.screen[s])
    assert any("↪" in c for _, c in wire.history[first]), "the redirect ack shows in the running turn's bubble"
    assert all(s in wire.finished for s in wire.order)

    # F1 queue: the second message is queued; m1 keeps its bubble and answer, m2 gets its own.
    wire = await busy_turns("busy: second message queued behind the running turn", "queue", "u4")
    shown = real_bubbles(wire)
    assert len(shown) == 2, shown
    assert shown[0].startswith("ANSWER to first questi") and "✓" not in shown[0] and "sleep 4" not in shown[0], shown
    assert shown[1].startswith("ANSWER to second quest") and "Queued" not in shown[1], shown
    assert not any("Queued" in v for v in shown), f"the queue ack must be transient: {shown}"
    assert all(s in wire.finished for s in wire.order)

    # G1: the turn's close frame comes back 846609 (subscription lost / kicked). Core already counts the
    # answer delivered; it must still reach the screen — as a markdown message.
    kicked = Wire(reject_finish=846609)
    await run_turn("close frame rejected 846609", kicked, msgid="m5", user="u5")
    assert any(m.startswith("herdr 是一个终端里的代理管理器。") for m in kicked.markdown), kicked.visible()

    # G1/G2: the socket drops after the answer streamed, before the close frame; the bot's next connection
    # must deliver the answer.
    dropped = Wire()

    dropped.drop_after = "代理管理器。"

    async def drop_then_reconnect():
        while not dropped.down:
            await asyncio.sleep(0.05)
        await asyncio.sleep(3.0)  # offline
        dropped.down = False
        await dropped.adapter._flush_parked_rescues()  # what connect() / the reconnect loop run once up
    task = asyncio.get_running_loop().create_task(drop_then_reconnect())
    await run_turn("socket drops before the close, then reconnects", dropped, msgid="m6", user="u6")
    await task
    await asyncio.sleep(0.5)
    print("   delivered after reconnect:", dropped.markdown)
    # The answer's frames were fire-and-forget, never proven on screen: after the reconnect it is re-sent.
    assert any(m.startswith("herdr 是一个终端里的代理管理器。") for m in dropped.markdown), dropped.visible()
    print("\nAUTHORITY-PATH E2E PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        shutil.rmtree(home, ignore_errors=True)
