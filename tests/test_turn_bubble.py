"""Turn-bubble routing at the adapter level (``xwecom/turn_bubble.py``): which sends join a turn, which keep
their own message, what lands on screen when the req_id is dead. Runs the real ``TurnBubbleMixin`` on a
minimal host over the fake WeCom wire (no Hermes checkout needed): the host's ``_send_outside_turn`` /
``_send_markdown`` record what took the old path."""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
import types

_ROOT = pathlib.Path(__file__).resolve().parents[1] / "xwecom"
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_messages import Clock, FakeWeCom, messages  # noqa: E402

# turn_bubble.py imports ``SendResult`` from the gateway and its siblings relatively; give it a tiny
# package with the real sibling modules and a SendResult stand-in.
_pkg = types.ModuleType("xw_tb"); _pkg.__path__ = [str(_ROOT)]  # noqa: E702
sys.modules["xw_tb"] = _pkg
sys.modules["xw_tb.messages"] = messages
for _name in ("turn", "compat", "turn_bubble"):
    _spec = importlib.util.spec_from_file_location(f"xw_tb.{_name}", _ROOT / f"{_name}.py")
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules[f"xw_tb.{_name}"] = _mod
    if _name == "compat" and "gateway.platforms.base" not in sys.modules:
        class SendResult:
            def __init__(self, success=False, message_id=None, error=None, **_):
                self.success, self.message_id, self.error = success, message_id, error
        for mod in ("gateway", "gateway.platforms", "gateway.platforms.base", "gateway.platforms._shared"):
            sys.modules.setdefault(mod, types.ModuleType(mod))
        sys.modules["gateway.platforms.base"].SendResult = SendResult
        sys.modules["gateway.platforms._shared"].get_scoped_secret = lambda name, default="": default
    _spec.loader.exec_module(_mod)
turn_bubble = sys.modules["xw_tb.turn_bubble"]
# Core builds a tool's progress line from its registry emoji + name or friendly verb; the test host has no
# registry, so give the mixin those line starts directly (what compat.tool_line_prefixes reads in production).
_TOOL_LINE_PREFIXES = {"web_search": ("🔍 web_search", "🔍 Searching the web"), "web_extract": ("📄 web_extract", "📄 Reading"),
                       "terminal": ("💻 terminal",), "read_file": ("📖 read_file", "📖 Reading"),
                       "execute_code": ("🐍 execute_code", "🐍 Running code")}
turn_bubble.tool_line_prefixes = lambda name: _TOOL_LINE_PREFIXES.get(name, (f"⚙️ {name}",))
SendResult = turn_bubble.SendResult
FrameOutcome, MessageBook = messages.FrameOutcome, messages.MessageBook


class Wire(FakeWeCom):
    """FakeWeCom with the real wire's ack semantics: a dead req_id answers 846604, which — as on the real
    wire — only an AWAITED frame (finish) can see: a fire-and-forget frame reports ok and renders nothing."""

    async def stream_frame(self, req_id, stream_id, content, *, finish):
        if req_id in self.dead:
            self.frames += 1
            if finish:
                return FrameOutcome(ok=False, expired=True, req_dead=True)
            return FrameOutcome(ok=True)
        return await super().stream_frame(req_id, stream_id, content, finish=finish)


class Source:
    def __init__(self, chat_id, user_id, chat_type="dm"):
        self.chat_id, self.user_id, self.chat_type = chat_id, user_id, chat_type


class Event:
    def __init__(self, chat_id, user_id, message_id, chat_type="dm", reply_expected=None):
        self.source, self.message_id = Source(chat_id, user_id, chat_type), message_id
        self.reply_expected, self.internal = reply_expected, False


class Host(turn_bubble.TurnBubbleMixin):
    name = "xw-test"

    def __init__(self, *, dead=()):
        self.clock = Clock()
        self.wire = Wire(self.clock, dead_req_ids=dead)
        self._book = MessageBook(self.wire, clock=self.clock)
        self._reply_req_ids: dict[str, str] = {}
        self._last: dict[str, str] = {}
        self.outside: list[tuple[str, str]] = []  # (chat, content) that took the old path
        self.markdown: list[str] = []
        self._ws = types.SimpleNamespace(closed=False)  # the WeCom socket (closed=True: the connection dropped)
        self._bot_id = f"bot-{id(self)}"
        self._init_turns()

    def _source_session_key(self, source):
        return f"{source.chat_id}:{source.user_id}" if source.chat_type == "group" else source.chat_id

    def _cached_reply_req_id(self, chat_id, reply_to):
        return self._reply_req_ids.get(reply_to or "") or self._last.get(chat_id)

    def inbound(self, chat_id, user_id, message_id, req_id, chat_type="dm", **kw) -> Event:
        self._reply_req_ids[message_id] = req_id
        self._last[chat_id] = req_id
        ev = Event(chat_id, user_id, message_id, chat_type, **kw)
        self._remember_inbound_session(message_id, ev.source)
        return ev

    async def _send_outside_turn(self, chat_id, content, reply_to, metadata):
        self.outside.append((chat_id, content))
        return SendResult(success=True, message_id="own")

    async def _send_markdown(self, chat_id, content, reply_to=None, metadata=None):
        self.markdown.append(content)
        return SendResult(success=True, message_id="md")

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        metadata = dict(metadata or {})
        folded = await self._send_into_turn(chat_id, content, reply_to, metadata)
        return folded if folded is not None else await self._send_outside_turn(chat_id, content, reply_to, metadata)


def run(coro):
    return asyncio.run(coro)


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


# ── opening ─────────────────────────────────────────────────────────────
def test_turn_is_registered_but_draws_nothing_until_something_happens():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        assert h.wire.frames == 0 and len(h._live_turns("u1")) == 1
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        return h
    h = run(go())
    assert h.wire.visible() == ["⠋ 💭 Thinking (0s)"]


def test_no_bubble_when_no_reply_is_expected():
    async def go():
        h = Host(); e = h.inbound("g", "alice", "m1", "R1", "group", reply_expected=False)
        await h._start_turn(e)
        return h
    assert run(go())._live_turns("g") == []


def test_silent_turn_closes_without_a_placeholder_bubble():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        await h._complete_turns(e)
        return h
    h = run(go())
    assert h.wire.visible() == [""] and h.wire.finished == set(h.wire.order)  # empty finish, no "✅"


def test_never_drawn_silent_turn_sends_nothing():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h._complete_turns(e)
        return h
    h = run(go())
    assert h.wire.frames == 0 and h.outside == [] and h.markdown == []


# ── B3: what is NOT the turn's ──────────────────────────────────────────
def test_unanchored_and_foreign_sends_keep_their_own_message():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "CRON: daily report")  # _standalone_send: no anchor, no metadata
        h.inbound("u1", "u1", "m2", "R2")
        await h.send("u1", "📊 Status: running 3m", reply_to="m2", metadata={"notify": True})  # inline /status
        await h.send("u1", "/status output", reply_to="m9-unknown", metadata={"notify": True})
        await h.send("u1", "approve?", metadata={"is_approval_prompt": True, "_interim_send": True})
        await h.send("u1", "conf", reply_to="m1", metadata={"force_proactive_send": True})
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h
    h = run(go())
    assert [c for _, c in h.outside] == ["CRON: daily report", "📊 Status: running 3m", "/status output", "approve?", "conf"]
    assert h.wire.visible() == ["answer"]


def test_inline_reply_on_a_folded_message_still_keeps_its_own_message_when_marked():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h.inbound("u1", "u1", "m2", "R2")
        await h.send("u1", "↪ Redirected current run. I'll adjust using your correction.", reply_to="m2")  # busy ack: m2 folds into the turn
        await h.send("u1", "redirected answer", reply_to="m2", metadata={"notify": True})  # re-anchored final
        await h._complete_turns(e)
        return h
    h = run(go())
    assert h.outside == [] and h.wire.visible() == ["redirected answer"]  # the ack was transient


# ── B4: _interim_send with content ──────────────────────────────────────
def test_interim_content_keeps_its_own_message_short_notices_fold():
    prose = "Here are 3 options I found: A) ... B) ... C) ..."
    command = "```\nrm -rf /tmp/build && make all\n```"

    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", prose, metadata={"_interim_send": True})  # clarify_context_sink
        await h.send("u1", command, metadata={"_interim_send": True})  # send_full_command
        await h.send("u1", "⌛ Approval timed out after 5 min — the command was NOT run. Ask me to try again if you still want it, or raise approvals.timeout in config.yaml.", metadata={"_interim_send": True})
        await h.send("u1", "done", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h
    h = run(go())
    assert [c for _, c in h.outside] == [prose, command]
    assert h.wire.visible() == ["done\n\n---\n⌛ Approval timed out after 5 min — the command was NOT run. Ask me to try again if you still want it, or raise approvals.timeout in config.yaml."]


def test_heartbeat_is_live_and_dropped_at_close():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        hb = await h.send("u1", "⏳ Working — 3 min", metadata={"_interim_send": True})
        upd = await h._edit_turn_part(hb.message_id, "⏳ Working — 4 min", finalize=False)
        assert upd.success
        shown = h.wire.visible()[0]
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h, shown
    h, shown = run(go())
    assert shown.endswith("⏳ Working — 4 min")
    assert h.wire.visible() == ["answer"]


# ── B2: group, two sessions ─────────────────────────────────────────────
def test_group_redirect_ack_and_answer_stay_with_their_own_session():
    async def go():
        h = Host()
        ea = h.inbound("g", "alice", "a1", "RA1", "group"); await h._start_turn(ea)
        eb = h.inbound("g", "bob", "b1", "RB1", "group"); await h._start_turn(eb)
        h.inbound("g", "alice", "a2", "RA2", "group")
        await h.send("g", "↪ Redirected current run. I'll adjust using your correction.", reply_to="a2")
        await h.send("g", "ALICE ANSWER", reply_to="a2", metadata={"notify": True})
        await h.send("g", "BOB ANSWER", reply_to="b1", metadata={"notify": True})
        await h._complete_turns(eb); await h._complete_turns(ea)
        return h
    h = run(go())
    assert h.outside == []
    assert sorted(h.wire.visible()) == ["ALICE ANSWER", "BOB ANSWER"]


def test_group_hooks_without_a_known_message_never_guess_between_sessions():
    async def go():
        h = Host()
        ea = h.inbound("g", "alice", "a1", "RA1", "group"); await h._start_turn(ea)
        eb = h.inbound("g", "bob", "b1", "RB1", "group"); await h._start_turn(eb)
        h.tool_started_hook = h._turn_tool_started("g", "k", "terminal", "", 0.0)
        return h
    assert run(go()).tool_started_hook is False


def test_unanchored_status_notice_in_a_shared_group_keeps_its_own_message():
    async def go():
        h = Host()
        ea = h.inbound("g", "alice", "a1", "RA1", "group"); await h._start_turn(ea)
        eb = h.inbound("g", "bob", "b1", "RB1", "group"); await h._start_turn(eb)
        await h.send("g", "⏳ Working — 3 min", metadata={"_interim_send": True})
        return h
    assert [c for _, c in run(go()).outside] == ["⏳ Working — 3 min"]


# ── B5: queued follow-up chain ──────────────────────────────────────────
def test_follow_up_turn_takes_the_hooks_and_the_answered_turn_closes():
    async def go():
        h = Host()
        e1 = h.inbound("u1", "u1", "m1", "R1"); await h._start_turn(e1)
        r = await h.send("u1", "answer 1", reply_to="m1", metadata={"expect_edits": True})
        await h._edit_turn_part(r.message_id, "answer 1", finalize=True)
        e2 = h.inbound("u1", "u1", "m2", "R2"); await h._start_turn(e2)
        await settle()
        h._inbound_sessions.pop("m1")  # resolve m1 through its turn, not the intake record
        # the follow-up's hooks still carry m1 (HERMES_SESSION_MESSAGE_ID is not re-bound)
        h.llm_request_started("u1", "m1", at=0.0)
        h._turn_tool_started("u1", "t2", "web_search", "m1", 0.0)
        await settle()
        mid = list(h.wire.visible())
        h.clock.now = 2.0
        h._turn_tool_finished("u1", "t2", "m1", 2.0)
        await h.send("u1", "answer 2", reply_to="m2", metadata={"notify": True})
        await h._complete_turns(e2)
        await h._complete_turns(e1)
        return h, mid
    h, mid = run(go())
    assert mid[0] == "answer 1" and len(mid) == 2 and "web_search" in mid[1]
    assert h.wire.visible() == ["answer 1", "answer 2"]
    assert h.wire.finished == set(h.wire.order)


def test_unanswered_earlier_turn_finishes_at_hand_over_and_never_shows_the_think_placeholder():
    async def go():
        h = Host()
        e1 = h.inbound("u1", "u1", "m1", "R1"); await h._start_turn(e1)
        h.llm_request_started("u1", "m1", at=0.0)  # thinking drawn, then interrupted: no reply ever
        await settle()
        e2 = h.inbound("u1", "u1", "m2", "R2"); await h._start_turn(e2)  # the follow-up takes over
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        first = h.wire.order[0]
        mid = (h.wire.screen[first], first in h.wire.finished, list(h.wire.visible()))
        await h.send("u1", "answer 2", reply_to="m2", metadata={"notify": True})
        await h._complete_turns(e2); await h._complete_turns(e1)
        return h, mid
    h, (r1_text, r1_finished, mid) = run(go())
    assert r1_finished and "think" not in r1_text and "Thinking" not in r1_text  # finished, empty, at hand-over
    assert not any("<think>" in v for v in mid)
    assert h.wire.visible()[1:] == ["answer 2"]


def test_unanswered_turn_with_tools_finishes_without_a_tool_footer():
    async def go():
        h = Host()
        e1 = h.inbound("u1", "u1", "m1", "R1"); await h._start_turn(e1)
        h._turn_tool_started("u1", "t1", "terminal", "m1", 0.0)
        await settle()
        h.clock.now = 4.0
        e2 = h.inbound("u1", "u1", "m2", "R2"); await h._start_turn(e2)
        return h
    h = run(go())
    first = h.wire.order[0]
    assert h.wire.screen[first].replace("\u200b", "") == "" and first in h.wire.finished


# ── B1: dead req_id ─────────────────────────────────────────────────────
def test_final_on_a_dead_req_id_reaches_the_screen_the_old_way_at_close():
    """B1 without awaiting any intermediate: the finish frame's 846604 is what detects the loss."""
    async def go():
        h = Host(dead={"R1"}); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "preview text", reply_to="m1", metadata={"expect_edits": True})
        res = await h.send("u1", "THE FINAL ANSWER", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h, res
    h, res = run(go())
    assert res.success  # fire-and-forget, as the pre-migration stream
    assert h.outside == [("u1", "preview text\n\nTHE FINAL ANSWER")]  # nothing was on screen: all of it
    assert h.wire.visible() == [] and h._live_turns("u1") == []


def test_req_id_dying_mid_turn_is_rescued_at_close_once():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        r = await h.send("u1", "streamed", reply_to="m1", metadata={"expect_edits": True})
        h.wire.dead.add("R1")  # the req_id dies mid-turn
        res = await h._edit_turn_part(r.message_id, "streamed answer", finalize=True)
        await h._complete_turns(e)
        return h, res
    h, res = run(go())
    assert res.success and h.outside == [("u1", "streamed answer")]


def test_close_resends_a_body_that_never_landed():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "half an answer", reply_to="m1", metadata={"expect_edits": True})
        h.wire.dead.add("R1")
        await h._complete_turns(e)  # interrupted: no finalize ever came
        return h
    h = run(go())
    assert h.outside == [("u1", "half an answer")]  # never acked on screen: delivered the old way, once


# ── N1: edits after close / growth across rotation ──────────────────────
def test_late_edit_and_rotation_continue_cleanly_without_a_tool_footer():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h._turn_tool_started("u1", "k", "terminal", "m1", 0.0)
        h._turn_tool_finished("u1", "k", "m1", 3.0)
        r = await h.send("u1", "part A", reply_to="m1", metadata={"expect_edits": True})
        h.clock.now = 545
        await h._book.rotate_due()
        await h._edit_turn_part(r.message_id, "part A part B", finalize=True)
        await h._complete_turns(e)
        await h._edit_turn_part(r.message_id, "part A part B + late", finalize=True)
        return h
    h = run(go())
    shown = h.wire.visible()
    assert "".join(v.removesuffix(messages.CONTINUED_SUFFIX).split("\n\n---\n")[0] for v in shown) == "part A part B + late"
    assert not any("terminal" in v for v in shown)


# ── N2 / N4: idle expiry, busy turns skipped by the tick ────────────────
def test_idle_turn_is_closed_by_the_ticker():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        turn = h._live_turns("u1")[0]
        turn.last_activity = -turn_bubble.TURN_IDLE_TIMEOUT - 1
        await h._tick_turns(0.0)
        await settle()
        return h
    assert run(go())._live_turns("u1") == []


def test_tick_skips_a_turn_whose_message_is_busy():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        turn = h._live_turns("u1")[0]
        async with turn.msg.lock:
            before = len(asyncio.all_tasks())
            await h._tick_turns(10.0)
            return len(asyncio.all_tasks()) - before, turn.ticks
    assert run(go()) == (0, 0)  # nothing queued behind the held lock


def test_follow_up_hooks_carrying_the_first_message_id_reach_the_follow_up_turn():
    async def go():
        h = Host()
        e1 = h.inbound("u1", "u1", "m1", "R1"); await h._start_turn(e1)
        await h.send("u1", "half of answer 1", reply_to="m1", metadata={"expect_edits": True})  # mid-answer
        e2 = h.inbound("u1", "u1", "m2", "R2"); await h._start_turn(e2)  # turn 1 stays live, frozen
        h._turn_tool_started("u1", "t2", "web_search", "m1", 0.0)  # the chain's context still says m1
        return h._turn_for_message("u1", "m1"), h._turn_for_message("u1", "m2")
    t1, t2 = run(go())
    assert "t2" in t2.tools and not t1.tools


# ── round 2: R1–R6 ──────────────────────────────────────────────────────
def test_r2_edited_interim_post_keeps_its_final_text_on_screen():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        post = await h.send("u1", "🔨 Deploy started", metadata={"_interim_send": True})  # plugin_messages._send
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h, post
    h, post = run(go())
    assert post.message_id == "own"  # its own message: a later plugin_messages.edit edits THAT message
    assert h.outside == [("u1", "🔨 Deploy started")] and h.wire.visible() == ["answer"]


def test_r2_emoji_led_answer_tail_is_content_not_a_status_line():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "✅ All 3 checks passed.", metadata={"_interim_send": True})  # segment-break tail
        await h.send("u1", "🎉 Done!", metadata={"_interim_send": True, "reply_to_message_id": "m1"})  # commentary
        await h._complete_turns(e)
        return h
    h = run(go())
    assert [c for _, c in h.outside] == ["✅ All 3 checks passed.", "🎉 Done!"]
    assert h.wire.frames == 0  # nothing folded, nothing dropped


def test_r3_req_death_after_an_acked_close_is_never_resent():
    """What an acked finish frame proved on screen is never re-sent (``confirmed_upto``)."""
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        r = await h.send("u1", "Answer shown.", reply_to="m1", metadata={"expect_edits": True})
        await h._complete_turns(e)  # finish frame acked
        h.wire.dead.add("R1")
        await h._edit_turn_part(r.message_id, "Answer shown. And a late tail.", finalize=True)
        return h
    h = run(go())
    assert h.wire.visible() == ["Answer shown."]
    assert h.outside == []  # late edit on a dead req_id after close: reported to Hermes, nothing duplicated


class _Flaky(Wire):
    """A transient (non-846604) failure: the frame could not be sent at all (websocket blip)."""
    flaky = False

    async def stream_frame(self, req_id, stream_id, content, *, finish):
        if self.flaky:
            return FrameOutcome(ok=False)
        return await super().stream_frame(req_id, stream_id, content, finish=finish)


def test_g1_any_failed_close_resends_the_unproven_answer_once():
    """G1: the close fails for a reason that is NOT a dead req_id (a server reject / the frame could not be
    sent): the answer was reported delivered, so it must still reach the screen — the old way, once."""
    async def go():
        h = Host(); h.wire = _Flaky(h.clock); h._book._wire = h.wire
        e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        res = await h.send("u1", "THE ANSWER", reply_to="m1", metadata={"notify": True})
        h.wire.flaky = True
        await h._complete_turns(e)
        return h, res
    h, res = run(go())
    assert res.success
    assert h.outside == [("u1", "THE ANSWER")]


class _Rejecting(Wire):
    """Every finish frame comes back with *errcode* (846609 subscription lost / kicked, 40008 a reject)."""
    def __init__(self, clock, errcode):
        super().__init__(clock)
        self.errcode = errcode

    async def stream_frame(self, req_id, stream_id, content, *, finish):
        if finish:
            self.frames += 1
            dead = self.errcode in (846604, 846609)
            return FrameOutcome(ok=False, expired=dead, req_dead=dead)
        return await super().stream_frame(req_id, stream_id, content, finish=finish)


def test_g1_a_close_rejected_with_846609_resends_the_answer():
    async def go():
        h = Host(); h.wire = _Rejecting(h.clock, 846609); h._book._wire = h.wire
        e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "THE ANSWER", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h
    assert run(go()).outside == [("u1", "THE ANSWER")]


def test_g1_with_the_socket_down_the_answer_is_parked_and_sent_on_the_next_connection():
    async def go():
        h = Host(); h.wire = _Flaky(h.clock); h._book._wire = h.wire
        e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "THE ANSWER", reply_to="m1", metadata={"notify": True})
        h.wire.flaky = True
        h._ws.closed = True  # the connection dropped: the close frame cannot leave
        await h._complete_turns(e)
        parked = list(h.outside)
        # Hermes reconnects the platform with a NEW adapter object for the same bot
        h2 = Host(); h2._bot_id = h._bot_id
        await h2._flush_parked_rescues()
        return parked, h2.outside
    parked, delivered = run(go())
    assert parked == []  # nothing could go out while the socket was down …
    assert delivered == [("u1", "THE ANSWER")]  # … and the next connection sends it


def test_g2_disconnect_parks_every_live_turns_unfinished_answer():
    async def go():
        h = Host()
        e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        r = await h.send("u1", "half of the answer", reply_to="m1", metadata={"expect_edits": True})
        await h._edit_turn_part(r.message_id, "half of the answer, and the rest", finalize=False)
        await h._rescue_live_turns()  # adapter.disconnect() / the listen loop losing the socket
        h2 = Host(); h2._bot_id = h._bot_id
        await h2._flush_parked_rescues()
        return h, h2
    h, h2 = run(go())
    assert h._live_turns("u1") == []
    assert h2.outside == [("u1", "half of the answer, and the rest")]


def test_g4_closed_openers_are_bounded():
    async def go():
        h = Host()
        for n in range(turn_bubble.MAX_INBOUND_SESSIONS + 50):
            e = h.inbound("u1", "u1", f"m{n}", "R1")
            await h._start_turn(e)
            await h._complete_turns(e)
        return h
    h = run(go())
    assert len(h._closed_openers) <= turn_bubble.MAX_INBOUND_SESSIONS


def test_r4_another_sessions_media_never_closes_a_running_turn():
    async def go():
        h = Host()
        ea = h.inbound("g", "alice", "a1", "RA1", "group"); await h._start_turn(ea)
        r = await h.send("g", "Let me check…", reply_to="a1", metadata={"expect_edits": True})
        await h._edit_turn_part(r.message_id, "Let me check…", finalize=True)  # tool-boundary preamble
        eb = h.inbound("g", "bob", "b1", "RB1", "group"); await h._start_turn(eb)
        await h._settle_turns_before_media("g", "b1")  # Bob's image
        await h._settle_turns_before_media("g", "a1")  # Alice's own mid-turn image: answer not delivered
        h._turn_tool_started("g", "t1", "terminal", "a1", 0.0)
        await settle()
        r2 = await h.send("g", "Result: 42", reply_to="a1", metadata={"expect_edits": True})
        await h._edit_turn_part(r2.message_id, "Result: 42", finalize=True)
        await h._complete_turns(ea); await h._complete_turns(eb)
        return h
    h = run(go())
    assert h.wire.visible() == ["Let me check…\n\nResult: 42"]


def test_r4_media_after_a_delivered_answer_frees_that_turns_req_id():
    async def go():
        h = Host(); e = h.inbound("g", "alice", "a1", "RA1", "group")
        await h._start_turn(e)
        await h.send("g", "here is the chart", reply_to="a1", metadata={"notify": True})
        await h._settle_turns_before_media("g", "a1")
        return h
    h = run(go())
    assert h._book.open_messages(chat_id="g") == [] and h.wire.finished == set(h.wire.order)


def test_r6_a_tool_running_past_the_idle_timeout_keeps_its_bubble():
    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h._turn_tool_started("u1", "t1", "terminal", "m1", 0.0)
        await settle()
        h.clock.now = turn_bubble.TURN_IDLE_TIMEOUT + 60
        await h._tick_turns(h.clock.now)
        await settle()
        return h
    h = run(go())
    assert h._live_turns("u1") and not h.wire.finished
    assert "🔧 terminal" in h.wire.visible()[0]


# ── pre-production: 2a tighter status templates, 2b undo an undelivered finalize ──
def test_2a_plugin_posts_that_resemble_status_templates_keep_their_own_message():
    posts = ["⏳ Working — 5 min: deploy FAILED, run rollback.sh now",
             "⏳ Queued for the next turn (your build artifact is at /tmp/x.zip)"]

    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        for post in posts:
            await h.send("u1", post, metadata={"_interim_send": True})
        hb = await h.send("u1", "⏳ Working — 5 min — iteration 2/90, terminal", metadata={"_interim_send": True})
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h, hb
    h, hb = run(go())
    assert [c for _, c in h.outside] == posts  # their own messages: they survive the turn's close
    assert hb.message_id.startswith("xwt_")  # the real heartbeat still folds
    assert h.wire.visible() == ["answer"]


def test_2a_busy_ack_with_detail_still_folds_but_only_on_the_unmarked_path():
    ack = "↪ Redirected current run (3 min elapsed, iteration 2/90). I'll adjust using your correction."

    async def go():
        h = Host(); e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h.inbound("u1", "u1", "m2", "R2")
        folded = await h.send("u1", ack, reply_to="m2")  # run_busy._send_busy_reply shape
        await h.send("u1", ack, metadata={"_interim_send": True})  # same text marked interim: not a status key
        await h._complete_turns(e)
        return h, folded
    h, folded = run(go())
    assert folded.message_id.startswith("xwt_")
    assert h.outside == [("u1", ack)]


def test_2b_failed_finalize_restores_the_shown_text_so_the_fallback_tail_is_not_duplicated():
    async def go():
        h = Host(); h.wire = _Flaky(h.clock); h._book._wire = h.wire
        e = h.inbound("u1", "u1", "m1", "R1")
        await h._start_turn(e)
        r = await h.send("u1", "Part one.  ▉", reply_to="m1", metadata={"expect_edits": True})
        h.wire.flaky = True
        res = await h._edit_turn_part(r.message_id, "Part one. Part two (unseen).", finalize=True)
        h.wire.flaky = False
        # core's fallback (no cursor-strip edit on a turn-final flood) sends the unseen tail on its own
        await h.send("u1", "Part two (unseen).", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        return h, res
    h, res = run(go())
    assert not res.success
    shown = h.wire.visible()
    assert len(shown) == 1 and shown[0].count("Part two (unseen).") == 1, shown
    assert shown[0] == "Part one. Part two (unseen).", shown  # the tail continues the sentence


# ── authority path: open without on_processing_start, close without on_processing_complete ──
class _Cmd(Event):
    def get_command(self):
        return "new"


def _intake(h, chat, user, mid, req, **kw):
    ev = h.inbound(chat, user, mid, req, **kw)
    ev.get_command = lambda: None
    h._note_inbound(ev)
    return ev


def test_turn_opens_from_the_first_model_call_when_no_lifecycle_hook_ran():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        h.llm_request_started("u1", "m1", at=0.0)  # pre_api_request, carrying the session's message id
        await settle()
        return h
    h = run(go())
    assert h.wire.visible() == ["⠋ 💭 Thinking (0s)"]


def test_lifecycle_hook_after_the_backstop_does_not_open_a_second_bubble():
    async def go():
        h = Host()
        ev = _intake(h, "u1", "u1", "m1", "R1")
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        await h._start_turn(ev)  # on_processing_start arriving late (or twice)
        await h._start_turn(ev)
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(ev)
        await h._start_turn(ev)  # a stray lifecycle call after the turn ended must not reopen it
        await settle()
        return h
    h = run(go())
    assert h.wire.visible() == ["answer"] and len(h._turns["u1"]) == 1


def test_commands_never_get_a_turn_bubble():
    async def go():
        h = Host()
        ev = _Cmd("u1", "u1", "m1")
        h._reply_req_ids["m1"] = "R1"; h._last["u1"] = "R1"  # noqa: E702
        h._note_inbound(ev)
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        return h
    assert run(go()).wire.frames == 0


def test_a_turn_whose_answer_landed_closes_without_on_processing_complete():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        await h.send("u1", "answer, complete", reply_to="m1", metadata={"notify": True})  # the complete reply
        h.clock.now = turn_bubble.SETTLED_IDLE_TIMEOUT + 1
        await h._tick_turns(h.clock.now)
        await settle()
        return h
    h = run(go())
    assert h.wire.visible() == ["answer, complete"] and h.wire.finished == set(h.wire.order)


def test_a_finalized_preamble_followed_by_a_tool_is_not_closed_early():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        r = await h.send("u1", "Let me check…", reply_to="m1", metadata={"expect_edits": True})
        await h._edit_turn_part(r.message_id, "Let me check…", finalize=True)  # tool-boundary finalize
        h._turn_tool_started("u1", "t1", "terminal", "m1", 0.0)
        h.clock.now = turn_bubble.SETTLED_IDLE_TIMEOUT + 1
        await h._tick_turns(h.clock.now)
        await settle()
        return h
    h = run(go())
    assert h._live_turns("u1") and not h.wire.finished


async def send_progress_messages(h, *steps):
    """Stand-in for core's ``TurnRunner.send_progress_messages`` lane: the plugin identifies that lane by
    the coroutine issuing the adapter call, so the calls must come from a coroutine of that name."""
    out = []
    for op, *args in steps:
        out.append(await (h.send(*args) if op == "send" else h._edit_turn_part(*args, finalize=True)))
    return out


def test_tool_progress_lane_labels_the_status_lines_instead_of_a_second_bubble():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        h._turn_tool_started("u1", "c1", "web_search", "m1", 0.0)
        await settle()
        first = await asyncio.create_task(send_progress_messages(h, ("send", "u1", "🔍 Searching the web for herdr.dev")))
        h._turn_tool_started("u1", "c2", "web_extract", "m1", 0.0)
        await asyncio.create_task(send_progress_messages(
            h, ("edit", first[0].message_id, "🔍 Searching the web for herdr.dev\n📄 Reading https://herdr.dev")))
        await settle()
        live = h.wire.visible()[0]
        h.clock.now = 2.0
        h._turn_tool_finished("u1", "c1", "m1", 2.0)
        h._turn_tool_finished("u1", "c2", "m1", 2.0)
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(h._live_turns("u1")[0].event or _Ev("u1", "m1"))
        return h, first, live
    h, first, live = run(go())
    assert first[0].message_id.startswith("xwt_") and h.outside == []
    assert "🔍 Searching the web for herdr.dev (0s)" in live and "📄 Reading https://herdr.dev (0s)" in live
    assert len(h.wire.order) == 1
    assert h.wire.visible() == ["answer"]


class _Ev(Event):
    def __init__(self, chat, mid):
        super().__init__(chat, chat, mid)


def test_tool_progress_lane_without_a_live_turn_keeps_its_own_message():
    async def go():
        h = Host()
        await asyncio.create_task(send_progress_messages(h, ("send", "u1", "🔍 Searching the web for x")))
        return h
    assert run(go()).outside == [("u1", "🔍 Searching the web for x")]


def test_same_text_from_a_non_lane_caller_is_not_folded():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        h._turn_tool_started("u1", "c1", "web_search", "m1", 0.0)
        await h.send("u1", "🔍 Searching the web for x")  # same text, but not core's progress lane
        return h
    assert run(go()).outside == [("u1", "🔍 Searching the web for x")]


def test_agent_status_lane_is_one_status_line_updated_in_place():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        await h.send_or_update_status("u1", "k:lifecycle", "👁️ Hindsight — recalled 24 memories")
        await h.send_or_update_status("u1", "k:lifecycle", "👁️ Hindsight — recalled 25 memories")
        await settle()
        live = h.wire.visible()[0]
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(_Ev("u1", "m1"))
        return h, live
    h, live = run(go())
    assert live.count("Hindsight") == 1 and live.endswith("👁️ Hindsight — recalled 25 memories")
    assert h.outside == [] and h.wire.visible() == ["answer"]


def test_first_reply_opens_the_turn_when_nothing_else_did():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        r = await h.send("u1", "answer ▉", reply_to="m1", metadata={"expect_edits": True})
        h._turn_tool_started("u1", "t1", "terminal", "m1", 0.0)
        await settle()
        return h, r
    h, r = run(go())
    assert r.message_id.startswith("xwt_") and len(h.wire.order) == 1
    assert "answer" in h.wire.visible()[0] and "🔧 terminal" in h.wire.visible()[0]


def test_f5_a_turn_whose_finish_waits_on_a_lost_ack_never_stops_other_turns_drawing():
    async def go():
        h = Host()
        ea = h.inbound("g", "alice", "a1", "RA1", "group"); await h._start_turn(ea)
        eb = h.inbound("g", "bob", "b1", "RB1", "group"); await h._start_turn(eb)
        h.llm_request_started("g", "a1", at=0.0); h.llm_request_started("g", "b1", at=0.0)
        await settle()
        alice = h._turn_for_message("g", "a1")
        before = h.wire.frames
        async with alice.msg.lock:  # Alice's finish frame is waiting for an ack that never comes
            for n in range(1, 4):
                h.clock.now = float(n)
                await h._tick_turns(h.clock.now)
                await settle()
        bob_frames = [c for sid, c in h.wire.screen.items() if "Thinking" in c]
        return h.wire.frames - before, bob_frames
    drawn, shown = run(go())
    assert drawn == 3  # Bob's bubble ticked every second; Alice's was skipped, not waited on


# ── F1 / F3 (fourth review) ─────────────────────────────────────────────
def _busy_round(ack):
    async def go():
        h = Host()
        e1 = _intake(h, "u1", "u1", "m1", "R1")
        await h._start_turn(e1)
        h.llm_request_started("u1", "m1", at=0.0)
        await settle()
        _intake(h, "u1", "u1", "m2", "R2")  # the second message through intake (as _on_message records it)
        await h.send("u1", ack, reply_to="m2")  # run_busy._send_busy_reply: anchored on m2, no metadata
        await settle()
        mid = list(h.wire.visible())
        await h.send("u1", "ANSWER", reply_to="m2", metadata={"notify": True})  # the redirected final
        await h._complete_turns(e1)
        return h, mid
    return run(go())


def test_f1_a_redirect_ack_never_opens_or_closes_a_turn():
    h, mid = _busy_round("↪ Redirected current run. I'll adjust using your correction.")
    assert len(mid) == 1 and mid[0].endswith("↪ Redirected current run. I'll adjust using your correction.")
    assert h.wire.visible() == ["ANSWER"]  # one bubble, ack transient, the running turn kept


def test_f1_a_queue_ack_keeps_the_running_turn_and_the_follow_up_gets_its_own():
    async def go():
        h = Host()
        e1 = _intake(h, "u1", "u1", "m1", "R1")
        await h._start_turn(e1)
        h._turn_tool_started("u1", "t1", "terminal", "m1", 0.0)
        e2 = _intake(h, "u1", "u1", "m2", "R2")
        await h.send("u1", "⏳ Queued for the next turn. I'll respond once the current task finishes.", reply_to="m2")
        h.clock.now = 2.0
        h._turn_tool_finished("u1", "t1", "m1", 2.0)
        await h.send("u1", "ANSWER 1", reply_to="m1", metadata={"notify": True})
        await h._start_turn(e2)  # the queued follow-up starts executing
        h.llm_request_started("u1", "m1", at=2.0)  # the chain still runs under m1's context
        await h.send("u1", "ANSWER 2", reply_to="m2", metadata={"notify": True})
        await h._complete_turns(e2); await h._complete_turns(e1)
        return h
    h = run(go())
    assert h.wire.visible() == ["ANSWER 1", "ANSWER 2"]


def test_f3_a_late_segment_finalize_never_settles_a_turn_that_is_thinking_again():
    async def go():
        h = Host()
        _intake(h, "u1", "u1", "m1", "R1")
        h.llm_request_started("u1", "m1", at=0.0)
        r = await h.send("u1", "Let me check.", reply_to="m1", metadata={"expect_edits": True})
        h._turn_tool_started("u1", "t1", "terminal", "m1", 0.0)
        h._turn_tool_finished("u1", "t1", "m1", 0.0)
        h.llm_request_started("u1", "m1", at=0.0)  # the next model call already started …
        await h._edit_turn_part(r.message_id, "Let me check.", finalize=True)  # … when the boundary finalize lands
        h.clock.now = turn_bubble.SETTLED_IDLE_TIMEOUT + 5  # the model stalls past the settled-idle window
        await h._tick_turns(h.clock.now)
        await settle()
        return h
    h = run(go())
    assert h._live_turns("u1") and not h.wire.finished
    assert "💭 Thinking" in h.wire.visible()[0]


def test_g2_an_answer_into_a_dead_req_id_is_rescued_within_seconds_not_at_the_idle_close():
    async def go():
        h = Host(dead={"R1"})
        e = _intake(h, "u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "THE ANSWER", reply_to="m1", metadata={"notify": True})  # reported delivered
        h.clock.now = turn_bubble.SETTLE_AFTER_FINAL_SECONDS + 0.5  # no on_processing_complete arrives
        await h._tick_turns(h.clock.now)
        for _ in range(10):
            await settle()
        return h
    h = run(go())
    assert h.outside == [("u1", "THE ANSWER")]


def test_g2_quick_seal_does_not_cut_a_turn_that_keeps_working():
    async def go():
        h = Host()
        e = _intake(h, "u1", "u1", "m1", "R1")
        await h._start_turn(e)
        await h.send("u1", "Here is part one.", reply_to="m1", metadata={"notify": True})
        h.llm_request_started("u1", "m1", at=1.0)  # the turn goes on
        h.clock.now = turn_bubble.SETTLE_AFTER_FINAL_SECONDS + 0.5
        await h._tick_turns(h.clock.now)
        await settle()
        return h
    h = run(go())
    assert h._live_turns("u1") and not h.wire.finished


def test_g2_an_edit_after_the_quick_seal_continues_without_repeating():
    async def go():
        h = Host()
        e = _intake(h, "u1", "u1", "m1", "R1")
        await h._start_turn(e)
        r = await h.send("u1", "THE ANSWER", reply_to="m1", metadata={"notify": True})
        h.clock.now = turn_bubble.SETTLE_AFTER_FINAL_SECONDS + 0.5
        await h._tick_turns(h.clock.now)
        for _ in range(10):
            await settle()
        await h._edit_turn_part(r.message_id, "THE ANSWER, with a late tail", finalize=True)
        await h._complete_turns(e)  # on_processing_complete after the quick seal: a no-op
        return h
    h = run(go())
    assert "".join(v for v in h.wire.visible()) == "THE ANSWER, with a late tail"
    assert h.wire.finished == set(h.wire.order)
