"""Internal / synthetic turns (a background-process completion injected by core with ``internal=True`` and
no message id, admitted on the authority path without the adapter lifecycle): their hooks carry an empty
message id and every send is unanchored. They still get ONE turn bubble on the chat's cached req_id,
invisible until there is a tool line or text, closed by settle / idle — never by handing over a user turn."""
from __future__ import annotations

import asyncio
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_messages import Clock, FakeWeCom, messages  # noqa: E402
from test_turn_bubble import Host, _intake, run, send_progress_messages, settle, turn_bubble  # noqa: E402


TID = "s1:task:aaaa"  # the injected turn's turn_id (core mints one per run_conversation)


def _dm_seen(h: Host, chat: str = "u1", req: str = "R0") -> None:
    """The chat had an earlier inbound message: its req_id is cached (``_last_chat_req_ids``)."""
    h._last[chat] = req


async def _injected_turn(h: Host) -> None:
    """The live 02:46 shape: model call → ssh tool (+ its progress line, then "(×2)") → text → tool → text."""
    h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
    await settle()
    h._turn_tool_started("u1", "c1", "terminal", "", 0.0, session="u1", turn_id=TID)
    first = await asyncio.create_task(send_progress_messages(h, ("send", "u1", "💻 terminal: ssh box uptime")))
    await asyncio.create_task(send_progress_messages(h, ("edit", first[0].message_id, "💻 terminal: ssh box uptime (×2)")))
    await settle()
    h.clock.now = 2.0
    h._turn_tool_finished("u1", "c1", "", 2.0)
    h.llm_request_started("u1", "", at=2.0, session="u1", turn_id=TID, wake=True)
    r = await h.send("u1", "进程已完成 ▉", metadata={"expect_edits": True})
    await h._edit_turn_part(r.message_id, "进程已完成。", finalize=True)
    h.llm_request_started("u1", "", at=3.0, session="u1", turn_id=TID, wake=True)
    h._turn_tool_started("u1", "c2", "terminal", "", 3.0, session="u1", turn_id=TID)
    await settle()
    h.clock.now = 4.0
    h._turn_tool_finished("u1", "c2", "", 4.0)
    h.llm_request_started("u1", "", at=4.0, session="u1", turn_id=TID, wake=True)
    r2 = await h.send("u1", "结果：一切正常 ▉", metadata={"expect_edits": True})
    await h._edit_turn_part(r2.message_id, "结果：一切正常。", finalize=True)
    await settle()


def test_injected_internal_turn_renders_in_one_bubble():
    async def go():
        h = Host(); _dm_seen(h)
        await _injected_turn(h)
        h.clock.now = 4.0 + turn_bubble.SETTLED_IDLE_TIMEOUT + 1
        await h._tick_turns(h.clock.now)
        await settle()
        return h
    h = run(go())
    assert h.outside == [] and h.markdown == [], h.outside  # nothing took the old one-bubble-per-send path
    assert h.wire.visible() == ["进程已完成。\n\n结果：一切正常。"], h.wire.visible()
    assert h.wire.finished == set(h.wire.order), "the internal turn's bubble must close without a lifecycle hook"


def test_internal_turn_shows_its_tools_before_text():
    async def go():
        h = Host(); _dm_seen(h)
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
        h._turn_tool_started("u1", "c1", "terminal", "", 0.0, session="u1", turn_id=TID)
        await settle()
        return h
    h = run(go())
    assert h.wire.visible() == ["⠋ 🔧 terminal (0s)"], h.wire.visible()


def test_silent_internal_wake_draws_nothing():
    async def go():
        h = Host(); _dm_seen(h)
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
        await settle()
        for now in (1.0, 5.0, 30.0):
            h.clock.now = now
            await h._tick_turns(now)
            await settle()
        return h
    h = run(go())
    assert h.wire.frames == 0 and h.outside == [], h.wire.visible()  # no thinking placeholder for a silent wake


def test_progress_and_status_lanes_join_a_live_internal_turn_but_never_open_one():
    async def go():
        h = Host(); _dm_seen(h)
        await h.send_or_update_status("u1", "lifecycle", "👁️ Hindsight — recalled 3 memories")  # no live turn
        await asyncio.create_task(send_progress_messages(h, ("send", "u1", "💻 terminal: ls")))
        lone = (list(h.outside), list(h._turns.get("u1", ())))
        h.outside.clear()
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
        await h.send_or_update_status("u1", "lifecycle", "👁️ Hindsight — recalled 4 memories")
        h._turn_tool_started("u1", "c1", "terminal", "", 0.0, session="u1", turn_id=TID)
        await asyncio.create_task(send_progress_messages(h, ("send", "u1", "💻 terminal: ls")))
        await settle()
        return h, lone
    h, (outside, turns) = run(go())
    assert turns == [] and outside == [("u1", "👁️ Hindsight — recalled 3 memories"), ("u1", "💻 terminal: ls")], outside
    assert h.outside == [] and len(h.wire.order) == 1
    assert "Hindsight — recalled 4" in h.wire.visible()[0] and "terminal" in h.wire.visible()[0]


def test_internal_turn_never_hands_over_or_closes_a_live_user_turn():
    async def go():
        h = Host()
        e = _intake(h, "u1", "u1", "m1", "R1")
        await h._start_turn(e)
        h.llm_request_started("u1", "", at=0.0, session="u1")  # same session: it is that turn's hook
        await settle()
        r = await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        return h, e, r
    h, e, r = run(go())
    live = h._live_turns("u1")
    assert len(live) == 1 and live[0].opener_id == "m1" and not live[0].internal
    assert h.wire.visible() == ["answer"] and not h.wire.finished


def test_group_internal_turn_is_its_own_session_and_leaves_bobs_turn_alone():
    async def go():
        h = Host()
        eb = h.inbound("g", "bob", "b1", "RB1", "group"); await h._start_turn(eb)
        h.llm_request_started("g", "b1", at=0.0)
        h.llm_request_started("g", "", at=0.0, session="g:alice", turn_id=TID, wake=True)
        h._turn_tool_started("g", "c1", "terminal", "", 0.0, session="g:alice", turn_id=TID)
        await settle()
        await h.send("g", "BOB ANSWER", reply_to="b1", metadata={"notify": True})
        await h._complete_turns(eb)
        return h
    h = run(go())
    turns = h._turns["g"]
    assert [t.internal for t in turns] == [False, True] and not turns[1].closed
    assert "BOB ANSWER" in h.wire.visible() and not any("terminal" in v for v in h.wire.visible() if "BOB" in v)


def test_group_without_a_cached_req_id_keeps_the_old_path():
    async def go():
        h = Host()
        h.llm_request_started("g", "", at=0.0, session="g:alice", turn_id=TID, wake=True)
        await asyncio.create_task(send_progress_messages(h, ("send", "g", "💻 terminal: ls")))
        return h
    h = run(go())
    assert h._live_turns("g") == [] and h.outside == [("g", "💻 terminal: ls")]


def test_unanchored_marked_send_without_a_live_internal_turn_keeps_its_own_message():
    """Cron delivers with ``notify`` and no anchor too: it must never open a turn of its own."""
    async def go():
        h = Host(); _dm_seen(h)
        await h.send("u1", "CRON: daily report", metadata={"notify": True})
        return h
    h = run(go())
    assert h.outside == [("u1", "CRON: daily report")] and h._live_turns("u1") == []


def test_review_summary_after_the_turn_closed_keeps_its_own_message_and_never_reopens_the_bubble():
    async def go():
        h = Host()
        e = _intake(h, "u1", "u1", "m1", "R1"); await h._start_turn(e)
        await h.send("u1", "answer", reply_to="m1", metadata={"notify": True})
        await h._complete_turns(e)
        frames = h.wire.frames
        h.llm_request_started("u1", "m1", at=1.0)  # the review fork keeps the turn's message id
        await settle()
        await h.send("u1", "💾 Self-improvement review: Skill 'x' patched", metadata={"_interim_send": True})
        return h, frames
    h, frames = run(go())
    assert h.wire.frames == frames and h.wire.visible() == ["answer"]
    assert h.outside == [("u1", "💾 Self-improvement review: Skill 'x' patched")]
    assert h._live_turns("u1") == []


def test_progress_edit_of_a_closed_bubble_continues_with_the_whole_line():
    clock = Clock(); wire = FakeWeCom(clock); book = messages.MessageBook(wire, clock=clock)

    async def go():
        msg = await book.create("c", "r", "💻 Running ssh box", finalize=True)
        await book.update(msg, "💻 Running ssh box (×2)", finalize=True, whole_lines=True)
        await book.update(msg, "💻 Running ssh box (×2)\n📖 Reading a.txt", finalize=True, whole_lines=True)
    run(go())
    assert wire.visible() == ["💻 Running ssh box", "💻 Running ssh box (×2)", "📖 Reading a.txt"], wire.visible()


# ── B1: only core's internal-notification wake opens; foreign turn_ids after settled text are ignored ──
async def _review_fork(h: Host, at: float) -> None:
    """The background review fork (run_agent: propagate_context_to_thread): message id "", the session key,
    its OWN turn_id, a review prompt (no marker); a model call, a memory tool, another model call."""
    h.llm_request_started("u1", "", at=at, session="u1", turn_id="s1:review:bbbb")
    h._turn_tool_started("u1", "rv1", "memory", "", at + 0.5, session="u1", turn_id="s1:review:bbbb")
    await settle()
    h._turn_tool_finished("u1", "rv1", "", at + 1.0)
    h.llm_request_started("u1", "", at=at + 1.0, session="u1", turn_id="s1:review:bbbb")
    await settle()


async def _internal_answer(h: Host) -> None:
    h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
    r = await h.send("u1", "进程已完成 ▉", metadata={"expect_edits": True})
    await h._edit_turn_part(r.message_id, "进程已完成。", finalize=True)
    await settle()


async def _tick(h: Host, now: float) -> None:
    h.clock.now = now
    await h._tick_turns(now)
    await settle()


def test_review_fork_during_a_settled_internal_turn_is_ignored_and_the_turn_settles():
    async def go():
        h = Host(); _dm_seen(h)
        await _internal_answer(h)
        h.clock.now = 1.0
        await _review_fork(h, 1.0)
        for now in (10.0, 25.0, 26.0):
            await _tick(h, now)
        return h
    h = run(go())
    assert all(t.closed for t in h._turns["u1"]), "the internal turn must settle, not wait 40 min"
    assert h.wire.visible() == ["进程已完成。"] and h.wire.finished == set(h.wire.order)
    assert not any("memory" in v or "Thinking" in v for v in h.wire.visible())


def test_review_fork_after_the_internal_turn_closed_opens_nothing():
    async def go():
        h = Host(); _dm_seen(h)
        await _internal_answer(h)
        await _tick(h, 25.0); await _tick(h, 26.0)
        frames = h.wire.frames
        await _review_fork(h, 30.0)
        await _tick(h, 60.0)
        return h, frames
    h, frames = run(go())
    assert [t.internal for t in h._turns["u1"]] == [True] and h.wire.frames == frames, h.wire.visible()


def test_model_call_without_the_wake_marker_and_tool_hooks_never_open_a_turn():
    async def go():
        h = Host(); _dm_seen(h)
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id="s1:resume:cccc")  # subagent auto-resume
        h._turn_tool_started("u1", "c1", "terminal", "", 0.0, session="u1", turn_id="s1:resume:cccc")
        await settle()
        return h
    h = run(go())
    assert h._turns.get("u1", []) == [] and h.wire.frames == 0


def test_internal_turn_never_draws_thinking():
    async def go():
        h = Host(); _dm_seen(h)
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
        h._turn_tool_started("u1", "c1", "terminal", "", 0.0, session="u1", turn_id=TID)
        await settle()
        h._turn_tool_finished("u1", "c1", "", 2.0)
        h.llm_request_started("u1", "", at=2.0, session="u1", turn_id=TID, wake=True)
        for now in (3.0, 9.0, 60.0):
            await _tick(h, now)
        return h
    h = run(go())
    assert h.wire.order and not any("Thinking" in v for v in h.wire.visible()), h.wire.visible()


# ── B2: a user turn taking over closes an internal turn at once, even mid-text ──
def test_hand_over_closes_an_internal_turn_mid_stream():
    async def go():
        h = Host(); _dm_seen(h)
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
        await h.send("u1", "partial notice text ▉", metadata={"expect_edits": True}); await settle()
        e = _intake(h, "u1", "u1", "m2", "R2")
        h.clock.now = 1.0
        await h._start_turn(e)
        await h.send("u1", "USER ANSWER", reply_to="m2", metadata={"notify": True})
        late = await h.send("u1", "CRON: daily", metadata={"notify": True})
        await h._complete_turns(e); await settle()
        return h, late
    h, late = run(go())
    internal = next(t for t in h._turns["u1"] if t.internal)
    assert internal.closed and internal.closed_at == 1.0
    assert h.wire.visible() == ["partial notice text", "USER ANSWER"] and h.wire.finished == set(h.wire.order)
    assert h.outside == [("u1", "CRON: daily")]


def test_user_backstop_open_hands_over_a_silent_internal_turn():
    async def go():
        h = Host(); _dm_seen(h)
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)  # silent: never drawn
        _intake(h, "u1", "u1", "m2", "R2")
        h.llm_request_started("u1", "m2", at=1.0); await settle()  # the user turn opens from its first model call
        await h.send("u1", "USER ANSWER", reply_to="m2", metadata={"notify": True}); await settle()
        return h
    h = run(go())
    assert [(t.internal, t.closed) for t in h._turns["u1"]] == [(True, True), (False, False)]
    assert h.wire.visible() == ["USER ANSWER"] and h.outside == []


# ── B3: an internal turn with nothing drawn closes; its notices go out the old way ──
def test_undrawn_internal_turn_closes_and_sends_its_notices_the_old_way():
    async def go():
        h = Host(); _dm_seen(h)
        h.llm_request_started("u1", "", at=0.0, session="u1", turn_id=TID, wake=True)
        await h.send_or_update_status("u1", "lifecycle", "⚠️ provider fallback to X")  # joins, nothing drawn
        await _tick(h, turn_bubble.UNDRAWN_INTERNAL_IDLE - 1)
        still = not h._turns["u1"][0].closed
        await _tick(h, turn_bubble.UNDRAWN_INTERNAL_IDLE + 1)
        return h, still
    h, still = run(go())
    assert still and h._turns["u1"][0].closed
    assert h.wire.frames == 0 and h.outside == [("u1", "⚠️ provider fallback to X")]


def test_status_lane_in_a_group_with_no_live_turn_keeps_its_own_message():
    async def go():
        h = Host()
        ea = h.inbound("g", "alice", "a1", "RA1", "group"); await h._start_turn(ea)
        await h.send("g", "A", reply_to="a1", metadata={"notify": True}); await h._complete_turns(ea)
        await h.send_or_update_status("g", "k", "⚠️ fallback notice"); await settle()
        eb = h.inbound("g", "bob", "b2", "RB2", "group"); await h._start_turn(eb)
        h._turn_tool_started("g", "c1", "terminal", "b2", 0.0)
        await asyncio.create_task(send_progress_messages(h, ("send", "g", "💻 terminal: ls"))); await settle()
        return h
    h = run(go())
    assert [t.internal for t in h._turns["g"]] == [False, False]
    assert h.outside == [("g", "⚠️ fallback notice")] and "terminal" in h.wire.visible()[-1]
