"""Turn-bubble status area (``xwecom/turn.py``) and the MessageBook primitives the turn bubble draws with,
against the fake WeCom wire of test_messages.py (measured behaviour: 600s wall, finished streams ignore
updates)."""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1] / "xwecom"


def _load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, _ROOT / file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


turn_mod = _load("xwecom_turn", "turn.py")
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_messages import Clock, FakeWeCom, messages  # noqa: E402

Turn, MessageBook, CONTINUED_SUFFIX = turn_mod.Turn, messages.MessageBook, messages.CONTINUED_SUFFIX


def new_turn(now: float = 0.0) -> "Turn":
    return Turn(chat_id="c", opener_id="m1", session="c:u", msg=None, started=now, thinking_since=now)


# ── status area ─────────────────────────────────────────────────────────
def test_thinking_is_shown_from_zero_seconds_without_model_name():
    t = new_turn(100.0)
    assert t.status(100.0) == "⠋ 💭 Thinking (0s)"
    t.ticks = 2
    assert t.status(107.4) == "⠹ 💭 Thinking (7s)"


def test_thinking_counts_from_the_latest_model_call():
    t = new_turn(0.0)
    t.llm_request_started(50.0)
    assert t.status(53.0).endswith("💭 Thinking (3s)")


def test_running_tool_replaces_thinking_and_completes_into_history():
    t = new_turn(0.0)
    t.tool_started("call_1", "terminal", 10.0)
    assert t.status(14.0) == "⠋ 🔧 terminal (4s)"
    t.tool_finished("call_1", 13.0)
    assert t.completed == ["✓ terminal (3s)"]
    assert t.status(15.0) == "✓ terminal (3s)\n⠋ 💭 Thinking (2s)"


def test_parallel_tools_each_get_a_line_and_a_model_call_closes_the_round():
    t = new_turn(0.0)
    t.tool_started("a", "terminal", 1.0)
    t.tool_started("b", "web_search", 2.0)
    assert t.status(5.0).splitlines() == ["⠋ 🔧 terminal (4s)", "⠋ 🔧 web_search (3s)"]
    t.llm_request_started(6.0)  # core: a new model call means every tool of the round is done
    assert t.completed == ["✓ terminal (5s)", "✓ web_search (4s)"] and not t.tools


def test_completed_history_keeps_the_last_five():
    t = new_turn(0.0)
    for i in range(7):
        t.tool_started(str(i), f"t{i}", 0.0)
        t.tool_finished(str(i), 1.0)
    assert [line.split()[1] for line in t.completed] == ["t2", "t3", "t4", "t5", "t6"]


def test_finished_unknown_tool_is_ignored():
    t = new_turn(0.0)
    t.tool_finished("never-started", 5.0)
    assert t.completed == []


def test_reply_text_stops_the_thinking_line_but_notice_stays():
    t = new_turn(0.0)
    t.set_notice("n1", "⏳ Working — 3 min")
    i = t.add_segment("")
    assert "Thinking" in t.status(1.0)  # an empty first frame is not reply text
    t.set_segment(i, "Hello")
    assert t.status(2.0) == "⏳ Working — 3 min"


def test_body_joins_segments_and_strips_cursors():
    t = new_turn(0.0)
    t.add_segment("first ▉")
    t.add_segment("   ")
    t.add_segment("second")
    assert t.body((" ▉",)) == "first\n\nsecond"


def test_final_text_keeps_lasting_notices_but_drops_tools_and_live_status():
    t = new_turn(0.0)
    t.tool_started("x", "terminal", 0.0)
    t.tool_finished("x", 3.0)
    t.add_segment("answer ▉")
    t.set_notice("hb", "⏳ Working — 3 min")
    t.edit_notice("hb", "⏳ Working — 4 min")
    t.set_notice("ack", "↪ Redirected current run. I'll adjust using your correction.")
    t.set_notice("to", "⌛ Approval timed out after 5 min", lasting=True)
    assert t.final_text((" ▉",)) == "answer\n\n---\n⌛ Approval timed out after 5 min"
    assert new_turn().final_text(()) == ""  # an empty turn has nothing to show — no placeholder


def test_answers_opener_and_folded_inbound_messages():
    t = new_turn()
    t.aliases.add("m2")
    assert t.answers("m1") and t.answers("m2") and not t.answers("m3") and not t.answers("")


# ── MessageBook primitives used by the turn bubble ─────────────────────
def run(coro):
    return asyncio.run(coro)


def test_registered_message_draws_nothing_until_the_first_redraw():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = book.register("c", "r")
        assert wire.frames == 0 and book.get(msg.message_id) is msg
        await book.redraw(msg, lambda: ("", "⠋ 💭 Thinking (0s)"))
        return msg
    run(go())
    assert wire.visible() == ["⠋ 💭 Thinking (0s)"]


def test_redraw_puts_status_under_body_and_finalize_drops_it():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = book.register("c", "r")
        await book.redraw(msg, lambda: ("answer", "⠙ 💭 Thinking (2s)"))
        live = wire.visible()[0]
        await book.redraw(msg, lambda: ("answer\n\n---\n✓ terminal (3s)", ""), finalize=True)
        return live, msg
    live, msg = run(go())
    assert live == "answer\n\n---\n⠙ 💭 Thinking (2s)"
    assert wire.visible() == ["answer\n\n---\n✓ terminal (3s)"] and msg.finalized and msg.live is None


def test_turn_bubble_past_the_ten_minute_wall_rotates_and_status_moves_to_the_new_bubble():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = book.register("c", "r")
        await book.redraw(msg, lambda: ("", "⠋ 💭 Thinking (0s)"))
        clock.now = 545
        assert await book.rotate_due() == 1
        await book.redraw(msg, lambda: ("", "⠙ 💭 Thinking (546s)"))
        clock.now = 700  # first bubble is now past 600s; the second is not
        await book.redraw(msg, lambda: ("the answer", "⠹ 💭 Thinking (700s)"))
        await book.redraw(msg, lambda: ("the answer", ""), finalize=True)
    run(go())
    shown = wire.visible()
    assert len(shown) == 2
    assert "Thinking" not in shown[0].removesuffix(CONTINUED_SUFFIX)  # status never sealed into history
    assert shown[1] == "the answer"
    assert wire.finished == set(wire.order)


def test_update_with_overlay_draws_both_in_one_frame():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        await book.create("c", "r", "body", finalize=False, overlay="⠋ x")
        return wire.frames
    assert run(go()) == 1
    assert wire.visible() == ["body\n\n---\n⠋ x"]


def test_status_templates_match_only_core_status_text():
    hb = turn_mod.template_pattern("⏳ Working — {minutes} min{detail}")
    assert turn_mod.matches_status("⏳ Working — 3 min — iteration 1/90, waiting", [hb])
    assert not turn_mod.matches_status("⏳ Working — 3 min\nsecond line", [hb])
    assert not turn_mod.matches_status("⏳ Deploying now", [hb])  # emoji-led, but not this template
    ack = turn_mod.busy_ack_pattern(["↪ Redirected current run"], [". I'll adjust using your correction."])
    assert turn_mod.matches_status("↪ Redirected current run (3 min elapsed). I'll adjust using your correction.", [ack])
    assert not turn_mod.matches_status("↪ Redirected current run, then I deleted your files.", [ack])


def test_freeze_closes_running_tools_and_stops_thinking():
    t = new_turn(0.0)
    t.tool_started("a", "terminal", 1.0)
    t.freeze(4.0)
    assert t.completed == ["✓ terminal (3s)"] and not t.tools and t.status(5.0) == "✓ terminal (3s)"


def test_status_shows_only_the_newest_notices():
    t = new_turn(0.0)
    for i in range(5):
        t.set_notice(f"n{i}", f"note {i}")
    assert t.status(0.0).splitlines()[-3:] == ["note 2", "note 3", "note 4"]


def test_glued_segment_keeps_its_join_when_more_segments_follow():
    t = new_turn()
    t.add_segment("Part one. ")
    t.glued.add(0)
    t.add_segment("Part two.")
    shown = t.body()
    t.add_segment("Later reply.")
    assert t.body().startswith(shown) and t.body() == "Part one. Part two.\n\nLater reply."


# ── F2: progress lines label the RIGHT tool call ────────────────────────
P = {"web_search": ("🔍 web_search", "🔍 Searching the web"), "read_file": ("📖 read_file", "📖 Reading"),
     "terminal": ("💻 terminal",)}


def start(t, key, tool, now=0.0):
    t.tool_started(key, tool, now, P[tool])


def labels(t):
    return [c.running for c in t.calls]


def test_a_fresh_progress_message_after_a_reset_labels_only_the_later_calls():
    t = new_turn()
    start(t, "a", "web_search"); t.open_progress_message("p1"); t.apply_progress_lines("🔍 Searching the web for alpha", "p1")
    start(t, "b", "read_file"); t.open_progress_message("p2"); t.apply_progress_lines("📖 Reading notes.md", "p2")
    assert labels(t) == ["🔍 Searching the web for alpha", "📖 Reading notes.md"]


def test_a_repeat_counted_line_covers_that_many_calls():
    t = new_turn()
    for k in ("a", "b"):
        start(t, k, "terminal")
    start(t, "c", "web_search")
    t.open_progress_message("p"); t.apply_progress_lines("💻 terminal: ls (×2)\n🔍 Searching the web for beta", "p")
    assert labels(t) == ["💻 terminal: ls", "💻 terminal: ls", "🔍 Searching the web for beta"]


def test_new_mode_skipped_repeats_keep_their_bare_label_not_a_neighbours():
    t = new_turn()
    start(t, "a", "read_file"); start(t, "b", "read_file"); start(t, "c", "web_search")
    t.open_progress_message("p"); t.apply_progress_lines("📖 Reading a.md\n🔍 Searching the web for gamma", "p")
    assert labels(t) == ["📖 Reading a.md", "🔧 read_file", "🔍 Searching the web for gamma"]


def test_thinking_text_and_hints_in_the_progress_list_label_nothing():
    t = new_turn()
    start(t, "a", "web_search")
    t.open_progress_message("p")
    t.apply_progress_lines("💬 I should look this up first\n🔍 Searching the web for delta\n💡 Tip: /verbose shows full args", "p")
    assert labels(t) == ["🔍 Searching the web for delta"]


def test_reply_text_clears_tool_lines_and_a_later_round_shows_alone():
    t = new_turn(0.0)
    t.tool_started("a", "terminal", 0.0)
    t.tool_started("b", "web_search", 0.0)
    t.tool_finished("a", 2.0)
    assert t.status(3.0) == "✓ terminal (2s)\n⠋ 🔧 web_search (3s)"
    i = t.add_segment("Hello")
    assert t.status(4.0) == ""  # running and completed lines both gone, no thinking either
    t.llm_request_started(5.0)  # closes the still-running web_search: it stays hidden
    t.tool_started("c", "read_file", 5.0)
    assert t.status(6.0) == "⠋ 🔧 read_file (1s)"
    t.tool_finished("c", 7.0)
    assert t.status(7.0) == "✓ read_file (2s)\n⠋ 💭 Thinking (0s)"
    t.set_segment(i, "Hello")  # an edit that adds no text clears nothing
    assert t.status(7.0).startswith("✓ read_file (2s)")
    t.llm_request_started(8.0)
    t.set_segment(i, "Hello world")
    assert t.status(8.0) == "" and t.final_tail() == ""


def test_late_delivered_text_keeps_the_tool_its_own_model_call_started():
    """The consumer delivers a round's text on its own task, so it can land after that round's tool started
    (seen on the real authority path). The tool comes after the text: it stays, below it; the earlier
    round's tool is cleared."""
    t = new_turn(0.0)
    t.llm_request_started(0.0)
    t.tool_started("a", "terminal", 0.0)
    t.llm_request_started(2.0)  # round 2: the model says "let me search" and calls web_search
    t.tool_started("b", "web_search", 2.0)
    t.add_segment("let me search")  # ... whose text lands now
    assert t.status(3.0) == "⠋ 🔧 web_search (1s)"
    t.tool_finished("b", 4.0)
    assert t.status(4.0) == "✓ web_search (2s)\n⠋ 💭 Thinking (0s)"
    t.llm_request_started(4.0)
    t.add_segment("found it")
    assert t.status(5.0) == ""
