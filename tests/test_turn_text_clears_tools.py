"""Reply text clears the turn bubble's tool lines (parity with core's ``_append_accumulated``): tool lines
show while the turn has no new text, vanish on the first frame carrying text, reappear below the text for a
later tool round (without the earlier round's lines), and never survive into the closed bubble."""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_turn_bubble import Host, Wire, run, settle  # noqa: E402

TOOL_MARKS = ("✓", "🔧", "terminal", "web_search")


class RecordingWire(Wire):
    """Wire that also keeps every rendered frame in order, so the test can see the transitions."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.log: list[str] = []

    async def stream_frame(self, req_id, stream_id, content, *, finish):
        outcome = await super().stream_frame(req_id, stream_id, content, finish=finish)
        if req_id not in self.dead:
            self.log.append(content.replace("​", ""))
        return outcome


def recording_host() -> Host:
    h = Host()
    h.wire = RecordingWire(h.clock)
    h._book._wire = h.wire
    return h


async def two_tool_rounds(h: Host) -> dict[str, list[str]]:
    """thinking → terminal → text → web_search → more text → final answer; returns the frames per phase."""
    phases: dict[str, list[str]] = {}

    def mark(name: str) -> None:
        phases[name] = list(h.wire.log)
        h.wire.log.clear()

    e = h.inbound("u1", "u1", "m1", "R1")
    await h._start_turn(e)
    h.llm_request_started("u1", "m1", at=0.0)
    h._turn_tool_started("u1", "c1", "terminal", "m1", 0.0)
    await settle()
    h.clock.now = 2.0
    h._turn_tool_finished("u1", "c1", "m1", 2.0)
    h.llm_request_started("u1", "m1", at=2.0)
    await settle()
    mark("round1")
    r = await h.send("u1", "Let me check", reply_to="m1", metadata={"expect_edits": True})
    await settle()
    mark("text1")
    await h._edit_turn_part(r.message_id, "Let me check", finalize=True)  # tool-boundary preamble
    h.llm_request_started("u1", "m1", at=3.0)
    h._turn_tool_started("u1", "c2", "web_search", "m1", 3.0)
    await settle()
    h.clock.now = 5.0
    h._turn_tool_finished("u1", "c2", "m1", 5.0)
    await settle()
    await h._redraw_turn(h._live_turns("u1")[0])
    mark("round2")
    h.llm_request_started("u1", "m1", at=5.0)
    r2 = await h.send("u1", "Found it", reply_to="m1", metadata={"expect_edits": True})
    await settle()
    mark("text2")
    await h._edit_turn_part(r2.message_id, "Found it: 42", finalize=True)
    await h.send("u1", "Final answer", reply_to="m1", metadata={"notify": True})
    await h._complete_turns(e)
    mark("close")
    return phases


def test_tool_lines_show_before_any_text():
    phases = run(two_tool_rounds(recording_host()))
    frames = phases["round1"]
    assert any("🔧 terminal" in f for f in frames), frames
    assert frames[-1].startswith("✓ terminal (2s)\n") and "💭 Thinking" in frames[-1], frames


def test_first_frame_with_text_carries_no_tool_lines():
    phases = run(two_tool_rounds(recording_host()))
    frames = phases["text1"]
    assert frames and frames[0] == "Let me check", frames
    assert not any(m in f for f in frames for m in TOOL_MARKS), frames


def test_second_tool_round_shows_below_the_text_without_the_first_round():
    phases = run(two_tool_rounds(recording_host()))
    frames = phases["round2"]
    assert any(f.startswith("Let me check\n\n---\n") and "🔧 web_search" in f for f in frames), frames
    assert frames[-1].startswith("Let me check\n\n---\n✓ web_search (2s)"), frames
    assert not any("terminal" in f for f in frames), "an earlier round's tools came back"


def test_text_after_the_second_round_clears_it_again():
    phases = run(two_tool_rounds(recording_host()))
    frames = phases["text2"]
    assert frames and frames[0] == "Let me check\n\nFound it", frames
    assert not any(m in f for f in frames for m in TOOL_MARKS), frames


def test_closed_bubble_has_no_tool_footer():
    h = recording_host()
    run(two_tool_rounds(h))
    assert h.wire.visible() == ["Let me check\n\nFound it: 42\n\nFinal answer"]
    assert h.wire.finished == set(h.wire.order)
