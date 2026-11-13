"""Behaviour of the editable-message emulation against a fake WeCom wire that models what the live API
was measured to do: a finished stream accepts updates but never shows them; a stream older than 600s
rejects every frame with 846608."""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys


_ROOT = pathlib.Path(__file__).resolve().parents[1] / "xwecom"
_spec = importlib.util.spec_from_file_location("xwecom_messages", _ROOT / "messages.py")
messages = importlib.util.module_from_spec(_spec)
sys.modules["xwecom_messages"] = messages
_spec.loader.exec_module(messages)

FrameOutcome, MessageBook, CONTINUED_SUFFIX = messages.FrameOutcome, messages.MessageBook, messages.CONTINUED_SUFFIX


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FakeWeCom:
    """Screen model: stream_id -> (visible content, finished). Mirrors measured server behaviour."""

    def __init__(self, clock: Clock, *, dead_req_ids=()):
        self.clock, self.dead = clock, set(dead_req_ids)
        self.screen: dict[str, str] = {}
        self.order: list[str] = []
        self.finished: set[str] = set()
        self.first_frame_at: dict[str, float] = {}
        self.frames = 0

    async def stream_frame(self, req_id, stream_id, content, *, finish):
        self.frames += 1
        if req_id in self.dead:
            return FrameOutcome(ok=False, expired=True)
        started = self.first_frame_at.setdefault(stream_id, self.clock.now)
        if self.clock.now - started >= 600:
            return FrameOutcome(ok=False, expired=True)
        if stream_id in self.finished:
            return FrameOutcome(ok=True)  # acked, never rendered
        if stream_id not in self.screen:
            self.order.append(stream_id)
        self.screen[stream_id] = content
        if finish:
            self.finished.add(stream_id)
        return FrameOutcome(ok=True)

    def visible(self):
        return [self.screen[s].replace("​", "") for s in self.order]

    def text(self):
        return "".join(v.removesuffix(CONTINUED_SUFFIX) for v in self.visible())


def run(coro):
    return asyncio.run(coro)


def test_edits_replace_one_bubble_and_finalize_closes_it():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = await book.create("c", "r", "Hel", finalize=False)
        await book.update(msg, "Hello", finalize=False)
        await book.update(msg, "Hello world", finalize=True)
        return msg
    msg = run(go())
    assert wire.visible() == ["Hello world"]
    assert msg.live is None and msg.finalized


def test_edit_after_finalize_continues_in_a_new_bubble_instead_of_vanishing():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = await book.create("c", "r", "part one", finalize=True)
        await book.update(msg, "part one + part two", finalize=True)
    run(go())
    assert wire.text() == "part one + part two"
    assert len(wire.visible()) == 2


def test_rotation_before_the_ten_minute_wall_keeps_every_character_once():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = await book.create("c", "r", "A" * 10, finalize=False)
        clock.now = 545
        assert await book.rotate_due() == 1
        await book.update(msg, "A" * 10 + "B" * 5, finalize=False)
        clock.now = 1200  # the second bubble is far from its own wall
        await book.update(msg, "A" * 10 + "B" * 8, finalize=True)
    run(go())
    assert wire.text() == "A" * 10 + "B" * 8
    assert wire.visible()[0] == "A" * 10 + CONTINUED_SUFFIX


def test_expired_bubble_without_rotation_still_delivers_the_rest():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock, rotate_after=10_000)

    async def go():
        msg = await book.create("c", "r", "early", finalize=False)
        clock.now = 700  # missed the wall: the bubble is dead
        return await book.update(msg, "early and late", finalize=True)
    assert run(go()) is True
    assert wire.text() == "early and late"


def test_overlay_is_drawn_live_but_never_sealed():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = await book.create("c", "r", "body", finalize=False)
        await book.set_overlay(msg, "⠹ terminal 3s")
        shown_with_overlay = wire.visible()[0]
        await book.update(msg, "body done", finalize=True)
        return shown_with_overlay
    assert run(go()) == "body\n\n---\n⠹ terminal 3s"
    assert wire.visible() == ["body done"]


def test_body_over_one_frame_spills_into_further_bubbles():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock, byte_budget=20)
    text = "\n".join(f"line{i:02d}" for i in range(12))

    async def go():
        msg = await book.create("c", "r", text, finalize=True)
        return msg
    run(go())
    assert len(wire.visible()) > 1
    assert wire.text() == text
    assert all(len(v.encode()) <= 20 + len(CONTINUED_SUFFIX.encode()) + 3 for v in wire.visible())


def test_dead_req_id_reports_failure_so_the_caller_can_fall_back():
    clock = Clock(); wire = FakeWeCom(clock, dead_req_ids={"r"}); book = MessageBook(wire, clock=clock)
    assert run(book.create("c", "r", "hi", finalize=True)) is None


def test_close_seals_an_interrupted_message_without_the_overlay():
    clock = Clock(); wire = FakeWeCom(clock); book = MessageBook(wire, clock=clock)

    async def go():
        msg = await book.create("c", "r", "half", finalize=False)
        await book.set_overlay(msg, "⠹ x 1s")
        await book.close(msg)
        return msg
    run(go())
    assert wire.visible() == ["half"] and wire.finished == {wire.order[0]}
    assert book.open_messages(req_id="r") == []
