"""Editable-message emulation over WeCom ``msgtype: stream`` bubbles.

WeCom has no edit API, but a reply *stream* is a bubble whose content can be replaced until it is closed
with ``finish=true`` (or reaches its hard 10-minute lifetime, errcode 846608). Hermes' edit-based streaming
expects a normal IM: ``send()`` returns a message id, ``edit_message(id, full_text)`` replaces it,
``finalize=True`` marks the last edit. This module bridges the two:

* one Hermes message id → one :class:`VirtualMessage` → a chain of WeCom stream bubbles. Only the last
  bubble can be live; earlier ones are sealed.
* an edit replaces the live bubble's content with the part of the body it owns (everything after the
  previous bubble's seal point), so a message that outgrows or outlives one bubble continues in the next
  one without repeating the sealed prefix.
* :meth:`MessageBook.rotate_due` seals a bubble at :data:`ROTATE_AFTER_SECONDS` (inside the 10-minute
  wall) and continues in a fresh one; Hermes keeps editing the same id and never notices.
* an edit after the chain closed (finalized, expired, turn ended) continues in a fresh bubble instead of
  being silently dropped — WeCom acks updates to a finished stream with errcode 0 but never renders them
  (verified against the live API).
* a transient *overlay* (tool timer lines) is drawn under the body between Hermes' edits; it never
  becomes part of a sealed bubble.

WeCom I/O goes through a :class:`StreamWire` (the adapter), which owns req_id routing, ack tracking,
coalescing and the final-frame retry.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol

logger = logging.getLogger(__name__)

# Measured: an update at 606s after the first frame gets 846608 "(>10 minutes)"; the finish frame is
# rejected too, so the seal must land before the wall.
ROTATE_AFTER_SECONDS = 540.0
# Per-frame UTF-8 byte cap is 20480; keep room for the overlay and the continuation marker.
BUBBLE_BYTE_BUDGET = 19000
CONTINUED_SUFFIX = "\n\n⏬"
THINKING_SEED = "<think></think>"  # WeCom renders this as its native "thinking" placeholder
OVERLAY_RULE = "\n\n---\n"
ZWSP = "​"


@dataclass
class FrameOutcome:
    ok: bool
    expired: bool = False  # 846608 / 846604: this stream (or its req_id) can no longer be updated
    indeterminate: bool = False  # sent, but the ack never confirmed it
    req_dead: bool = False  # 846604: the inbound req_id itself expired — nothing sent on it can render


class StreamWire(Protocol):
    async def stream_frame(self, req_id: str, stream_id: str, content: str, *, finish: bool) -> FrameOutcome: ...


@dataclass
class Bubble:
    stream_id: str
    opened_at: float
    offset: int  # body index where this bubble's text starts
    shown_upto: int = -1  # body index this bubble has displayed up to (-1 = nothing sent yet)
    shown: str = ""  # exact content of the last frame (dedup)
    closed: bool = False

    @property
    def end(self) -> int:
        return max(self.offset, self.shown_upto)


@dataclass
class VirtualMessage:
    message_id: str
    chat_id: str
    req_id: str
    body: str = ""
    overlay: str = ""
    bubbles: list[Bubble] = field(default_factory=list)
    finalized: bool = False
    confirmed_upto: int = 0  # body index an acked finish frame proved on screen
    req_dead: bool = False  # a frame on this req_id got 846604: nothing more can render there
    sealed_text: str = ""  # body[:sealed_upto] as it was when the chain was last sealed/closed
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def live(self) -> Optional[Bubble]:
        return self.bubbles[-1] if self.bubbles and not self.bubbles[-1].closed else None

    @property
    def sealed_upto(self) -> int:
        return self.bubbles[-1].end if self.bubbles else 0


def _new_stream_id() -> str:
    return f"stream_{uuid.uuid4().hex[:12]}"


def utf8_len(text: str) -> int:
    return len(text.encode("utf-8"))


def cut_point(text: str, budget: int) -> int:
    """Largest prefix length of *text* within *budget* UTF-8 bytes, preferring to cut at a line break."""
    if utf8_len(text) <= budget:
        return len(text)
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if utf8_len(text[:mid]) <= budget:
            lo = mid
        else:
            hi = mid - 1
    newline = text.rfind("\n", 0, lo)
    return newline if newline > lo // 2 else max(lo, 1)


class MessageBook:
    """The virtual messages of one adapter, keyed by Hermes message id."""

    def __init__(self, wire: StreamWire, *, rotate_after: float = ROTATE_AFTER_SECONDS,
                 byte_budget: int = BUBBLE_BYTE_BUDGET, clock: Callable[[], float] = time.monotonic):
        self._wire = wire
        self._rotate_after = rotate_after
        self._budget = byte_budget
        self._clock = clock
        self._messages: dict[str, VirtualMessage] = {}

    def get(self, message_id: str) -> Optional[VirtualMessage]:
        return self._messages.get(message_id)

    def open_messages(self, *, req_id: Optional[str] = None, chat_id: Optional[str] = None) -> list[VirtualMessage]:
        return [m for m in self._messages.values() if m.live is not None
                and (req_id is None or m.req_id == req_id) and (chat_id is None or m.chat_id == chat_id)]

    # ── operations (Hermes-facing) ──────────────────────────────────────
    def register(self, chat_id: str, req_id: str) -> VirtualMessage:
        """A message with nothing on screen yet: findable at once, drawn by the first ``update``."""
        msg = VirtualMessage(message_id=f"xw_{uuid.uuid4().hex[:16]}", chat_id=chat_id, req_id=req_id)
        self._messages[msg.message_id] = msg
        return msg

    def discard(self, msg: VirtualMessage) -> None:
        self._messages.pop(msg.message_id, None)

    async def create(self, chat_id: str, req_id: str, body: str, *, finalize: bool,
                     overlay: str = "") -> Optional[VirtualMessage]:
        msg = self.register(chat_id, req_id)
        if not await self.update(msg, body, finalize=finalize, overlay=overlay):
            self._messages.pop(msg.message_id, None)
            return None
        return msg

    async def update(self, msg: VirtualMessage, body: str, *, finalize: bool, overlay: Optional[str] = None) -> bool:
        """Replace the body (and, when given, the overlay in the same frame)."""
        async with msg.lock:
            msg.body = body
            if overlay is not None:
                msg.overlay = overlay
            if finalize:
                msg.overlay = ""
            ok = await self._render(msg, finish=finalize)
            if ok and finalize:
                msg.finalized = True
            return ok

    async def redraw(self, msg: VirtualMessage, compose: Callable[[], tuple[str, str]], *, finalize: bool = False,
                     final_overlay: str = "") -> bool:
        """Re-render from ``compose() -> (body, overlay)``, evaluated under the message lock so a
        concurrent writer can never be overwritten by an older snapshot. ``final_overlay`` is drawn under the body on the finish frame only — it
        never becomes part of ``body``, so the seal offsets stay valid for a later continuation."""
        async with msg.lock:
            msg.body, msg.overlay = compose()
            if finalize:
                msg.overlay = ""
            ok = await self._render(msg, finish=finalize, final_overlay=final_overlay)
            if ok and finalize:
                msg.finalized = True
            return ok

    async def set_overlay(self, msg: VirtualMessage, overlay: str) -> bool:
        """Redraw the transient overlay under the body. Never reopens a closed chain."""
        async with msg.lock:
            if msg.live is None or overlay == msg.overlay:
                return True
            msg.overlay = overlay
            return await self._render(msg, finish=False)

    async def close(self, msg: VirtualMessage, *, strip_suffixes: tuple[str, ...] = ()) -> None:
        """Seal whatever is live (the turn ended without a finalize edit, e.g. interrupted). A streaming
        cursor the caller left on the last interim body is stripped — a sealed bubble is not "typing"."""
        async with msg.lock:
            msg.overlay = ""
            for suffix in strip_suffixes:
                if suffix and msg.body.endswith(suffix):
                    msg.body = msg.body[: -len(suffix)]
                    break
            if msg.live is not None:
                await self._render(msg, finish=True)
            msg.finalized = True

    async def rotate_due(self) -> int:
        """Seal-and-continue every live bubble near the 10-minute wall. Returns how many rotated."""
        rotated = 0
        for msg in list(self._messages.values()):
            live = msg.live
            if live is None or self._clock() - live.opened_at < self._rotate_after:
                continue
            async with msg.lock:
                if msg.live is live and await self._rotate(msg, live):
                    rotated += 1
                    await self._render(msg, finish=False)
        return rotated

    def forget_closed(self, max_age: float = 3600.0) -> None:
        now = self._clock()
        for mid, msg in list(self._messages.items()):
            if msg.live is None and (not msg.bubbles or now - msg.bubbles[-1].opened_at > max_age):
                del self._messages[mid]

    # ── rendering ───────────────────────────────────────────────────────
    async def _render(self, msg: VirtualMessage, *, finish: bool, final_overlay: str = "") -> bool:
        live = msg.live
        if live is None:
            start = min(msg.sealed_upto, len(msg.body))
            known = msg.sealed_text[:start]
            if len(known) == start and msg.body[:start] != known:
                start = 0  # the edit rewrote text that is already sealed: show the new text whole
        # KNOWN GAP (pre-existing, not handled): this check covers a CLOSED chain only. While a chain is
        # still open, a body whose prefix changed under a bubble that was already sealed by spill
        # (``cut_point``) or by rotation (``_rotate``) is rendered from ``live.offset`` on — the sealed
        # bubble keeps the old prefix and the live bubble shows the new text from the same offset, so the
        # two read as garbled. Hermes' edit streams only append, and the turn bubble only appends segments,
        # so it does not arise there; a non-append edit to a long or >9-min open message would hit it.
            if msg.bubbles and start >= len(msg.body) and not final_overlay:
                return True  # every byte of the body is already on screen in sealed bubbles
            live = self._open(msg, start)
        # A body longer than one frame: seal full chunks and continue in new bubbles.
        while True:
            own = msg.body[live.offset:]
            cut = cut_point(own, self._budget)
            if cut >= len(own):
                break
            if not await self._seal(msg, live, live.offset + cut):
                return False
            live = self._open(msg, live.offset + cut)
        return await self._send_live(msg, live, finish=finish, final_overlay=final_overlay)

    async def _send_live(self, msg: VirtualMessage, live: Bubble, *, finish: bool, retried: bool = False,
                         final_overlay: str = "") -> bool:
        body = msg.body[live.offset:]
        content = body
        overlay = final_overlay if finish else msg.overlay
        if overlay:
            content = f"{body}{OVERLAY_RULE}{overlay}" if body else overlay
        if not content:
            content = "" if finish else THINKING_SEED
        if content == live.shown:
            if not finish:
                return True
            content += ZWSP  # WeCom silently drops a final frame identical to the previous one
        outcome = await self._wire.stream_frame(msg.req_id, live.stream_id, content, finish=finish)
        if outcome.req_dead:
            live.closed = msg.req_dead = True  # nothing sent on this req_id renders: deliver another way
            return False
        if outcome.expired:
            # The bubble died (wall reached, or a dead req_id). What it showed stays on screen; continue
            # the unshown rest in ONE fresh bubble. A dead req_id expires that one too → report failure so
            # the caller falls back to a proactive send.
            live.closed = True
            if retried:
                return False
            if finish and live.end >= len(msg.body):
                return True  # everything was already on screen; nothing to continue
            return await self._send_live(msg, self._open(msg, live.end), finish=finish, retried=True,
                                         final_overlay=final_overlay)
        if not outcome.ok:
            return False
        live.shown, live.shown_upto = content, len(msg.body)
        if finish:
            msg.confirmed_upto = len(msg.body)
            msg.sealed_text = msg.body
        live.closed = finish
        return True

    def _open(self, msg: VirtualMessage, offset: int) -> Bubble:
        bubble = Bubble(stream_id=_new_stream_id(), opened_at=self._clock(), offset=offset)
        msg.bubbles.append(bubble)
        return bubble

    async def _seal(self, msg: VirtualMessage, bubble: Bubble, upto: int) -> bool:
        """Close *bubble* showing ``body[offset:upto]`` plus a continuation marker."""
        content = msg.body[bubble.offset:upto] + CONTINUED_SUFFIX
        if content == bubble.shown:
            content += ZWSP
        outcome = await self._wire.stream_frame(msg.req_id, bubble.stream_id, content, finish=True)
        bubble.closed = True
        if outcome.req_dead:
            msg.req_dead = True
            return False
        if outcome.ok:
            bubble.shown, bubble.shown_upto = content, upto
            msg.sealed_text = msg.body[:upto]
            return True
        # An expired bubble keeps what it last showed; carry on from there. Any other failure is real.
        logger.warning("[xwecom] sealing bubble %s failed (expired=%s)", bubble.stream_id, outcome.expired)
        return outcome.expired

    async def _rotate(self, msg: VirtualMessage, live: Bubble) -> bool:
        """Seal the aged live bubble at the current body end and open its successor."""
        if not await self._seal(msg, live, max(len(msg.body), live.offset)):
            return False
        self._open(msg, msg.bubbles[-1].end)
        return True
