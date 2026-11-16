"""WeCom reply-stream wire: one ``msgtype: stream`` frame on the ``aibot_respond_msg`` command, with
per-req_id ack tracking.

WeCom acks a reply frame with a message carrying only the inbound ``req_id`` (no frame id, no stream id),
and several streams may be open on one req_id at once (a turn bubble plus other bubbles of the same
reply). Acks arrive in send order on the one connection (measured: every frame — final or not — gets
exactly one ``errcode`` ack, in order), so correlation is strictly FIFO per req_id:

* every frame sent on a req_id appends a waiter to that req_id's FIFO; each ack resolves the OLDEST
  waiter. No frame is ever refused or held back for an ack (the old design kept ONE pending slot per
  req_id; a confirmed frame then waited behind — and was resolved by — another stream's ack, timed out,
  and *poisoned* the req_id, after which every later intermediate frame was skipped and the turn bubble
  froze).
* intermediate frames are fire-and-forget: nothing awaits their waiter.
* only a FINISH frame awaits its own waiter (``REPLY_ACK_TIMEOUT``), as before the turn bubble; every
  other frame — every redraw of a live bubble — is fire-and-forget, whatever the ack state.
  A finish frame A timeout leaves its slot in
  the FIFO marked *timed out*: the late ack that eventually comes resolves that slot (and nothing else),
  so the order stays aligned. A timeout only makes THAT frame's delivery indeterminate — drawing never
  stops; there is no poisoning.
* ``_resolve_reply_ack`` with no waiter outstanding is an unrouted payload (returns False).

Errcodes: 846608 stream older than 10 minutes, 846604 req_id expired, 846609 subscription lost,
6000 a newer frame already replaced this one (benign for a final).
"""

from __future__ import annotations

import asyncio
import collections
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from .messages import FrameOutcome

logger = logging.getLogger(__name__)

APP_CMD_RESPONSE = "aibot_respond_msg"
APP_CMD_RESPONSE_UPDATE = "aibot_respond_update_msg"  # in-place template-card update after a click

STREAM_EXPIRED_ERRCODE = 846608
STREAM_REQUEST_EXPIRED_ERRCODE = 846604
STREAM_NOT_SUBSCRIBED_ERRCODE = 846609
STREAM_VERSION_CONFLICT_ERRCODE = 6000
EXPIRED_ERRCODES = (STREAM_EXPIRED_ERRCODE, STREAM_REQUEST_EXPIRED_ERRCODE)
MAX_STREAM_CONTENT_LENGTH = 20480  # bytes per frame, server-enforced
REPLY_ACK_TIMEOUT = 15.0  # official REPLY_SEND_TIMEOUT_MS
# An ack never came for this long: the slot is dropped from the FIFO head (a lost ack must not shift every
# later correlation forever). Far past REPLY_ACK_TIMEOUT; measured late acks arrive within ~6s.
STALE_ACK_SECONDS = 120.0


@dataclass
class ReplyFrame:
    body: dict[str, Any]
    future: asyncio.Future
    is_final: bool = False
    sent_at: float = 0.0


@dataclass
class ReplyQueue:
    """The frames sent on one inbound req_id whose ack has not arrived yet, oldest first."""
    req_id: str
    waiters: collections.deque = field(default_factory=collections.deque)

    @property
    def idle(self) -> bool:
        return not self.waiters


def truncate_utf8(content: str, limit: int = MAX_STREAM_CONTENT_LENGTH) -> str:
    encoded = content.encode("utf-8")
    return content if len(encoded) <= limit else encoded[:limit].decode("utf-8", errors="ignore")


class StreamWireMixin:
    """Mixed into the adapter; needs ``_send_json``, ``_require_ws``, ``_reply_queues`` and ``name``."""

    _REPLY_ACK_TIMEOUT = REPLY_ACK_TIMEOUT

    # ── messages.StreamWire ─────────────────────────────────────────────
    async def stream_frame(self, req_id: str, stream_id: str, content: str, *, finish: bool) -> FrameOutcome:
        """Intermediate frames return at once (never awaited, never skipped); a finish frame reports its ack:
        delivered, dead req_id (846604), expired stream (846608) or indeterminate (no ack in time)."""
        body = {"msgtype": "stream", "stream": {"id": stream_id, "finish": bool(finish), "content": truncate_utf8(content)}}
        try:
            if not finish:
                await self._send_reply_queued(req_id, body, is_final=False)
                return FrameOutcome(ok=True)
            response = await self._send_reply_queued(req_id, body, is_final=True)
        except Exception as exc:
            logger.warning("[%s] stream frame failed (req_id=%s finish=%s): %s", self.name, req_id, finish, exc)
            return FrameOutcome(ok=False)
        errcode = response.get("errcode", 0)
        if errcode in (STREAM_REQUEST_EXPIRED_ERRCODE, STREAM_NOT_SUBSCRIBED_ERRCODE):
            # 846604 req_id expired; 846609 subscription lost (kicked / reconnected): nothing sent on this
            # req_id renders any more either way.
            return FrameOutcome(ok=False, expired=True, req_dead=True)
        if errcode in EXPIRED_ERRCODES:
            return FrameOutcome(ok=False, expired=True)
        if response.get("errmsg") == "settlement_indeterminate":
            return FrameOutcome(ok=True, indeterminate=True)
        if errcode in (0, None, STREAM_VERSION_CONFLICT_ERRCODE):
            return FrameOutcome(ok=True)
        logger.warning("[%s] stream final rejected (req_id=%s): errcode=%s %s", self.name, req_id, errcode, response.get("errmsg"))
        return FrameOutcome(ok=False)

    # ── ack-tracked send ────────────────────────────────────────────────
    async def _send_reply_queued(self, req_id: str, body: dict[str, Any], *, is_final: bool) -> dict[str, Any]:
        """Send one reply frame on *req_id*. Intermediates return at once; a final frame awaits its own ack,
        re-sending the identical frame once on a timeout (same stream id + content is idempotent within the
        stream's window)."""
        self._require_ws()
        if not is_final:
            await self._dispatch_reply_frame(req_id, body, is_final=False)
            return {"errcode": 0, "errmsg": "sent_nonblocking"}
        for attempt in (1, 2):
            frame = await self._dispatch_reply_frame(req_id, body, is_final=True)
            try:
                return await asyncio.wait_for(asyncio.shield(frame.future), timeout=self._REPLY_ACK_TIMEOUT)
            except asyncio.TimeoutError:
                # The slot stays in the FIFO: its late ack must resolve IT, not the next frame's waiter.
                logger.warning("[%s] final frame ack timeout (req_id=%s, attempt %d)", self.name, req_id, attempt)
        return {"errcode": 0, "errmsg": "settlement_indeterminate"}

    async def _dispatch_reply_frame(self, req_id: str, body: dict[str, Any], *, is_final: bool) -> ReplyFrame:
        """Register the frame's waiter BEFORE sending (an ack can beat the send's return)."""
        queue = self._reply_queues.setdefault(req_id, ReplyQueue(req_id))
        frame = ReplyFrame(body=body, future=asyncio.get_running_loop().create_future(), is_final=is_final,
                           sent_at=time.monotonic())
        queue.waiters.append(frame)
        try:
            await self._send_json({"cmd": APP_CMD_RESPONSE, "headers": {"req_id": req_id}, "body": body})
        except Exception:
            # Never on the wire: no ack will come for it.
            if frame in queue.waiters:
                queue.waiters.remove(frame)
            frame.future.cancel()
            if queue.idle:
                self._reply_queues.pop(req_id, None)
            raise
        return frame

    def _resolve_reply_ack(self, req_id: str, payload: dict[str, Any]) -> bool:
        """Route an ack to the oldest frame on *req_id* still waiting for one; True when handled."""
        queue = self._reply_queues.get(req_id)
        if queue is None:
            return False
        now = time.monotonic()
        while len(queue.waiters) > 1 and now - queue.waiters[0].sent_at > STALE_ACK_SECONDS:
            lost = queue.waiters.popleft()  # its ack never came: realign on the frames after it
            logger.warning("[%s] no ack for a frame sent %.0fs ago on req_id=%s; dropped from the ack order",
                           self.name, now - lost.sent_at, req_id)
            if not lost.future.done():
                lost.future.set_result({"errcode": 0, "errmsg": "settlement_indeterminate"})
        if not queue.waiters:
            self._reply_queues.pop(req_id, None)
            return False
        frame = queue.waiters.popleft()
        if not frame.future.done():
            frame.future.set_result(payload)
        if queue.idle:
            self._reply_queues.pop(req_id, None)
        return True

    def _fail_reply_queues(self, error: Exception) -> None:
        for queue in self._reply_queues.values():
            for frame in queue.waiters:
                if not frame.future.done():
                    frame.future.set_exception(error)
        self._reply_queues.clear()
