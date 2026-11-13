"""WeCom reply-stream wire: one ``msgtype: stream`` frame on the ``aibot_respond_msg`` command, with
per-req_id ack tracking.

WeCom acks a reply frame with a message carrying only the inbound ``req_id`` (no frame id), so at most one
frame per req_id may wait for an ack at a time; anything else could not tell which frame an ack proves.

* Intermediate frames are fire-and-forget. While a frame is waiting for its ack, a newer intermediate is
  *coalesced* (the latest cumulative snapshot wins) and flushed when that ack arrives. Dropping instead
  froze the bubble on a stale prefix under ack lag.
* A final frame raises the *finalizing fence* (no coalesced successor may take the ack slot), drains the
  pending ack, sends, and awaits its own ack. On a timeout it re-sends the identical frame once: the same
  stream id + content is idempotent within the stream's window. Two timeouts mean the delivery is
  indeterminate, and the req_id is *poisoned*: a late ack can never again prove a later frame.

Errcodes: 846608 stream older than 10 minutes, 846604 req_id expired, 846609 subscription lost,
6000 a newer frame already replaced this one (benign for a final).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

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


@dataclass
class ReplyFrame:
    body: dict[str, Any]
    future: asyncio.Future
    is_final: bool = False
    sent_at: float = 0.0


class ReplyQueue:
    """Ack bookkeeping for one inbound req_id."""

    def __init__(self, req_id: str):
        self.req_id = req_id
        self.pending_ack: Optional[ReplyFrame] = None
        self.coalesced_body: Optional[dict[str, Any]] = None
        self.finalizing = False
        self.ack_poisoned = False

    @property
    def idle(self) -> bool:
        return self.pending_ack is None and self.coalesced_body is None and not self.finalizing and not self.ack_poisoned


def truncate_utf8(content: str, limit: int = MAX_STREAM_CONTENT_LENGTH) -> str:
    encoded = content.encode("utf-8")
    return content if len(encoded) <= limit else encoded[:limit].decode("utf-8", errors="ignore")


class StreamWireMixin:
    """Mixed into the adapter; needs ``_send_json``, ``_require_ws``, ``_reply_queues`` and ``name``."""

    _REPLY_ACK_TIMEOUT = REPLY_ACK_TIMEOUT

    # ── messages.StreamWire ─────────────────────────────────────────────
    async def stream_frame(self, req_id: str, stream_id: str, content: str, *, finish: bool) -> FrameOutcome:
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
        self._require_ws()
        queue = self._reply_queues.setdefault(req_id, ReplyQueue(req_id))
        if not is_final:
            if queue.finalizing or queue.ack_poisoned:
                return {"skipped": True, "errcode": 0}
            if queue.pending_ack is not None:
                queue.coalesced_body = body  # flushed by _resolve_reply_ack
                return {"skipped": True, "errcode": 0, "errmsg": "coalesced"}
            await self._dispatch_reply_frame(queue, body, is_final=False)
            return {"errcode": 0, "errmsg": "sent_nonblocking"}
        queue.finalizing = True
        try:
            if queue.pending_ack is not None:
                await self._drain_pending_ack(queue)
            if queue.ack_poisoned:
                # Send anyway, but no ack on this req_id can prove it any more.
                await self._send_json({"cmd": APP_CMD_RESPONSE, "headers": {"req_id": req_id}, "body": body})
                return {"errcode": 0, "errmsg": "settlement_indeterminate"}
            for attempt in (1, 2):
                frame = await self._dispatch_reply_frame(queue, body, is_final=True)
                try:
                    return await asyncio.wait_for(frame.future, timeout=self._REPLY_ACK_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.warning("[%s] final frame ack timeout (req_id=%s, attempt %d)", self.name, req_id, attempt)
                finally:
                    self._release_pending(queue, frame)
                    if attempt == 2:
                        queue.ack_poisoned = True  # an ack for either attempt may still arrive
            return {"errcode": 0, "errmsg": "settlement_indeterminate"}
        finally:
            queue.finalizing = False
            if queue.idle:
                self._reply_queues.pop(req_id, None)

    async def _dispatch_reply_frame(self, queue: ReplyQueue, body: dict[str, Any], *, is_final: bool) -> ReplyFrame:
        """Register the frame as the pending ack BEFORE sending (an ack can beat the send's return)."""
        frame = ReplyFrame(body=body, future=asyncio.get_running_loop().create_future(), is_final=is_final, sent_at=time.monotonic())
        self._reply_queues[queue.req_id] = queue
        queue.pending_ack = frame
        try:
            await self._send_json({"cmd": APP_CMD_RESPONSE, "headers": {"req_id": queue.req_id}, "body": body})
        except Exception:
            self._release_pending(queue, frame)
            frame.future.cancel()
            raise
        return frame

    async def _drain_pending_ack(self, queue: ReplyQueue) -> None:
        pending = queue.pending_ack
        try:
            await asyncio.wait_for(asyncio.shield(pending.future), timeout=self._REPLY_ACK_TIMEOUT)
        except asyncio.TimeoutError:
            queue.ack_poisoned = True
            pending.future.cancel()
            logger.warning("[%s] ack timeout draining a pending frame (req_id=%s)", self.name, queue.req_id)
        except Exception:
            pass
        queue.pending_ack = None
        queue.coalesced_body = None  # the final supersedes any buffered intermediate

    def _release_pending(self, queue: ReplyQueue, frame: ReplyFrame) -> None:
        if queue.pending_ack is frame:
            queue.pending_ack = None
        if queue.idle:
            self._reply_queues.pop(queue.req_id, None)

    def _resolve_reply_ack(self, req_id: str, payload: dict[str, Any]) -> bool:
        """Route an ack to its pending frame; True when handled."""
        queue = self._reply_queues.get(req_id)
        if queue is not None and queue.ack_poisoned:
            return True  # uncorrelatable late ack
        if queue is None or queue.pending_ack is None:
            return False
        frame = queue.pending_ack
        if not frame.future.done():
            frame.future.set_result(payload)
        queue.pending_ack = None
        if queue.coalesced_body is not None:
            with contextlib.suppress(RuntimeError):
                asyncio.get_running_loop().create_task(self._flush_coalesced(req_id))
        elif not queue.finalizing:
            self._reply_queues.pop(req_id, None)
        return True

    async def _flush_coalesced(self, req_id: str) -> None:
        queue = self._reply_queues.get(req_id)
        if queue is None or queue.coalesced_body is None or queue.pending_ack is not None or queue.finalizing or queue.ack_poisoned:
            return
        body, queue.coalesced_body = queue.coalesced_body, None
        with contextlib.suppress(Exception):  # a cumulative snapshot the next frame supersedes anyway
            await self._dispatch_reply_frame(queue, body, is_final=False)

    def _fail_reply_queues(self, error: Exception) -> None:
        for queue in self._reply_queues.values():
            if queue.pending_ack and not queue.pending_ack.future.done():
                queue.pending_ack.future.set_exception(error)
        self._reply_queues.clear()
