"""
Message sender — proactive send + stream reply with non-blocking semantics.

Aligned with OpenClaw: src/message-sender.ts and src/monitor.ts replyStream
flow. Adds:

  - send_we_com_reply             : block until ACK, with timeout + 846608/846609 handling
  - send_we_com_reply_non_blocking: skip intermediate frame if previous ACK pending
  - thinking message (<think></think> placeholder) helper
  - 6-minute stream timeout watchdog
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

try:
    from .constants import (
        REQUEST_TIMEOUT_SECONDS,
        STREAM_EXPIRED_ERRCODE,
        STREAM_NOT_SUBSCRIBED_ERRCODE,
    )
    from .stream import StreamExpiredError
except ImportError:  # pragma: no cover — tests use absolute imports
    from constants import (  # type: ignore[no-redef]
        REQUEST_TIMEOUT_SECONDS,
        STREAM_EXPIRED_ERRCODE,
        STREAM_NOT_SUBSCRIBED_ERRCODE,
    )
    from stream import StreamExpiredError  # type: ignore[no-redef]

logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────────────────

# Default per-frame send timeout. 25s leaves margin over SDK ack timeout (5s).
# Aligned with OpenClaw: src/const.ts:REPLY_SEND_TIMEOUT_MS
REPLY_SEND_TIMEOUT_S = 25.0

# Stream session lifetime — WeCom rejects updates after 6 minutes.
# Aligned with OpenClaw: const.ts:STREAM_EXPIRED_ERRCODE rationale.
STREAM_LIFETIME_S = 6 * 60.0  # 360s

# Thinking-message placeholder — triggers WeCom client's typing animation.
# Aligned with OpenClaw: const.ts:THINKING_MESSAGE (empty <think> tag)
THINKING_MESSAGE = "<think></think>"


class StreamNotSubscribedError(RuntimeError):
    """errcode 846609 — WS lost subscription, need reconnect.

    Aligned with OpenClaw: 846609 fallback in monitor.ts
    """

    def __init__(self, errmsg: str = "") -> None:
        super().__init__(f"WeCom stream not subscribed (errcode={STREAM_NOT_SUBSCRIBED_ERRCODE}): {errmsg}")
        self.errcode = STREAM_NOT_SUBSCRIBED_ERRCODE
        self.errmsg = errmsg


# ── Pending ACK tracker (one slot per streamId) ────────────────────────────


@dataclass
class _PendingAck:
    """Bookkeeping for in-flight non-blocking stream frames.

    Aligned with OpenClaw: replyStreamNonBlocking pending semantics.
    """

    stream_id: str
    in_flight: bool = False
    started_at: float = field(default_factory=time.time)


class NonBlockingStreamGate:
    """Per-streamId gate to skip intermediate frames when an ACK is pending.

    Final frames (finish=True) always pass through and never get skipped.
    Aligned with OpenClaw: src/message-sender.ts:sendWeComReplyNonBlocking
    """

    def __init__(self) -> None:
        self._gates: Dict[str, _PendingAck] = {}
        self._lock = asyncio.Lock()

    async def try_acquire(self, stream_id: str, *, finish: bool) -> bool:
        """Return True if caller may proceed; False if skipped.

        Always returns True when ``finish=True`` (final frame must go).
        """
        async with self._lock:
            slot = self._gates.get(stream_id)
            if finish:
                # Force-replace: final frame must run regardless of in-flight slot.
                self._gates[stream_id] = _PendingAck(
                    stream_id=stream_id, in_flight=True
                )
                return True
            if slot is None or not slot.in_flight:
                self._gates[stream_id] = _PendingAck(
                    stream_id=stream_id, in_flight=True
                )
                return True
            return False

    async def release(self, stream_id: str) -> None:
        async with self._lock:
            slot = self._gates.get(stream_id)
            if slot is not None:
                slot.in_flight = False

    async def clear(self, stream_id: str) -> None:
        async with self._lock:
            self._gates.pop(stream_id, None)


# ── Error classification ───────────────────────────────────────────────────


def classify_stream_error(err: BaseException) -> Optional[BaseException]:
    """Inspect a raised SDK error and re-classify into StreamExpired / NotSubscribed.

    Aligned with OpenClaw: src/message-sender.ts catch block.

    Returns the converted exception or ``None`` if it doesn't match.
    """
    errcode = getattr(err, "errcode", None)
    errmsg = getattr(err, "errmsg", "") or getattr(err, "message", "") or str(err)
    if errcode == STREAM_EXPIRED_ERRCODE or str(STREAM_EXPIRED_ERRCODE) in errmsg:
        return StreamExpiredError(errcode=STREAM_EXPIRED_ERRCODE, errmsg=errmsg)
    if errcode == STREAM_NOT_SUBSCRIBED_ERRCODE or str(STREAM_NOT_SUBSCRIBED_ERRCODE) in errmsg:
        return StreamNotSubscribedError(errmsg=errmsg)
    return None


def _extract_response_errcode(resp: Any) -> Optional[int]:
    """Pull errcode out of a server response dict (best-effort)."""
    if not isinstance(resp, dict):
        return None
    if "errcode" in resp:
        try:
            return int(resp["errcode"])
        except (TypeError, ValueError):
            return None
    data = resp.get("data")
    if isinstance(data, dict) and "errcode" in data:
        try:
            return int(data["errcode"])
        except (TypeError, ValueError):
            return None
    return None


# ── Send helpers ───────────────────────────────────────────────────────────


async def _with_timeout(coro: Awaitable[Any], timeout: float, what: str) -> Any:
    """Run ``coro`` with a wall-clock timeout.

    Aligned with OpenClaw: src/timeout.ts:withTimeout
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise asyncio.TimeoutError(f"{what} timed out after {timeout}s") from exc


async def send_we_com_reply(
    ws_client: Any,
    frame: Dict[str, Any],
    *,
    text: str,
    stream_id: str,
    finish: bool = True,
    timeout: float = REPLY_SEND_TIMEOUT_S,
) -> str:
    """Send a stream reply, blocking until ACK.

    Aligned with OpenClaw: src/message-sender.ts:sendWeComReply

    Raises:
        StreamExpiredError: server returned 846608 — caller should fallback to
            ``send_message``.
        StreamNotSubscribedError: server returned 846609 — caller should reconnect.
    """
    if not text:
        return ""

    body = frame.get("body") or {}
    msgtype = body.get("msgtype")

    # Event callbacks have no usable req_id for replyStream — use proactive send.
    # Aligned with OpenClaw: message-sender.ts event branch
    if msgtype == "event":
        if not finish:
            return stream_id
        chat_id = body.get("chatid") or (body.get("from") or {}).get("userid")
        if not chat_id:
            raise RuntimeError("Missing chatId for event callback reply")
        resp = await _with_timeout(
            ws_client.send_message(
                chat_id,
                {"msgtype": "markdown", "markdown": {"content": text}},
            ),
            timeout,
            f"Event reply send (streamId={stream_id})",
        )
        _raise_for_response_errcode(resp)
        return stream_id

    try:
        resp = await _with_timeout(
            ws_client.reply_stream(frame, stream_id, text, finish),
            timeout,
            f"Reply send (streamId={stream_id})",
        )
    except (StreamExpiredError, StreamNotSubscribedError):
        raise
    except Exception as err:  # noqa: BLE001
        reclassified = classify_stream_error(err)
        if reclassified is not None:
            raise reclassified from err
        raise

    _raise_for_response_errcode(resp)
    return stream_id


def _raise_for_response_errcode(resp: Any) -> None:
    """Raise StreamExpired/NotSubscribed if the dict response carries the code."""
    errcode = _extract_response_errcode(resp)
    if errcode == STREAM_EXPIRED_ERRCODE:
        errmsg = ""
        if isinstance(resp, dict):
            errmsg = resp.get("errmsg") or (resp.get("data") or {}).get("errmsg") or ""
        raise StreamExpiredError(errcode=STREAM_EXPIRED_ERRCODE, errmsg=errmsg)
    if errcode == STREAM_NOT_SUBSCRIBED_ERRCODE:
        errmsg = ""
        if isinstance(resp, dict):
            errmsg = resp.get("errmsg") or (resp.get("data") or {}).get("errmsg") or ""
        raise StreamNotSubscribedError(errmsg=errmsg)


async def send_we_com_reply_non_blocking(
    ws_client: Any,
    frame: Dict[str, Any],
    *,
    text: str,
    stream_id: str,
    gate: NonBlockingStreamGate,
    finish: bool = False,
    timeout: float = REPLY_SEND_TIMEOUT_S,
) -> str:
    """Non-blocking stream reply.

    If the previous frame for ``stream_id`` is still awaiting ACK, the call
    returns the literal ``'skipped'``. Final frames (finish=True) always run.

    Aligned with OpenClaw: src/message-sender.ts:sendWeComReplyNonBlocking
    """
    if not text:
        return "skipped"
    proceed = await gate.try_acquire(stream_id, finish=finish)
    if not proceed:
        return "skipped"
    try:
        return await send_we_com_reply(
            ws_client,
            frame,
            text=text,
            stream_id=stream_id,
            finish=finish,
            timeout=timeout,
        )
    finally:
        # Whether success, skipped, or error, release the slot so future frames flow.
        await gate.release(stream_id)


# ── Thinking message ───────────────────────────────────────────────────────


async def send_thinking_reply(
    ws_client: Any,
    frame: Dict[str, Any],
    *,
    stream_id: str,
    text: str = THINKING_MESSAGE,
    timeout: float = REPLY_SEND_TIMEOUT_S,
) -> None:
    """Emit the initial ``<think></think>`` placeholder.

    Aligned with OpenClaw: src/monitor.ts:sendThinkingReply
    """
    try:
        await send_we_com_reply(
            ws_client,
            frame,
            text=text,
            stream_id=stream_id,
            finish=False,
            timeout=timeout,
        )
    except StreamExpiredError:
        logger.warning("xwecom: stream expired during thinking reply")
        raise
    except Exception as err:  # noqa: BLE001
        logger.error(f"xwecom: failed to send thinking message: {err}")


# ── Stream-lifetime watchdog ───────────────────────────────────────────────


class StreamLifetimeWatcher:
    """Track stream age and flip a flag once the 6-minute window elapses.

    Aligned with OpenClaw: monitor.ts STREAM_EXPIRED_ERRCODE pre-emptive guard.

    Usage::

        watcher = StreamLifetimeWatcher(lifetime_s=STREAM_LIFETIME_S)
        watcher.mark_start(stream_id)
        ...
        if watcher.is_expired(stream_id):
            # bypass replyStream, use proactive sendMessage
    """

    def __init__(self, lifetime_s: float = STREAM_LIFETIME_S) -> None:
        self._lifetime_s = lifetime_s
        self._started_at: Dict[str, float] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._expired_callbacks: Dict[str, Callable[[str], Any]] = {}

    def mark_start(
        self,
        stream_id: str,
        *,
        on_expired: Optional[Callable[[str], Any]] = None,
    ) -> None:
        """Begin tracking a stream's lifetime.

        ``on_expired`` is invoked once the lifetime is reached (sync or async).
        """
        if stream_id in self._started_at:
            return
        self._started_at[stream_id] = time.time()
        if on_expired is not None:
            self._expired_callbacks[stream_id] = on_expired
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return

            async def _wait_and_fire() -> None:
                try:
                    await asyncio.sleep(self._lifetime_s)
                except asyncio.CancelledError:
                    return
                cb = self._expired_callbacks.pop(stream_id, None)
                if cb is not None:
                    res = cb(stream_id)
                    if asyncio.iscoroutine(res):
                        try:
                            await res
                        except Exception as err:  # noqa: BLE001
                            logger.warning(
                                f"xwecom: stream expiry callback raised: {err}"
                            )

            self._tasks[stream_id] = loop.create_task(_wait_and_fire())

    def is_expired(self, stream_id: str) -> bool:
        ts = self._started_at.get(stream_id)
        if ts is None:
            return False
        return (time.time() - ts) >= self._lifetime_s

    def remaining(self, stream_id: str) -> float:
        ts = self._started_at.get(stream_id)
        if ts is None:
            return self._lifetime_s
        return max(0.0, self._lifetime_s - (time.time() - ts))

    def cancel(self, stream_id: str) -> None:
        task = self._tasks.pop(stream_id, None)
        if task and not task.done():
            task.cancel()
        self._started_at.pop(stream_id, None)
        self._expired_callbacks.pop(stream_id, None)

    def cancel_all(self) -> None:
        for sid in list(self._tasks.keys()):
            self.cancel(sid)
