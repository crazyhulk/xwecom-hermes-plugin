"""
Monitor — buffered block dispatcher, enter_chat welcome, session bookkeeping,
disconnect-event handling.

Aligned with OpenClaw: src/monitor.ts (the bits that don't depend on
OpenClaw core dispatch). We supply a generic message-pipeline that the
adapter wires into Hermes.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Tunables (mirror OpenClaw const.ts) ────────────────────────────────────

# Aligned with OpenClaw: src/const.ts buffered dispatcher params
DEFAULT_BUFFER_WINDOW_S = 0.30  # 300ms debounce window
DEFAULT_BUFFER_FLUSH_MAX_S = 1.5  # never hold a message longer than this

# Aligned with OpenClaw: src/const.ts MESSAGE_PROCESS_TIMEOUT_MS
DEFAULT_MESSAGE_PROCESS_TIMEOUT_S = 6 * 60.0  # 6 minutes


@dataclass
class BufferedMessage:
    """One entry inside the per-chat debounce buffer."""

    frame: Dict[str, Any]
    enqueued_at: float = field(default_factory=time.time)


class BufferedBlockDispatcher:
    """Per-chat debounce buffer that flushes via ``handler`` when stable.

    Aligned with OpenClaw: src/monitor.ts buffered block dispatcher behavior.

    When a message lands:
      - if a buffer for that chat is open, the new message is appended.
      - the flush timer is reset (debounce).
      - if the buffer's oldest message is older than ``flush_max_s``, it is
        force-flushed even if new messages keep arriving.
    The handler is given the *list* of accumulated frames in arrival order.
    """

    def __init__(
        self,
        handler: Callable[[List[Dict[str, Any]]], Awaitable[None]],
        *,
        window_s: float = DEFAULT_BUFFER_WINDOW_S,
        flush_max_s: float = DEFAULT_BUFFER_FLUSH_MAX_S,
    ) -> None:
        self._handler = handler
        self._window_s = window_s
        self._flush_max_s = flush_max_s
        self._buffers: Dict[str, List[BufferedMessage]] = {}
        self._timers: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def submit(self, chat_id: str, frame: Dict[str, Any]) -> None:
        """Enqueue a frame for ``chat_id`` and (re)arm the flush timer."""
        async with self._lock:
            buffer = self._buffers.setdefault(chat_id, [])
            buffer.append(BufferedMessage(frame=frame))
            oldest = buffer[0].enqueued_at
            elapsed = time.time() - oldest

        if elapsed >= self._flush_max_s:
            await self._flush(chat_id)
            return

        # Re-arm timer
        loop = asyncio.get_running_loop()
        existing = self._timers.get(chat_id)
        if existing and not existing.done():
            existing.cancel()
        delay = min(self._window_s, max(0.0, self._flush_max_s - elapsed))
        self._timers[chat_id] = loop.create_task(self._delayed_flush(chat_id, delay))

    async def _delayed_flush(self, chat_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        await self._flush(chat_id)

    async def _flush(self, chat_id: str) -> None:
        async with self._lock:
            buffer = self._buffers.pop(chat_id, None) or []
            timer = self._timers.pop(chat_id, None)
        if timer and not timer.done():
            timer.cancel()
        if not buffer:
            return
        frames = [b.frame for b in buffer]
        try:
            await self._handler(frames)
        except Exception as err:  # noqa: BLE001
            logger.error(f"[monitor] dispatcher handler raised: {err}")

    async def flush_all(self) -> None:
        keys = list(self._buffers.keys())
        for k in keys:
            await self._flush(k)

    async def cancel_all(self) -> None:
        for task in list(self._timers.values()):
            if not task.done():
                task.cancel()
        self._timers.clear()
        self._buffers.clear()


# ── Enter-chat welcome handler ─────────────────────────────────────────────


@dataclass
class EnterChatContext:
    """Payload passed to the welcome callback.

    Aligned with OpenClaw: src/monitor.ts enter_chat reply path
    """

    frame: Dict[str, Any]
    user_id: str
    chat_id: str
    is_first_today: bool = True


async def handle_enter_chat_event(
    frame: Dict[str, Any],
    ws_client: Any,
    *,
    welcome_text: Optional[str] = None,
    welcome_builder: Optional[Callable[[EnterChatContext], Optional[str]]] = None,
) -> bool:
    """If ``frame`` is an ``enter_chat`` event, reply with a welcome message.

    Aligned with OpenClaw: monitor.ts enter_chat handler — uses ``reply_welcome``
    when available, else falls back to ``send_message``.

    Returns True when a welcome was sent, False otherwise.
    """
    body = frame.get("body") or {}
    event = body.get("event") or {}
    if event.get("eventtype") != "enter_chat":
        return False

    from_obj = body.get("from") or {}
    user_id = from_obj.get("userid", "")
    chat_id = body.get("chatid") or user_id
    ctx = EnterChatContext(frame=frame, user_id=user_id, chat_id=chat_id)

    text = welcome_builder(ctx) if welcome_builder else welcome_text
    if not text:
        return False

    payload = {"msgtype": "markdown", "markdown": {"content": text}}
    if hasattr(ws_client, "reply_welcome"):
        try:
            await ws_client.reply_welcome(frame, payload)
            return True
        except Exception as err:  # noqa: BLE001
            logger.warning(f"[monitor] reply_welcome failed: {err} — falling back")

    if chat_id:
        await ws_client.send_message(chat_id, payload)
        return True
    return False


# ── Disconnect / kicked-out handler ────────────────────────────────────────


async def handle_disconnected_event(
    frame: Dict[str, Any],
    ws_client: Any,
    *,
    on_kicked: Optional[Callable[[str], Awaitable[None]]] = None,
) -> bool:
    """Process a ``disconnected_event`` (kicked because of a new connection).

    Aligned with OpenClaw: monitor.ts ``event.disconnected_event`` listener.

    Cleanly disconnects the client and invokes ``on_kicked`` if supplied.
    Returns True if the event was the kicked-out one.
    """
    body = frame.get("body") or {}
    event = body.get("event") or {}
    if event.get("eventtype") != "disconnected_event":
        return False
    reason = (
        "Kicked by server: a new connection was established elsewhere. "
        "Auto-restart is suppressed to avoid mutual kicking."
    )
    try:
        res = ws_client.disconnect()
        if asyncio.iscoroutine(res):
            await res
    except Exception:
        pass
    if on_kicked is not None:
        try:
            await on_kicked(reason)
        except Exception as err:  # noqa: BLE001
            logger.warning(f"[monitor] on_kicked callback raised: {err}")
    return True


# ── Session recording ──────────────────────────────────────────────────────


@dataclass
class SessionRecord:
    """Tracks the state of one in-flight conversation turn.

    Aligned with OpenClaw: monitor.ts session bookkeeping (used to recover on
    reconnect and to detect orphaned long-running turns).
    """

    chat_id: str
    user_id: str
    message_id: str
    req_id: str
    stream_id: str
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    status: str = "processing"  # processing | finished | failed | timeout
    last_error: Optional[str] = None

    def mark_finished(self) -> None:
        self.finished_at = time.time()
        self.status = "finished"

    def mark_failed(self, error: str) -> None:
        self.finished_at = time.time()
        self.status = "failed"
        self.last_error = error

    def mark_timeout(self) -> None:
        self.finished_at = time.time()
        self.status = "timeout"


class SessionRecorder:
    """Records and queries in-flight sessions."""

    def __init__(self) -> None:
        self._records: Dict[str, SessionRecord] = {}
        self._lock = asyncio.Lock()

    async def open(self, record: SessionRecord) -> None:
        async with self._lock:
            self._records[record.message_id] = record

    async def get(self, message_id: str) -> Optional[SessionRecord]:
        async with self._lock:
            return self._records.get(message_id)

    async def close(self, message_id: str, *, error: Optional[str] = None) -> None:
        async with self._lock:
            rec = self._records.get(message_id)
            if not rec:
                return
            if error:
                rec.mark_failed(error)
            else:
                rec.mark_finished()

    async def active(self) -> List[SessionRecord]:
        async with self._lock:
            return [
                r for r in self._records.values() if r.status == "processing"
            ]

    async def drop_finished(self, older_than_s: float = 600.0) -> int:
        cutoff = time.time() - older_than_s
        async with self._lock:
            keys = [
                k
                for k, r in self._records.items()
                if r.finished_at is not None and r.finished_at < cutoff
            ]
            for k in keys:
                self._records.pop(k, None)
            return len(keys)


# ── Message processing timeout guard ───────────────────────────────────────


async def run_with_message_timeout(
    coro: Awaitable[Any],
    *,
    timeout_s: float = DEFAULT_MESSAGE_PROCESS_TIMEOUT_S,
    on_timeout: Optional[Callable[[], Awaitable[None]]] = None,
) -> Any:
    """Run ``coro`` with a hard wall-clock timeout.

    Aligned with OpenClaw: monitor.ts MESSAGE_PROCESS_TIMEOUT_MS guard.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    except asyncio.TimeoutError:
        logger.warning(f"[monitor] message processing exceeded {timeout_s}s")
        if on_timeout is not None:
            try:
                await on_timeout()
            except Exception as err:  # noqa: BLE001
                logger.warning(f"[monitor] on_timeout raised: {err}")
        raise
