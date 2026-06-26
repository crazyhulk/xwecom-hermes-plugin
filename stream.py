"""Block stream manager — coalesce LLM tokens into sentence-aligned blocks.

Aligned with official wecom-openclaw-plugin/src/webhook/helpers.ts behavior:
each frame carries cumulative content, blocks are sentence-aligned 120-360 chars.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Callable, Coroutine, Dict, Optional

try:
    from .constants import (
        BLOCK_STREAM_IDLE_FLUSH,
        BLOCK_STREAM_MAX_CHARS,
        BLOCK_STREAM_MIN_CHARS,
        MAX_INTERMEDIATE_FRAMES,
        MAX_STREAM_CONTENT_LENGTH,
        SENTENCE_TERMINATORS,
        STREAM_EXPIRED_ERRCODE,
    )
except ImportError:
    from constants import (  # type: ignore[no-redef]
        BLOCK_STREAM_IDLE_FLUSH,
        BLOCK_STREAM_MAX_CHARS,
        BLOCK_STREAM_MIN_CHARS,
        MAX_INTERMEDIATE_FRAMES,
        MAX_STREAM_CONTENT_LENGTH,
        SENTENCE_TERMINATORS,
        STREAM_EXPIRED_ERRCODE,
    )

logger = logging.getLogger(__name__)


class StreamExpiredError(RuntimeError):
    """Raised when WeCom returns errcode 846608 (stream update expired)."""

    def __init__(self, errcode: int = STREAM_EXPIRED_ERRCODE, errmsg: str = ""):
        super().__init__(f"WeCom stream expired (errcode={errcode}): {errmsg or 'no detail'}")
        self.errcode = errcode
        self.errmsg = errmsg


class BlockChunker:
    """Coalesce streaming text into sentence-aligned blocks.

    Matches official wecom-openclaw-plugin behavior:
    - Don't emit below BLOCK_STREAM_MIN_CHARS (unless forced)
    - Force break above BLOCK_STREAM_MAX_CHARS
    - Break on sentence terminators when between min/max
    """

    def __init__(
        self,
        min_chars: int = BLOCK_STREAM_MIN_CHARS,
        max_chars: int = BLOCK_STREAM_MAX_CHARS,
    ):
        self._min_chars = min_chars
        self._max_chars = max_chars
        self._emitted_len = 0  # How many chars of cumulative text have been emitted

    def reset(self) -> None:
        """Reset state for a new stream."""
        self._emitted_len = 0

    def should_emit(self, cumulative_text: str, force: bool = False) -> bool:
        """Check if we should emit a frame for the current cumulative text.

        Args:
            cumulative_text: The full response text so far.
            force: Force emission (e.g., idle timeout or finalize).

        Returns:
            True if a frame should be emitted.
        """
        if force:
            return len(cumulative_text) > self._emitted_len

        new_tail_len = len(cumulative_text) - self._emitted_len
        if new_tail_len <= 0:
            return False

        # Hard cap — force a break
        if new_tail_len >= self._max_chars:
            return True

        # Below minimum — don't emit yet
        if new_tail_len < self._min_chars:
            return False

        # Between min and max — check for sentence boundary
        tail = cumulative_text[self._emitted_len:]
        for i in range(len(tail) - 1, max(len(tail) - 20, -1), -1):
            if i >= 0 and tail[i] in SENTENCE_TERMINATORS:
                return True

        return False

    def mark_emitted(self, cumulative_text: str) -> None:
        """Mark that we've emitted up to this point."""
        self._emitted_len = len(cumulative_text)

    @property
    def emitted_length(self) -> int:
        return self._emitted_len


class StreamSession:
    """Manages a single stream reply session."""

    def __init__(self, stream_id: str, req_id: str):
        self.stream_id = stream_id
        self.req_id = req_id
        self.chunker = BlockChunker()
        self.frame_count = 0
        self.started_at = time.time()
        self.finished = False

    def generate_stream_id() -> str:
        """Generate a unique stream ID."""
        return f"stream_{uuid.uuid4().hex[:12]}"


class BlockStreamManager:
    """Manages multiple concurrent stream sessions."""

    def __init__(self):
        self._sessions: Dict[str, StreamSession] = {}

    def create_session(self, req_id: str) -> StreamSession:
        """Create a new stream session for a request."""
        stream_id = f"stream_{uuid.uuid4().hex[:12]}"
        session = StreamSession(stream_id=stream_id, req_id=req_id)
        self._sessions[req_id] = session
        return session

    def get_session(self, req_id: str) -> Optional[StreamSession]:
        """Get existing session for a request."""
        return self._sessions.get(req_id)

    def finish_session(self, req_id: str) -> Optional[StreamSession]:
        """Mark session as finished and remove."""
        session = self._sessions.pop(req_id, None)
        if session:
            session.finished = True
        return session

    def can_send_frame(self, session: StreamSession) -> bool:
        """Check if we can send another intermediate frame."""
        return session.frame_count < MAX_INTERMEDIATE_FRAMES


# ── Re-exports from message_sender (so callers can `from stream import …`) ─

try:
    from .message_sender import (  # noqa: F401
        NonBlockingStreamGate,
        REPLY_SEND_TIMEOUT_S,
        STREAM_LIFETIME_S,
        StreamLifetimeWatcher,
        StreamNotSubscribedError,
        THINKING_MESSAGE,
        classify_stream_error,
        send_thinking_reply,
        send_we_com_reply,
        send_we_com_reply_non_blocking,
    )
except ImportError:  # pragma: no cover — flat-import fallback for tests
    try:
        from message_sender import (  # type: ignore[no-redef]  # noqa: F401
            NonBlockingStreamGate,
            REPLY_SEND_TIMEOUT_S,
            STREAM_LIFETIME_S,
            StreamLifetimeWatcher,
            StreamNotSubscribedError,
            THINKING_MESSAGE,
            classify_stream_error,
            send_thinking_reply,
            send_we_com_reply,
            send_we_com_reply_non_blocking,
        )
    except ImportError:
        # message_sender not installed yet — top-level helpers unavailable.
        pass
