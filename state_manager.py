"""
Global state manager — message states, reqId store, WSClient registry.

Aligned with OpenClaw: src/state-manager.ts

Implements:
  - Per-account WSClient instance map
  - Message state map (with TTL + cap-based eviction)
  - chatId → reqId persistent map (in-memory only — disk store omitted)
  - sessionKey → SessionChatInfo map (for original-case chatId recovery)
  - Periodic cleanup
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple


# ── Tunables (mirroring OpenClaw const.ts) ─────────────────────────────────

# Message state TTL — drop after 10 min idle.
MESSAGE_STATE_TTL_MS = 10 * 60 * 1000

# Cleanup tick — every 60s.
MESSAGE_STATE_CLEANUP_INTERVAL_MS = 60 * 1000

# Soft cap — evict oldest beyond this size.
MESSAGE_STATE_MAX_SIZE = 5000

# SessionChatInfo cap.
SESSION_CHAT_INFO_MAX_SIZE = 5000


# ── Data types ─────────────────────────────────────────────────────────────


@dataclass
class MessageState:
    """In-progress message state.

    Aligned with OpenClaw: src/interface.ts:MessageState
    """

    accumulated_text: str = ""
    stream_id: Optional[str] = None
    has_media: bool = False
    has_media_failed: bool = False
    media_error_summary: Optional[str] = None
    stream_expired: bool = False
    has_template_card: bool = False
    # Tracking for debouncing / bookkeeping
    started_at: float = field(default_factory=time.time)
    last_frame_at: float = field(default_factory=time.time)


@dataclass
class _MessageStateEntry:
    state: MessageState
    created_at: float


@dataclass
class SessionChatInfo:
    """Original-case session info.

    Aligned with OpenClaw: src/state-manager.ts:SessionChatInfo
    """

    chat_id: str
    chat_type: Literal["single", "group"]


# ── State container ────────────────────────────────────────────────────────


class StateManager:
    """Per-process state container.

    All operations are thread-safe via a single re-entrant lock; cheap because
    the contained operations are short and bursty.

    Aligned with OpenClaw: src/state-manager.ts SharedState semantics.
    """

    def __init__(
        self,
        ttl_ms: int = MESSAGE_STATE_TTL_MS,
        max_size: int = MESSAGE_STATE_MAX_SIZE,
    ) -> None:
        self._ttl_ms = ttl_ms
        self._max_size = max_size

        self._lock = threading.RLock()

        # account_id → WSClient (kept opaque to avoid SDK import cycle)
        self._ws_clients: Dict[str, Any] = {}

        # msg_id → _MessageStateEntry
        self._message_states: Dict[str, _MessageStateEntry] = {}

        # (account_id, chat_id) → reqId
        self._reqid_store: Dict[Tuple[str, str], str] = {}

        # session_key → SessionChatInfo
        self._session_chat_info: Dict[str, SessionChatInfo] = {}

        # connection state per account
        self._connection_state: Dict[str, str] = {}

        # cleanup task handle
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_running = False

    # ── WSClient registry ──────────────────────────────────────────────

    def set_ws_client(self, account_id: str, client: Any) -> None:
        with self._lock:
            self._ws_clients[account_id] = client

    def get_ws_client(self, account_id: str) -> Optional[Any]:
        with self._lock:
            return self._ws_clients.get(account_id)

    def delete_ws_client(self, account_id: str) -> None:
        with self._lock:
            self._ws_clients.pop(account_id, None)

    # ── Connection state ───────────────────────────────────────────────

    def set_connection_state(self, account_id: str, state: str) -> None:
        with self._lock:
            self._connection_state[account_id] = state

    def get_connection_state(self, account_id: str) -> Optional[str]:
        with self._lock:
            return self._connection_state.get(account_id)

    # ── Message state ──────────────────────────────────────────────────

    def set_message_state(self, message_id: str, state: MessageState) -> None:
        with self._lock:
            self._message_states[message_id] = _MessageStateEntry(
                state=state, created_at=time.time()
            )
            self._prune_locked()

    def get_message_state(self, message_id: str) -> Optional[MessageState]:
        with self._lock:
            entry = self._message_states.get(message_id)
            if not entry:
                return None
            if (time.time() - entry.created_at) * 1000 >= self._ttl_ms:
                self._message_states.pop(message_id, None)
                return None
            return entry.state

    def delete_message_state(self, message_id: str) -> None:
        with self._lock:
            self._message_states.pop(message_id, None)

    def clear_all_message_states(self) -> None:
        with self._lock:
            self._message_states.clear()

    def _prune_locked(self) -> None:
        """Drop expired entries; cap by max_size with oldest-first eviction."""
        now = time.time()
        expired_ms = self._ttl_ms
        # 1) Drop expired
        for key in [
            k
            for k, e in self._message_states.items()
            if (now - e.created_at) * 1000 >= expired_ms
        ]:
            self._message_states.pop(key, None)
        # 2) Cap
        overflow = len(self._message_states) - self._max_size
        if overflow > 0:
            sorted_keys = sorted(
                self._message_states.items(), key=lambda kv: kv[1].created_at
            )[:overflow]
            for k, _ in sorted_keys:
                self._message_states.pop(k, None)

    def prune(self) -> None:
        with self._lock:
            self._prune_locked()

    # ── ReqId store ────────────────────────────────────────────────────

    def set_reqid_for_chat(
        self, chat_id: str, req_id: str, account_id: str = "default"
    ) -> None:
        with self._lock:
            self._reqid_store[(account_id, chat_id)] = req_id

    def get_reqid_for_chat(
        self, chat_id: str, account_id: str = "default"
    ) -> Optional[str]:
        with self._lock:
            return self._reqid_store.get((account_id, chat_id))

    def delete_reqid_for_chat(
        self, chat_id: str, account_id: str = "default"
    ) -> None:
        with self._lock:
            self._reqid_store.pop((account_id, chat_id), None)

    @staticmethod
    def generate_req_id(prefix: str = "req") -> str:
        """Generate a unique reqId.

        Aligned with OpenClaw: utils generateReqId — random 12-char hex suffix.
        """
        return f"{prefix}_{uuid.uuid4().hex[:12]}"

    # ── Session chat info ──────────────────────────────────────────────

    def set_session_chat_info(self, session_key: str, info: SessionChatInfo) -> None:
        if not session_key:
            return
        with self._lock:
            if (
                len(self._session_chat_info) >= SESSION_CHAT_INFO_MAX_SIZE
                and session_key not in self._session_chat_info
            ):
                # Evict the oldest insertion-ordered entry
                oldest = next(iter(self._session_chat_info), None)
                if oldest is not None:
                    self._session_chat_info.pop(oldest, None)
            self._session_chat_info[session_key] = info

    def get_session_chat_info(
        self, session_key: Optional[str]
    ) -> Optional[SessionChatInfo]:
        if not session_key:
            return None
        with self._lock:
            return self._session_chat_info.get(session_key)

    def delete_session_chat_info(self, session_key: str) -> None:
        with self._lock:
            self._session_chat_info.pop(session_key, None)

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start_cleanup(
        self, interval_ms: int = MESSAGE_STATE_CLEANUP_INTERVAL_MS
    ) -> None:
        """Start periodic pruning task (async).

        Aligned with OpenClaw: src/state-manager.ts:startMessageStateCleanup
        """
        if self._cleanup_running:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop — pruning will run on-demand via setters.
            return
        self._cleanup_running = True
        interval_s = max(0.1, interval_ms / 1000.0)

        async def _loop() -> None:
            try:
                while self._cleanup_running:
                    await asyncio.sleep(interval_s)
                    self.prune()
            except asyncio.CancelledError:
                pass

        self._cleanup_task = loop.create_task(_loop())

    def stop_cleanup(self) -> None:
        """Stop periodic pruning task.

        Aligned with OpenClaw: src/state-manager.ts:stopMessageStateCleanup
        """
        self._cleanup_running = False
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
        self._cleanup_task = None

    async def cleanup_account(self, account_id: str) -> None:
        """Disconnect and forget an account.

        Aligned with OpenClaw: src/state-manager.ts:cleanupAccount
        """
        client = None
        with self._lock:
            client = self._ws_clients.pop(account_id, None)
            self._connection_state.pop(account_id, None)
        if client is not None:
            try:
                disc = getattr(client, "disconnect", None)
                if disc is not None:
                    res = disc()
                    if asyncio.iscoroutine(res):
                        await res
            except Exception:
                pass

    async def cleanup_all(self) -> None:
        """Disconnect every account and clear all state.

        Aligned with OpenClaw: src/state-manager.ts:cleanupAll
        """
        self.stop_cleanup()
        with self._lock:
            clients = list(self._ws_clients.items())
            self._ws_clients.clear()
            self._message_states.clear()
            self._reqid_store.clear()
            self._session_chat_info.clear()
            self._connection_state.clear()
        for _, client in clients:
            try:
                disc = getattr(client, "disconnect", None)
                if disc is not None:
                    res = disc()
                    if asyncio.iscoroutine(res):
                        await res
            except Exception:
                pass


# ── Process-singleton (mirrors OpenClaw globalThis pattern) ────────────────

_singleton: Optional[StateManager] = None
_singleton_lock = threading.Lock()


def get_state_manager() -> StateManager:
    """Return the process-wide StateManager singleton.

    Aligned with OpenClaw: src/state-manager.ts:getSharedState
    """
    global _singleton
    if _singleton is None:
        with _singleton_lock:
            if _singleton is None:
                _singleton = StateManager()
    return _singleton


def reset_state_manager() -> None:
    """Replace the singleton (test-only)."""
    global _singleton
    with _singleton_lock:
        _singleton = StateManager()
