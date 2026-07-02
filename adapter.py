"""
xwecom — WeCom platform adapter for Hermes Agent using official Python SDK.

Replaces the built-in wecom adapter with one based on the official
wecom-aibot-python-sdk-async, with bug fixes and feature alignment
to the official OpenClaw TypeScript plugin.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
import time
import uuid
from collections import deque
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

try:
    from aiohttp import ClientSession, web
except ImportError:
    ClientSession = None  # type: ignore[assignment]
    web = None  # type: ignore[assignment]

try:
    from gateway.platforms.base import cache_document_from_bytes, cache_image_from_bytes
except ImportError:
    cache_document_from_bytes = None
    cache_image_from_bytes = None

try:
    from gateway.status import acquire_scoped_lock, release_scoped_lock
except ImportError:
    acquire_scoped_lock = None
    release_scoped_lock = None

try:
    from .sdk import WSClient, WSClientOptions
    from .constants import (
        BLOCK_STREAM_IDLE_FLUSH,
        DEDUP_MAX_SIZE,
        DEDUP_TTL_SECONDS,
        MAX_INTERMEDIATE_FRAMES,
        MAX_MESSAGE_LENGTH,
        MAX_STREAM_CONTENT_LENGTH,
        STREAM_EXPIRED_ERRCODE,
        STREAM_KEEPALIVE_INTERVAL_SECONDS,
        STREAM_ROTATE_AFTER_SECONDS,
        TEXT_BATCH_DELAY_SECONDS,
        TEXT_BATCH_SPLIT_DELAY_SECONDS,
        TEXT_BATCH_SPLIT_THRESHOLD,
    )
    from .callback import (
        ParsedCallbackMessage,
        decrypt_callback_message,
        decrypt_verified_callback_message,
        extract_encrypt_from_xml,
        parse_callback_message_xml,
        verify_callback_signature,
    )
    from .media import (
        check_file_size,
        detect_media_type,
        download_and_decrypt,
        upload_and_send_media,
        upload_media_chunked,
    )
    from .message_parser import parse_message_content, parse_message_simple
    from .message_sender import NonBlockingStreamGate, THINKING_MESSAGE
    from .monitor import (
        BufferedBlockDispatcher,
        DEFAULT_MESSAGE_PROCESS_TIMEOUT_S,
        SessionRecorder,
        SessionRecord,
        handle_disconnected_event,
        handle_enter_chat_event,
        run_with_message_timeout,
    )
    from .policy import check_dm_policy, check_group_policy
    from .state_manager import get_state_manager
    from .stream import BlockChunker, BlockStreamManager, StreamExpiredError
    from .template_card import (
        TemplateCardCache,
        mask_template_card_blocks,
        process_template_cards_if_needed,
    )
except ImportError:
    from sdk import WSClient, WSClientOptions  # type: ignore[no-redef]
    from constants import (  # type: ignore[no-redef]
        BLOCK_STREAM_IDLE_FLUSH,
        DEDUP_MAX_SIZE,
        DEDUP_TTL_SECONDS,
        MAX_INTERMEDIATE_FRAMES,
        MAX_MESSAGE_LENGTH,
        MAX_STREAM_CONTENT_LENGTH,
        STREAM_EXPIRED_ERRCODE,
        STREAM_KEEPALIVE_INTERVAL_SECONDS,
        STREAM_ROTATE_AFTER_SECONDS,
        TEXT_BATCH_DELAY_SECONDS,
        TEXT_BATCH_SPLIT_DELAY_SECONDS,
        TEXT_BATCH_SPLIT_THRESHOLD,
    )
    from media import (  # type: ignore[no-redef]
        check_file_size,
        detect_media_type,
        download_and_decrypt,
        upload_and_send_media,
        upload_media_chunked,
    )
    from message_parser import parse_message_content, parse_message_simple  # type: ignore[no-redef]
    from message_sender import NonBlockingStreamGate, THINKING_MESSAGE  # type: ignore[no-redef]
    from monitor import (  # type: ignore[no-redef]
        BufferedBlockDispatcher,
        DEFAULT_MESSAGE_PROCESS_TIMEOUT_S,
        SessionRecorder,
        SessionRecord,
        handle_disconnected_event,
        handle_enter_chat_event,
        run_with_message_timeout,
    )
    from policy import check_dm_policy, check_group_policy  # type: ignore[no-redef]
    from state_manager import get_state_manager  # type: ignore[no-redef]
    from stream import BlockChunker, BlockStreamManager, StreamExpiredError  # type: ignore[no-redef]
    from template_card import (  # type: ignore[no-redef]
        TemplateCardCache,
        mask_template_card_blocks,
        process_template_cards_if_needed,
    )
    from callback import (  # type: ignore[no-redef]
        ParsedCallbackMessage,
        decrypt_callback_message,
        decrypt_verified_callback_message,
        extract_encrypt_from_xml,
        parse_callback_message_xml,
        verify_callback_signature,
    )

logger = logging.getLogger(__name__)


REPLY_MEDIA_DIRECTIVE_RE = re.compile(
    r"^\s*(?:[-*]\s+|\d+\.\s+)?(?:MEDIA|FILE)\s*:\s*(.+?)\s*$",
    re.IGNORECASE,
)

DEFAULT_CALLBACK_HOST = "0.0.0.0"
DEFAULT_CALLBACK_PORT = 8645
DEFAULT_CALLBACK_PATH = "/wecom/callback"
MAX_CALLBACK_BODY_BYTES = 65_536
CALLBACK_MESSAGE_DEDUP_TTL_SECONDS = 300
ACCESS_TOKEN_TTL_SECONDS = 7200


class MessageDeduplicator:
    """Simple time-based message deduplicator."""

    def __init__(self, max_size: int = DEDUP_MAX_SIZE, ttl: float = DEDUP_TTL_SECONDS):
        self._seen: deque = deque(maxlen=max_size)
        self._ttl = ttl

    def is_duplicate(self, msg_id: str) -> bool:
        now = time.time()
        # Clean expired entries
        while self._seen and (now - self._seen[0][1]) > self._ttl:
            self._seen.popleft()
        # Check
        for mid, _ in self._seen:
            if mid == msg_id:
                return True
        self._seen.append((msg_id, now))
        return False


class StreamTurn:
    """Per-turn stream state for native streaming.

    Each turn (an LLM generation cycle for a single inbound message) carries:

    * ``frame`` — the original SDK ``WsFrame`` whose ``headers.req_id`` is what
      ``WSClient.reply_stream()`` needs.  WeCom binds the stream to the
      *inbound* req_id, so every outbound frame in the turn must reuse it.
    * ``stream_id`` — a per-turn opaque token sent in every frame's
      ``stream.id`` field.  Subsequent frames with the same id update the
      same client-side bubble.
    * ``chunker`` — a :class:`BlockChunker` that coalesces the consumer's
      cumulative text snapshots into sentence-aligned blocks (120-360 chars).
    * counters / flags — guard the frame cap, idle-flush timer, and
      seed/finalize lifecycle.

    Keyed by ``(chat_id, turn_id)`` in :attr:`XWeComAdapter._stream_turns`.
    """

    __slots__ = (
        "chat_id",
        "req_id",
        "frame",
        "stream_id",
        "chunker",
        "seeded",
        "finalized",
        "expired",
        "frame_count",
        "last_sent_content",
        "pending_cumulative",
        "base_cumulative_len",
        "last_wire_content",
        "full_content_fallback_sent",
        "idle_flush_handle",
        "keepalive_handle",
        "rotation_handle",
        "start_time",
    )

    def __init__(self, chat_id: str, req_id: str, frame: Dict[str, Any]):
        self.chat_id = chat_id
        self.req_id = req_id
        self.frame = frame
        self.stream_id = f"stream_{uuid.uuid4().hex[:12]}"
        self.chunker: Optional[BlockChunker] = None
        self.seeded = False
        self.finalized = False
        self.expired = False
        self.frame_count = 0
        self.last_sent_content = ""
        # Latest cumulative text the consumer has pushed for this turn —
        # the chunker is stateless wrt cumulative storage so the idle-flush
        # timer needs it here to know what to drain.
        self.pending_cumulative = ""
        self.base_cumulative_len = 0
        self.last_wire_content = THINKING_MESSAGE
        self.full_content_fallback_sent = False
        self.idle_flush_handle: Optional[asyncio.TimerHandle] = None
        self.keepalive_handle: Optional[asyncio.TimerHandle] = None
        self.rotation_handle: Optional[asyncio.TimerHandle] = None
        self.start_time = time.monotonic()


class XWeComAdapter(BasePlatformAdapter):
    """WeCom adapter using official Python SDK."""

    # GatewayStreamConsumer gates native streaming on this class attribute
    # plus the supports_native_streaming() probe below.  WeCom's
    # ``msgtype: "stream"`` is exactly the cumulative-text protocol the
    # consumer expects: every frame carries the full response so far, the
    # client diff-renders it, and ``finish=True`` closes the bubble.
    SUPPORTS_NATIVE_STREAMING = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("xwecom"))
        extra = config.extra or {}

        # Credentials
        self._bot_id = extra.get("bot_id") or os.getenv("XWECOM_BOT_ID", "")
        self._secret = extra.get("secret") or os.getenv("XWECOM_SECRET", "")
        self._ws_url = (
            extra.get("websocket_url")
            or os.getenv("XWECOM_WEBSOCKET_URL", "wss://openws.work.weixin.qq.com")
        )

        # Access control
        self._dm_policy = extra.get("dm_policy", "open")
        self._group_policy = extra.get("group_policy", "open")
        self._allow_from = self._coerce_list(extra.get("allow_from"))
        self._group_allow_from = self._coerce_list(extra.get("group_allow_from"))
        self._groups_config = extra.get("groups", {})
        self._welcome_text = extra.get("welcome_text") or os.getenv("XWECOM_WELCOME_TEXT", "")
        self._message_timeout_s = float(
            extra.get("message_timeout_s", DEFAULT_MESSAGE_PROCESS_TIMEOUT_S)
        )
        self._stream_keepalive_interval_s = float(
            extra.get("stream_keepalive_interval_s", STREAM_KEEPALIVE_INTERVAL_SECONDS)
        )
        self._stream_rotate_after_s = float(
            extra.get("stream_rotate_after_s", STREAM_ROTATE_AFTER_SECONDS)
        )
        self._reply_ack_timeout_s = float(
            extra.get("reply_ack_timeout_s")
            or os.getenv("XWECOM_REPLY_ACK_TIMEOUT_S", "30")
        )
        self._text_batch_delay_s = float(
            extra.get("text_batch_delay_s", TEXT_BATCH_DELAY_SECONDS)
        )
        self._text_batch_split_delay_s = float(
            extra.get("text_batch_split_delay_s", TEXT_BATCH_SPLIT_DELAY_SECONDS)
        )

        # Optional self-built app HTTP callback channel.  This is disabled by
        # default so the plugin keeps the existing AI Bot WebSocket behavior
        # unless explicitly configured.
        self._callback_enabled = self._truthy(
            extra.get("callback_enabled") or os.getenv("XWECOM_CALLBACK_ENABLED")
        )
        self._callback_host = str(
            extra.get("callback_host")
            or os.getenv("XWECOM_CALLBACK_HOST", DEFAULT_CALLBACK_HOST)
        )
        self._callback_port = int(
            extra.get("callback_port")
            or os.getenv("XWECOM_CALLBACK_PORT", DEFAULT_CALLBACK_PORT)
        )
        self._callback_path = self._normalize_callback_path(
            extra.get("callback_path")
            or os.getenv("XWECOM_CALLBACK_PATH", DEFAULT_CALLBACK_PATH)
        )
        self._callback_apps = self._normalize_callback_apps(extra)
        self._callback_runner: Optional[Any] = None
        self._callback_site: Optional[Any] = None
        self._callback_http_session: Optional[Any] = None
        self._callback_seen_messages: Dict[str, float] = {}
        self._callback_chat_apps: Dict[str, str] = {}
        self._callback_access_tokens: Dict[str, Dict[str, Any]] = {}

        # Internal state
        self._client: Optional[WSClient] = None
        self._stream_mgr = BlockStreamManager()
        self._dedup = MessageDeduplicator()
        self._lock_acquired = False
        self._account_id = extra.get("account_id") or self._bot_id or "default"
        self._state = get_state_manager()
        self._session_recorder = SessionRecorder()
        self._template_card_cache = TemplateCardCache()
        self._stream_gate = NonBlockingStreamGate()

        # Native-streaming bookkeeping. ``_last_chat_req_ids`` maps each chat
        # to the most recent inbound req_id (WeCom binds stream replies to
        # the inbound req_id).  ``_stream_turns`` keys per-turn state by
        # ``f"{chat_id}:{turn_id}"`` so concurrent consumers (parallel
        # subagents, /background) don't trample each other.
        # ``_stream_expired_chats`` blocks NEW turns on a chat after a
        # 846608, but lets already-active turns continue to finalize.
        self._last_chat_req_ids: Dict[str, str] = {}
        self._last_chat_frames: Dict[str, Dict[str, Any]] = {}
        self._stream_turns: Dict[str, StreamTurn] = {}
        self._stream_expired_chats: set = set()
        self._pending_text_batches: Dict[str, MessageEvent] = {}
        self._pending_text_batch_tasks: Dict[str, asyncio.Task] = {}

    @staticmethod
    def _coerce_list(value: Any) -> List[str]:
        """Coerce config values into a trimmed string list."""
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        return [str(value).strip()] if str(value).strip() else []

    @staticmethod
    def _truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}

    @staticmethod
    def _normalize_callback_path(value: Any) -> str:
        path = str(value or DEFAULT_CALLBACK_PATH).strip() or DEFAULT_CALLBACK_PATH
        return path if path.startswith("/") else f"/{path}"

    @staticmethod
    def _normalize_callback_apps(extra: Dict[str, Any]) -> List[Dict[str, Any]]:
        apps = extra.get("callback_apps")
        if isinstance(apps, list) and apps:
            normalized = [dict(app) for app in apps if isinstance(app, dict)]
        else:
            normalized = [
                {
                    "name": extra.get("callback_name")
                    or os.getenv("XWECOM_CALLBACK_NAME", "default"),
                    "corp_id": extra.get("corp_id") or os.getenv("XWECOM_CORP_ID", ""),
                    "corp_secret": extra.get("corp_secret")
                    or os.getenv("XWECOM_CORP_SECRET", ""),
                    "agent_id": str(
                        extra.get("agent_id") or os.getenv("XWECOM_AGENT_ID", "")
                    ),
                    "token": extra.get("callback_token")
                    or extra.get("token")
                    or os.getenv("XWECOM_CALLBACK_TOKEN", ""),
                    "encoding_aes_key": extra.get("encoding_aes_key")
                    or os.getenv("XWECOM_ENCODING_AES_KEY", ""),
                }
            ]

        result: List[Dict[str, Any]] = []
        for idx, app in enumerate(normalized):
            name = str(app.get("name") or f"app{idx + 1}")
            result.append(
                {
                    "name": name,
                    "corp_id": str(app.get("corp_id") or ""),
                    "corp_secret": str(app.get("corp_secret") or ""),
                    "agent_id": str(app.get("agent_id") or ""),
                    "token": str(app.get("token") or ""),
                    "encoding_aes_key": str(app.get("encoding_aes_key") or ""),
                }
            )
        return result

    @staticmethod
    def _callback_app_configured(app: Dict[str, Any]) -> bool:
        return bool(
            app.get("corp_id")
            and app.get("token")
            and app.get("encoding_aes_key")
        )

    @staticmethod
    def _interpret_scoped_lock_result(
        result: Any,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Normalize Hermes scoped-lock return values across runtime versions."""
        if isinstance(result, tuple):
            acquired = bool(result[0]) if result else False
            existing = (
                result[1]
                if len(result) > 1 and isinstance(result[1], dict)
                else None
            )
            return acquired, existing
        return bool(result), None

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Establish configured WeCom channels."""
        has_ws_credentials = bool(self._bot_id and self._secret)
        has_callback_credentials = self._callback_enabled and any(
            self._callback_app_configured(app) for app in self._callback_apps
        )
        if not has_ws_credentials and not has_callback_credentials:
            logger.error(
                "xwecom: bot_id/secret or callback corp_id/token/encoding_aes_key are required"
            )
            return False

        # Token lock — prevent two profiles from using same WS credential.
        if has_ws_credentials and acquire_scoped_lock is not None:
            lock_result = acquire_scoped_lock("xwecom", self._bot_id)
            acquired, existing = self._interpret_scoped_lock_result(lock_result)
            if not acquired:
                owner_pid = existing.get("pid") if existing else None
                owner_suffix = f" (PID {owner_pid})" if owner_pid else ""
                logger.error(
                    "xwecom: Token already in use by another profile%s. "
                    "Stop the other gateway first.",
                    owner_suffix,
                )
                return False
            self._lock_acquired = True

        try:
            if has_ws_credentials:
                await self._connect_ws()
            if self._callback_enabled:
                await self._start_callback_server()
            self._state.start_cleanup()
            self._mark_connected()
            logger.info("xwecom: adapter connected and ready")
            return True
        except Exception as e:
            logger.error(f"xwecom: connection failed - {e}")
            await self._stop_callback_server()
            if self._client:
                try:
                    self._client.disconnect()
                except Exception:
                    pass
                self._client = None
            if self._lock_acquired and release_scoped_lock is not None:
                release_scoped_lock("xwecom", self._bot_id)
                self._lock_acquired = False
            return False

    async def _connect_ws(self) -> None:
        opts = WSClientOptions(
            bot_id=self._bot_id,
            secret=self._secret,
            ws_url=self._ws_url,
            heartbeat_interval=30000,
            max_reconnect_attempts=-1,  # Infinite reconnection
            reply_ack_timeout=self._reply_ack_timeout_s,
        )
        self._client = WSClient(opts)

        # Bind event handlers
        self._client.on("message", self._on_message)
        self._client.on("event", self._on_event)
        self._client.on("connected", lambda: logger.info("xwecom: WebSocket connected"))
        self._client.on(
            "authenticated", lambda: logger.info("xwecom: authenticated successfully")
        )
        self._client.on(
            "disconnected",
            lambda reason="": logger.warning(f"xwecom: disconnected - {reason}"),
        )
        self._client.on(
            "error",
            lambda e: logger.error(
                f"xwecom: error - {e}", exc_info=isinstance(e, BaseException)
            ),
        )

        await self._client.connect()
        self._state.set_ws_client(self._account_id, self._client)
        self._state.set_connection_state(self._account_id, "connected")

    async def disconnect(self) -> None:
        """Clean shutdown."""
        # Cancel any pending idle-flush timers so they don't fire during/after
        # teardown and try to send on a dead socket.
        for turn in list(self._stream_turns.values()):
            self._cancel_idle_flush(turn)
        self._stream_turns.clear()
        self._last_chat_req_ids.clear()
        self._last_chat_frames.clear()
        self._stream_expired_chats.clear()
        for task in list(self._pending_text_batch_tasks.values()):
            task.cancel()
        self._pending_text_batch_tasks.clear()
        self._pending_text_batches.clear()

        if self._client:
            try:
                # SDK's disconnect() is synchronous — don't await it.
                self._client.disconnect()
            except Exception as e:
                logger.warning(f"xwecom: disconnect error - {e}")
            self._client = None
        await self._stop_callback_server()
        self._state.delete_ws_client(self._account_id)
        self._state.set_connection_state(self._account_id, "disconnected")
        self._state.stop_cleanup()

        if self._lock_acquired and release_scoped_lock is not None:
            release_scoped_lock("xwecom", self._bot_id)
            self._lock_acquired = False

        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message (proactive push via aibot_send_msg)."""
        callback_app = self._resolve_callback_app_for_chat(chat_id)
        if callback_app is not None:
            return await self._send_callback_text(callback_app, chat_id, content)

        if not self._client:
            return SendResult(success=False, error="Not connected")

        outbound_text = content
        card_frame = self._build_outbound_frame(chat_id, reply_to)
        try:
            card_result = await process_template_cards_if_needed(
                self._client,
                card_frame,
                accumulated_text=content,
                account_id=self._account_id,
                cache=self._template_card_cache,
            )
            if card_result is not None:
                outbound_text = card_result.remaining_text
        except Exception as err:  # noqa: BLE001
            logger.warning("xwecom: proactive template card send failed: %s", err)

        outbound_text = await self._process_reply_media_directives(
            chat_id,
            outbound_text,
        )

        if not outbound_text.strip():
            return SendResult(
                success=True,
                message_id=f"xwecom_{chat_id}_{int(time.time() * 1000)}",
            )

        body = {"msgtype": "markdown", "markdown": {"content": outbound_text}}
        try:
            resp = await self._client.send_message(chat_id, body)
            errcode = resp.get("errcode", resp.get("data", {}).get("errcode", 0))
            if errcode and errcode != 0:
                errmsg = resp.get("errmsg", resp.get("data", {}).get("errmsg", ""))
                return SendResult(success=False, error=f"errcode={errcode}: {errmsg}")
            msg_id = f"xwecom_{chat_id}_{int(time.time() * 1000)}"
            return SendResult(success=True, message_id=msg_id)
        except RuntimeError as e:
            logger.error(f"xwecom send failed (not connected): {e}")
            return SendResult(success=False, error=str(e))
        except Exception as e:
            logger.error(f"xwecom send failed: {e}")
            return SendResult(success=False, error=str(e))

    @staticmethod
    def _build_outbound_frame(
        chat_id: str,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a minimal frame for proactive helpers that expect WsFrame."""
        return {
            "headers": {"req_id": reply_to or f"proactive_{uuid.uuid4().hex[:12]}"},
            "body": {
                "chatid": chat_id,
                "from": {"userid": chat_id},
                "chattype": "group" if XWeComAdapter._is_group_chat(chat_id) else "single",
            },
        }

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return chat metadata."""
        is_group = self._is_group_chat(chat_id)
        return {
            "name": chat_id,
            "type": "group" if is_group else "dm",
        }

    # ── Inbound message handling ────────────────────────────────────────────

    async def _on_message(self, frame: Dict[str, Any]) -> None:
        """Handle inbound user messages from WeCom.

        SDK pushes a ``WsFrame`` dict — the actual message payload lives
        in ``frame["body"]`` and follows the schema defined by the
        official OpenClaw plugin (``src/message-parser.ts:MessageBody``):
        ``msgid``, ``chattype`` ("single"|"group"), ``chatid``,
        ``from.userid`` / ``from.corpid``, ``msgtype``, etc.
        """
        await self._dispatch_frame_with_timeout(frame)

    async def _dispatch_frame_with_timeout(self, frame: Dict[str, Any]) -> None:
        body = frame.get("body") or {}
        headers = frame.get("headers") or {}
        msg_id = body.get("msgid") or headers.get("req_id") or ""

        async def on_timeout() -> None:
            if msg_id:
                await self._session_recorder.close(msg_id, error="timeout")

        try:
            await run_with_message_timeout(
                self._dispatch_frame_as_message(frame),
                timeout_s=self._message_timeout_s,
                on_timeout=on_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("xwecom: message processing timed out msg_id=%s", msg_id)

    async def _dispatch_frame_as_message(self, frame: Dict[str, Any]) -> None:
        """Parse, policy-check, cache media, and forward a frame to Hermes.

        Aligned with OpenClaw: monitor.ts:processWeComMessageNow, while keeping
        Hermes' BasePlatformAdapter.handle_message as the dispatch boundary.
        """
        body = frame.get("body") or {}
        headers = frame.get("headers") or {}
        msg_id = body.get("msgid") or headers.get("req_id") or ""
        req_id = headers.get("req_id") or ""

        # Trace non-text inbound so schema mismatches surface immediately.
        msgtype = body.get("msgtype", "")
        if msgtype and msgtype != "text":
            logger.info("xwecom: inbound frame msgtype=%s body_keys=%s",
                        msgtype, sorted(body.keys()))

        # Dedup
        if msg_id and self._dedup.is_duplicate(msg_id):
            logger.debug(f"xwecom: duplicate message {msg_id}, skipping")
            return

        # Extract sender / chat info — aligned with OpenClaw conventions:
        # body.from.userid is the sender; chat_id falls back to userid for DMs.
        sender = body.get("from") or {}
        user_id = sender.get("userid", "")
        user_name = sender.get("name") or user_id
        chat_type_raw = (body.get("chattype") or "").lower()
        is_group = chat_type_raw == "group"
        chat_id = body.get("chatid") or sender.get("chat_id") or ""
        if not chat_id and not is_group:
            # For DMs WeCom may omit chatid — route by user_id
            chat_id = user_id

        # Remember this chat's most recent inbound req_id and raw frame.
        # ``send_stream_frame`` needs them: WeCom's stream protocol binds
        # outbound stream frames to an inbound req_id and the raw frame
        # carries ``headers.req_id`` exactly where ``WSClient.reply_stream``
        # expects it.
        if chat_id and req_id:
            self._last_chat_req_ids[chat_id] = req_id
            self._last_chat_frames[chat_id] = frame
            self._state.set_reqid_for_chat(chat_id, req_id, self._account_id)
            # Cap memory: prune oldest entries beyond DEDUP_MAX_SIZE.
            while len(self._last_chat_req_ids) > DEDUP_MAX_SIZE:
                drop = next(iter(self._last_chat_req_ids))
                self._last_chat_req_ids.pop(drop, None)
                self._last_chat_frames.pop(drop, None)
            # A fresh inbound req_id resurrects the stream channel.
            self._stream_expired_chats.discard(chat_id)

        # Access control
        if is_group:
            if not check_group_policy(
                self._group_policy,
                self._group_allow_from,
                chat_id,
                user_id,
                self._groups_config,
            ):
                logger.debug(f"xwecom: group message rejected by policy: {chat_id}/{user_id}")
                return
        else:
            if not check_dm_policy(self._dm_policy, self._allow_from, user_id):
                logger.debug(f"xwecom: DM rejected by policy: {user_id}")
                return

        # Parse message content (handles text / image / mixed / quote / event)
        text, images = self._parse_message_content(body)

        msgtype_log = body.get("msgtype", "?")
        if images:
            logger.info(
                "xwecom: inbound media — msgtype=%s images=%d urls_present=%d aes_keys_present=%d",
                msgtype_log,
                len(images),
                sum(1 for i in images if i.get("url")),
                sum(1 for i in images if i.get("aes_key")),
            )

        # Download media attachments
        cached_images: List[str] = []
        for idx, img_info in enumerate(images):
            url = img_info.get("url", "")
            aes_key = img_info.get("aes_key") or img_info.get("aeskey") or ""
            if not url:
                logger.warning("xwecom: image %d has no url, skipping", idx)
                continue
            try:
                img_data = await download_and_decrypt(self._client, url, aes_key)
            except Exception as e:  # download_and_decrypt swallows but be safe
                logger.warning("xwecom: image %d download exception: %s", idx, e)
                img_data = None
            if not img_data:
                logger.warning(
                    "xwecom: image %d download returned no data (url=%s aes_key_len=%d)",
                    idx, url[:80], len(aes_key),
                )
                continue
            if cache_image_from_bytes is None:
                logger.warning("xwecom: cache_image_from_bytes unavailable in this Hermes build")
                continue
            # cache_image_from_bytes takes an EXTENSION (e.g. ".jpg"), not a filename.
            fname = img_info.get("filename") or "image.png"
            ext = os.path.splitext(fname)[1] or ".png"
            try:
                path = cache_image_from_bytes(img_data, ext)
                if path:
                    cached_images.append(str(path))
                    logger.info("xwecom: cached image %d -> %s (%d bytes)",
                                idx, path, len(img_data))
            except Exception as e:
                logger.warning("xwecom: failed to cache image %d: %s (%d bytes)", idx, e, len(img_data))

        # Build MessageEvent
        source = self.build_source(
            chat_id=chat_id,
            chat_name=body.get("chat_name") or chat_id,
            chat_type="group" if is_group else "dm",
            user_id=user_id,
            user_name=user_name,
        )

        msg_type = MessageType.TEXT
        if cached_images and not text:
            msg_type = MessageType.PHOTO

        event = MessageEvent(
            text=text or "",
            message_type=msg_type,
            source=source,
            message_id=msg_id,
            media_urls=cached_images,
            media_types=["image"] * len(cached_images),
        )

        # Store frame ref for potential stream reply
        if not hasattr(event, "metadata"):
            event.metadata = {}
        event.metadata["_xwecom_frame"] = frame  # type: ignore[attr-defined]

        if msg_id:
            await self._session_recorder.open(
                SessionRecord(
                    chat_id=chat_id,
                    user_id=user_id,
                    message_id=msg_id,
                    req_id=req_id,
                    stream_id="",
                )
            )

        try:
            handled_now = await self._dispatch_event_to_hermes(event)
        except Exception as err:
            if msg_id:
                await self._session_recorder.close(msg_id, error=str(err))
            raise
        else:
            if msg_id and handled_now:
                await self._session_recorder.close(msg_id)

    async def _dispatch_event_to_hermes(self, event: MessageEvent) -> bool:
        """Dispatch an event, batching rapid plain-text chunks per session.

        Returns True when ``handle_message`` completed before returning. A
        False return means a text batch owns the eventual dispatch and session
        recorder close.
        """
        if (
            event.message_type == MessageType.TEXT
            and not getattr(event, "media_urls", None)
            and self._text_batch_delay_s > 0
        ):
            self._enqueue_text_event(event)
            return False
        await self.handle_message(event)
        return True

    def _text_batch_key(self, event: MessageEvent) -> str:
        source = event.source
        chat_id = getattr(source, "chat_id", None)
        user_id = getattr(source, "user_id", None)
        thread_id = getattr(source, "thread_id", None)
        if isinstance(source, dict):
            chat_id = source.get("chat_id")
            user_id = source.get("user_id")
            thread_id = source.get("thread_id")
        return ":".join(str(part or "") for part in (chat_id, user_id, thread_id))

    def _enqueue_text_event(self, event: MessageEvent) -> None:
        key = self._text_batch_key(event)
        existing = self._pending_text_batches.get(key)
        chunk_len = len(event.text or "")
        if existing is None:
            setattr(event, "_xwecom_last_chunk_len", chunk_len)
            setattr(
                event,
                "_xwecom_batch_message_ids",
                [event.message_id] if event.message_id else [],
            )
            self._pending_text_batches[key] = event
        else:
            if event.text:
                existing.text = (
                    f"{existing.text}\n{event.text}" if existing.text else event.text
                )
            setattr(existing, "_xwecom_last_chunk_len", chunk_len)
            batch_ids = list(getattr(existing, "_xwecom_batch_message_ids", []))
            if event.message_id:
                batch_ids.append(event.message_id)
            setattr(existing, "_xwecom_batch_message_ids", batch_ids)

        prior = self._pending_text_batch_tasks.get(key)
        if prior and not prior.done():
            prior.cancel()
        task = asyncio.create_task(self._flush_text_batch(key))
        self._pending_text_batch_tasks[key] = task

    async def _flush_text_batch(self, key: str) -> None:
        current_task = asyncio.current_task()
        try:
            pending = self._pending_text_batches.get(key)
            last_len = getattr(pending, "_xwecom_last_chunk_len", 0) if pending else 0
            delay = (
                self._text_batch_split_delay_s
                if last_len >= TEXT_BATCH_SPLIT_THRESHOLD
                else self._text_batch_delay_s
            )
            await asyncio.sleep(delay)
            if self._pending_text_batch_tasks.get(key) is not current_task:
                return
            event = self._pending_text_batches.pop(key, None)
            if event is not None:
                batch_ids = list(getattr(event, "_xwecom_batch_message_ids", []))
                try:
                    await self.handle_message(event)
                except Exception as err:
                    for msg_id in batch_ids:
                        await self._session_recorder.close(msg_id, error=str(err))
                    raise
                else:
                    for msg_id in batch_ids:
                        await self._session_recorder.close(msg_id)
        finally:
            if self._pending_text_batch_tasks.get(key) is current_task:
                self._pending_text_batch_tasks.pop(key, None)

    async def _on_event(self, frame: Dict[str, Any]) -> None:
        """Handle WeCom events (enter_chat, etc.)."""
        body = frame.get("body") or {}
        event_obj = body.get("event") or {}
        event_type = event_obj.get("eventtype", "unknown")
        logger.debug(f"xwecom: received event: {event_type}")
        if not self._client:
            return

        if await handle_disconnected_event(
            frame,
            self._client,
            on_kicked=self._on_kicked_by_wecom,
        ):
            self._mark_disconnected()
            self._state.set_connection_state(self._account_id, "displaced")
            return

        if await handle_enter_chat_event(
            frame,
            self._client,
            welcome_text=self._welcome_text,
        ):
            return

        if event_type == "template_card_event":
            try:
                await self._update_template_card_on_event(frame)
            except Exception as err:  # noqa: BLE001
                logger.warning("xwecom: template card update failed: %s", err)

        parsed = parse_message_content(body)
        if parsed.text:
            await self._dispatch_frame_with_timeout(frame)

    async def _on_kicked_by_wecom(self, reason: str) -> None:
        logger.warning("xwecom: %s", reason)
        self._client = None

    async def _update_template_card_on_event(self, frame: Dict[str, Any]) -> bool:
        """Update cached template card state on click/select callbacks.

        Aligned with OpenClaw: template-card-manager.ts:updateTemplateCardOnEvent.
        """
        try:
            from .template_card import update_template_card_on_event
        except ImportError:  # pragma: no cover
            from template_card import update_template_card_on_event  # type: ignore[no-redef]

        if not self._client:
            return False
        return await update_template_card_on_event(
            self._client,
            frame,
            account_id=self._account_id,
            cache=self._template_card_cache,
        )

    # ── Self-built app HTTP callback handling ──────────────────────────────

    async def _start_callback_server(self) -> None:
        if not self._callback_enabled:
            return
        if web is None or ClientSession is None:
            raise RuntimeError("aiohttp is required for xwecom callback server")
        valid_apps = [
            app for app in self._callback_apps if self._callback_app_configured(app)
        ]
        if not valid_apps:
            raise RuntimeError("xwecom callback enabled but no callback app is configured")
        self._callback_apps = valid_apps

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.connect(("127.0.0.1", self._callback_port))
            raise RuntimeError(f"callback port {self._callback_port} already in use")
        except (ConnectionRefusedError, OSError):
            pass

        self._callback_http_session = ClientSession()
        app = web.Application(client_max_size=MAX_CALLBACK_BODY_BYTES)
        app.router.add_get("/health", self._handle_callback_health)
        app.router.add_get(self._callback_path, self._handle_callback_verify)
        app.router.add_post(self._callback_path, self._handle_callback_post)
        self._callback_runner = web.AppRunner(app)
        await self._callback_runner.setup()
        self._callback_site = web.TCPSite(
            self._callback_runner,
            self._callback_host,
            self._callback_port,
        )
        await self._callback_site.start()
        logger.info(
            "xwecom: callback server listening on %s:%s%s",
            self._callback_host,
            self._callback_port,
            self._callback_path,
        )

    async def _stop_callback_server(self) -> None:
        self._callback_site = None
        if self._callback_runner is not None:
            try:
                await self._callback_runner.cleanup()
            except Exception as err:  # noqa: BLE001
                logger.warning("xwecom: callback server cleanup failed: %s", err)
            self._callback_runner = None
        if self._callback_http_session is not None:
            try:
                await self._callback_http_session.close()
            except Exception as err:  # noqa: BLE001
                logger.warning("xwecom: callback HTTP session cleanup failed: %s", err)
            self._callback_http_session = None

    async def _handle_callback_health(self, request: Any) -> Any:
        del request
        return web.json_response({"status": "ok", "platform": "xwecom"})

    async def _handle_callback_verify(self, request: Any) -> Any:
        msg_signature = request.query.get("msg_signature", "")
        timestamp = request.query.get("timestamp", "")
        nonce = request.query.get("nonce", "")
        echostr = request.query.get("echostr", "")

        for app in self._callback_apps:
            try:
                if not verify_callback_signature(
                    token=str(app.get("token") or ""),
                    timestamp=timestamp,
                    nonce=nonce,
                    msg_encrypt=echostr,
                    signature=msg_signature,
                ):
                    continue
                decrypted = decrypt_callback_message(
                    encoding_aes_key=str(app.get("encoding_aes_key") or ""),
                    encrypted=echostr,
                )
                if decrypted.corp_id != str(app.get("corp_id") or ""):
                    continue
                return web.Response(text=decrypted.xml, content_type="text/plain")
            except Exception:
                continue
        return web.Response(status=403, text="signature verification failed")

    async def _handle_callback_post(self, request: Any) -> Any:
        msg_signature = request.query.get("msg_signature", "")
        timestamp = request.query.get("timestamp", "")
        nonce = request.query.get("nonce", "")
        body_bytes = await request.read()
        if len(body_bytes) > MAX_CALLBACK_BODY_BYTES:
            return web.Response(status=413, text="payload too large")
        body = body_bytes.decode("utf-8", errors="replace")

        for app in self._callback_apps:
            try:
                verified = decrypt_verified_callback_message(
                    token=str(app.get("token") or ""),
                    encoding_aes_key=str(app.get("encoding_aes_key") or ""),
                    receive_id=str(app.get("corp_id") or ""),
                    timestamp=timestamp,
                    nonce=nonce,
                    msg_signature=msg_signature,
                    outer_xml=body,
                )
                event = await self._build_event_from_callback(app, verified.parsed)
                if event is not None:
                    if self._is_duplicate_callback(event.message_id):
                        return web.Response(text="success", content_type="text/plain")
                    task = asyncio.create_task(self._dispatch_event_to_hermes(event))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                return web.Response(text="success", content_type="text/plain")
            except ValueError:
                continue
            except Exception:
                logger.exception("xwecom: callback POST handling failed")
                break
        return web.Response(status=400, text="invalid callback payload")

    async def _build_event_from_callback(
        self,
        app: Dict[str, Any],
        parsed: Optional[ParsedCallbackMessage],
    ) -> Optional[MessageEvent]:
        if parsed is None:
            return None
        text = parsed.text or ""
        if not text and parsed.media_type:
            text = f"[{parsed.media_type}消息]"
        media_urls, media_types = await self._download_callback_media_if_any(
            app,
            parsed,
        )
        scoped_chat_id = self._callback_chat_key(
            str(app.get("corp_id") or ""),
            parsed.chat_id,
        )
        self._callback_chat_apps[scoped_chat_id] = str(app.get("name") or "")
        self._callback_chat_apps[parsed.chat_id] = str(app.get("name") or "")

        source = self.build_source(
            chat_id=scoped_chat_id,
            chat_name=parsed.chat_id,
            chat_type="group" if parsed.is_group_chat else "dm",
            user_id=parsed.sender_id,
            user_name=parsed.sender_id,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=parsed.msg_id,
            media_urls=media_urls,
            media_types=media_types,
        )
        if not hasattr(event, "metadata"):
            event.metadata = {}
        event.metadata["_xwecom_callback"] = {  # type: ignore[attr-defined]
            "app": str(app.get("name") or ""),
            "media_id": parsed.media_id,
            "media_type": parsed.media_type,
        }
        return event

    async def _download_callback_media_if_any(
        self,
        app: Dict[str, Any],
        parsed: ParsedCallbackMessage,
    ) -> Tuple[List[str], List[str]]:
        if not parsed.media_id or not parsed.media_type:
            return [], []
        if cache_image_from_bytes is None or cache_document_from_bytes is None:
            logger.warning("xwecom: callback media cache helpers unavailable")
            return [], []
        try:
            token = await self._get_callback_access_token(app)
            session = await self._ensure_callback_http_session()
            async with session.get(
                "https://qyapi.weixin.qq.com/cgi-bin/media/get",
                params={"access_token": token, "media_id": parsed.media_id},
            ) as resp:
                raw = await resp.read()
                headers = getattr(resp, "headers", {}) or {}
                status = getattr(resp, "status", 200)
            if status >= 400:
                logger.warning(
                    "xwecom: callback media download failed status=%s media_id=%s",
                    status,
                    parsed.media_id,
                )
                return [], []
            content_type = str(headers.get("content-type") or "").split(";", 1)[0].strip()
            if content_type == "application/json":
                logger.warning("xwecom: callback media download returned JSON error: %s", raw[:200])
                return [], []
            filename = self._filename_from_content_disposition(
                str(headers.get("content-disposition") or "")
            )
            if not filename:
                filename = self._callback_media_filename(
                    parsed.media_id,
                    parsed.media_type,
                    content_type,
                )
            detected = detect_media_type(content_type, filename)
            ok, final_type, error = check_file_size(raw, detected, filename)
            if not ok:
                logger.warning("xwecom: callback media rejected: %s", error)
                return [], []
            if final_type == "image":
                ext = os.path.splitext(filename)[1] or self._extension_for_mime(
                    content_type,
                    ".jpg",
                )
                path = cache_image_from_bytes(raw, ext)
                return [str(path)], [content_type or "image/jpeg"]
            path = cache_document_from_bytes(raw, filename)
            return [str(path)], [content_type or "application/octet-stream"]
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "xwecom: callback media download failed media_id=%s: %s",
                parsed.media_id,
                err,
            )
            return [], []

    @staticmethod
    def _filename_from_content_disposition(disposition: str) -> str:
        if not disposition:
            return ""
        match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", disposition, re.IGNORECASE)
        if match:
            return unquote(match.group(1).strip().strip('"'))
        match = re.search(r'filename\s*=\s*"?([^";]+)"?', disposition, re.IGNORECASE)
        if match:
            return unquote(match.group(1).strip())
        return ""

    @staticmethod
    def _extension_for_mime(content_type: str, fallback: str) -> str:
        mapping = {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "image/webp": ".webp",
            "audio/amr": ".amr",
            "video/mp4": ".mp4",
            "application/pdf": ".pdf",
            "text/plain": ".txt",
        }
        return mapping.get(content_type.lower(), fallback)

    @classmethod
    def _callback_media_filename(
        cls,
        media_id: str,
        media_type: str,
        content_type: str,
    ) -> str:
        ext = cls._extension_for_mime(
            content_type,
            ".jpg" if media_type == "image" else ".amr" if media_type == "voice" else ".bin",
        )
        return f"{media_id}{ext}"

    def _is_duplicate_callback(self, msg_id: str) -> bool:
        if not msg_id:
            return False
        now = time.time()
        seen_at = self._callback_seen_messages.get(msg_id)
        if seen_at is not None and now - seen_at < CALLBACK_MESSAGE_DEDUP_TTL_SECONDS:
            return True
        self._callback_seen_messages[msg_id] = now
        if len(self._callback_seen_messages) > 2000:
            cutoff = now - CALLBACK_MESSAGE_DEDUP_TTL_SECONDS
            self._callback_seen_messages = {
                key: ts for key, ts in self._callback_seen_messages.items() if ts > cutoff
            }
        return False

    @staticmethod
    def _callback_chat_key(corp_id: str, user_id: str) -> str:
        return f"{corp_id}:{user_id}" if corp_id else user_id

    def _get_callback_app_by_name(self, name: Optional[str]) -> Optional[Dict[str, Any]]:
        if not name:
            return None
        for app in self._callback_apps:
            if app.get("name") == name:
                return app
        return None

    def _resolve_callback_app_for_chat(self, chat_id: str) -> Optional[Dict[str, Any]]:
        app_name = self._callback_chat_apps.get(chat_id)
        if not app_name and ":" not in chat_id:
            matches = [
                key for key in self._callback_chat_apps if key.endswith(f":{chat_id}")
            ]
            if len(matches) == 1:
                app_name = self._callback_chat_apps.get(matches[0])
        return self._get_callback_app_by_name(app_name)

    async def _send_callback_text(
        self,
        app: Dict[str, Any],
        chat_id: str,
        content: str,
    ) -> SendResult:
        if web is None or ClientSession is None:
            return SendResult(success=False, error="aiohttp is required for callback send")
        touser = chat_id.split(":", 1)[1] if ":" in chat_id else chat_id
        payload = {
            "touser": touser,
            "msgtype": "text",
            "agentid": int(str(app.get("agent_id") or 0)),
            "text": {"content": content[:2048]},
            "safe": 0,
        }
        try:
            for attempt in range(2):
                token = await self._get_callback_access_token(app)
                session = await self._ensure_callback_http_session()
                async with session.post(
                    "https://qyapi.weixin.qq.com/cgi-bin/message/send",
                    params={"access_token": token},
                    json=payload,
                ) as resp:
                    data = await resp.json(content_type=None)
                errcode = data.get("errcode")
                if errcode in {40001, 42001} and attempt == 0:
                    self._callback_access_tokens.pop(str(app.get("name") or ""), None)
                    continue
                if errcode != 0:
                    return SendResult(success=False, error=str(data), raw_response=data)
                return SendResult(
                    success=True,
                    message_id=str(data.get("msgid") or ""),
                    raw_response=data,
                )
            return SendResult(success=False, error="callback send failed after token refresh")
        except Exception as err:  # noqa: BLE001
            return SendResult(success=False, error=str(err))

    async def _ensure_callback_http_session(self) -> Any:
        if self._callback_http_session is None or self._callback_http_session.closed:
            self._callback_http_session = ClientSession()
        return self._callback_http_session

    async def _get_callback_access_token(self, app: Dict[str, Any]) -> str:
        name = str(app.get("name") or "")
        cached = self._callback_access_tokens.get(name)
        now = time.time()
        if cached and cached.get("expires_at", 0) > now + 60:
            return str(cached["token"])
        if not app.get("corp_secret"):
            raise RuntimeError("corp_secret is required for callback proactive send")
        session = await self._ensure_callback_http_session()
        async with session.get(
            "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
            params={
                "corpid": app.get("corp_id"),
                "corpsecret": app.get("corp_secret"),
            },
        ) as resp:
            data = await resp.json(content_type=None)
        if data.get("errcode") != 0:
            raise RuntimeError(f"WeCom token refresh failed: {data}")
        token = str(data["access_token"])
        expires_in = int(data.get("expires_in", ACCESS_TOKEN_TTL_SECONDS))
        self._callback_access_tokens[name] = {
            "token": token,
            "expires_at": time.time() + expires_in,
        }
        return token

    # ── Message parsing ─────────────────────────────────────────────────────

    def _parse_message_content(
        self, data: Dict[str, Any]
    ) -> Tuple[str, List[Dict[str, str]]]:
        """Parse message body into text and image references.

        Returns: (text, [{"url": ..., "aes_key": ..., "filename": ...}])

        Aligned with OpenClaw: src/message-parser.ts:parseMessageContent
        Delegates the heavy lifting to ``message_parser.parse_message_content``
        and adapts the rich result back to the tuple form expected by tests.
        """
        parsed = parse_message_content(data)
        images: List[Dict[str, str]] = []
        for url in parsed.image_urls:
            images.append(
                {
                    "url": url,
                    "aes_key": parsed.image_aes_keys.get(url, ""),
                    "filename": "image.png",
                }
            )

        msgtype = data.get("msgtype", "text")
        text = parsed.text

        # Friendly stub texts for file/voice when there's no extracted text —
        # preserves the prior adapter contract used by tests / Hermes routing.
        if msgtype == "file" and not text:
            file_info = data.get("file") or {}
            text = f"[文件] {file_info.get('file_name', 'unknown')}"
        elif msgtype == "voice" and not text:
            text = "[语音消息]"

        return text.strip(), images

    @staticmethod
    def _is_group_chat(chat_id: str) -> bool:
        """Determine if a chat_id represents a group chat."""
        # WeCom group chat IDs typically don't start with user ID prefixes
        # and contain specific patterns. Use heuristic:
        # - If it looks like a userid (short alphanumeric), it's DM
        # - If it's a longer ID or has specific format, it's group
        if not chat_id:
            return False
        # Group IDs in WeCom typically have a specific format
        return len(chat_id) > 32 or "@" in chat_id

    # ── Native streaming ────────────────────────────────────────────────────

    def supports_native_streaming(
        self,
        chat_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Probed by ``GatewayStreamConsumer`` to gate native streaming.

        WeCom AI Bot supports ``msgtype: "stream"`` in both DMs and groups.
        Whether we can actually send depends on having a cached inbound
        ``req_id`` for the chat — that check happens inside
        :meth:`send_stream_frame` when the consumer asks us to push a frame.
        Returning ``True`` here just tells the consumer we *want* the
        native transport.
        """
        del chat_type, metadata
        return True

    async def send_stream_frame(
        self,
        text: str,
        *,
        finalize: bool = False,
        chat_id: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> bool:
        """Public entry-point for ``GatewayStreamConsumer``.

        Lifecycle (per turn):

          * **Seed call** (empty ``text``, ``finalize=False``): resolve the
            chat's last inbound req_id, allocate a :class:`StreamTurn`, send
            the official ``<think></think>`` placeholder via
            ``reply_stream(..., finish=False)``. This triggers the WeCom
            client's typing animation.
          * **Mid-stream calls**: feed cumulative text into the turn's
            :class:`BlockChunker`. When it produces a block, fire one frame.
            Otherwise arm a 250ms idle-flush timer so partial buffers still
            ship when the LLM pauses.
          * **Finalize call** (``finalize=True``): drain the chunker
            force-true, send a closing ``reply_stream(..., finish=True)``,
            and clean up the turn.

        Returns ``True`` when the frame landed (or was intentionally
        skipped because the chunker isn't ready yet); ``False`` when the
        stream is unavailable so the consumer can fall back to
        :meth:`send`.
        """
        chat = (chat_id or "").strip()
        if not chat:
            logger.warning("xwecom: send_stream_frame missing chat_id")
            return False

        if not self._client:
            logger.debug("xwecom: send_stream_frame called while disconnected")
            return False

        turn_id = kwargs.get("turn_id")

        # Chat-level stream expiry blocks NEW turn creation only.  An
        # already-active turn (looked up by turn_id) can keep going.
        if not turn_id and chat in self._stream_expired_chats:
            return False

        turn_key = self._stream_turn_key(chat, turn_id)
        turn = self._stream_turns.get(turn_key)

        # ── Resolve / create the turn ───────────────────────────────────
        if turn is None:
            if finalize:
                # No turn means nothing was opened — nothing to finalize.
                logger.debug(
                    "xwecom: cannot finalize non-existent turn for chat %s (turn_id=%s)",
                    chat, turn_id,
                )
                return False

            if chat in self._stream_expired_chats:
                logger.debug(
                    "xwecom: chat %s is stream-expired, refusing new turn", chat,
                )
                return False

            req_id, frame = self._resolve_stream_target(chat, reply_to)
            if not req_id or frame is None:
                logger.debug(
                    "xwecom: no cached req_id/frame for chat %s — cannot stream",
                    chat,
                )
                return False

            turn = StreamTurn(chat, req_id, frame)
            self._stream_turns[turn_key] = turn
            logger.debug(
                "xwecom: new stream turn %s for chat %s (req_id=%s, turn_id=%s)",
                turn.stream_id, chat, req_id, turn_id,
            )

        if turn.expired or turn.finalized:
            return False

        turn_text = self._turn_visible_text(turn, text)

        # ── Seed frame ──────────────────────────────────────────────────
        if not turn.seeded:
            ok = await self._send_stream_reply_frame(
                turn, THINKING_MESSAGE, finish=False, allow_skip=False
            )
            if not ok:
                self._cleanup_stream_turn(turn_key, turn)
                return False
            turn.seeded = True
            self._schedule_stream_rotation(turn, turn_key=turn_key, turn_id=turn_id)
            self._schedule_stream_keepalive(turn, turn_id=turn_id)
            # Consumer's explicit seed: empty text, not finalizing — done.
            if not turn_text and not finalize:
                return True

        # ── Finalize path ───────────────────────────────────────────────
        if finalize:
            self._cancel_idle_flush(turn)
            # Drain the chunker so the final frame carries the latest tail.
            if turn.chunker is not None:
                drained = turn.chunker.drain(turn_text)
                if drained is not None:
                    turn_text = drained

            final_text = turn_text or ""
            # WeCom silently drops a final frame whose content matches the
            # last intermediate frame.  Append a zero-width space to force
            # a content diff and make sure the bubble closes.
            if final_text and final_text == turn.last_sent_content:
                final_text = final_text + "​"

            ok = await self._send_stream_reply_frame(
                turn, final_text, finish=True, allow_skip=False
            )
            turn.finalized = True
            self._cleanup_stream_turn(turn_key, turn)
            # Final-frame ack timeout is non-fatal — WeCom usually already
            # rendered the content by the time we hit the timeout.  Treat
            # any return as success here.
            return True

        # ── Intermediate frame via the block chunker ────────────────────
        if turn.chunker is None:
            turn.chunker = BlockChunker()
        turn.pending_cumulative = turn_text

        if turn.frame_count >= MAX_INTERMEDIATE_FRAMES:
            # Frame cap reached — keep accumulating silently. The finalize
            # frame will carry the rest.
            return True

        if turn.chunker.should_emit(turn_text):
            self._cancel_idle_flush(turn)
            ok = await self._send_stream_reply_frame(turn, turn_text, finish=False)
            if not ok:
                return False
            turn.chunker.mark_emitted(turn_text)
            turn.frame_count += 1
            turn.last_sent_content = turn_text
            return True

        # Not ready to emit yet — arm idle flush so a quiet LLM still
        # makes progress on the wire.
        self._arm_idle_flush(turn, turn_id=turn_id)
        return True

    # ── Stream-turn helpers ─────────────────────────────────────────────────

    @staticmethod
    def _stream_turn_key(chat: str, turn_id: Optional[str]) -> str:
        return f"{chat}:{turn_id or '_default'}"

    def _resolve_stream_target(
        self, chat: str, reply_to: Optional[str]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Pick the (req_id, frame) tuple to use for a stream reply.

        Currently uses the most recent inbound message for the chat —
        ``reply_to`` is accepted for forward compatibility but not yet
        keyed because WeCom doesn't expose a stable per-message id we can
        index back to its frame.
        """
        del reply_to
        req_id = self._last_chat_req_ids.get(chat)
        frame = self._last_chat_frames.get(chat)
        return req_id, frame

    @staticmethod
    def _turn_visible_text(turn: StreamTurn, cumulative_text: str) -> str:
        """Return the content segment that belongs to the current stream id.

        Aligned with openclaw-plugin-wecom stream rotation: once a stream is
        rotated, the new stream should show only content generated after the
        rotation point, avoiding duplicate old text in the new bubble.
        """
        text = cumulative_text or ""
        if turn.base_cumulative_len <= 0:
            return text
        if len(text) <= turn.base_cumulative_len:
            return ""
        return text[turn.base_cumulative_len :]

    def _schedule_stream_keepalive(
        self,
        turn: StreamTurn,
        *,
        turn_id: Optional[str],
    ) -> None:
        self._cancel_keepalive(turn)
        if turn.finalized or turn.expired:
            return
        if self._stream_keepalive_interval_s <= 0:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        turn.keepalive_handle = loop.call_later(
            self._stream_keepalive_interval_s,
            self._on_keepalive_fire,
            turn,
            turn_id,
        )

    def _on_keepalive_fire(
        self,
        turn: StreamTurn,
        turn_id: Optional[str],
    ) -> None:
        turn.keepalive_handle = None
        if turn.finalized or turn.expired:
            return
        try:
            asyncio.ensure_future(self._send_keepalive(turn, turn_id))
        except RuntimeError:
            pass

    async def _send_keepalive(
        self,
        turn: StreamTurn,
        turn_id: Optional[str],
    ) -> None:
        if turn.finalized or turn.expired:
            return
        if turn.frame_count >= MAX_INTERMEDIATE_FRAMES:
            return
        content = turn.pending_cumulative or turn.last_wire_content or THINKING_MESSAGE
        ok = await self._send_stream_reply_frame(turn, content, finish=False)
        if ok:
            turn.frame_count += 1
            if content:
                turn.last_sent_content = content
                if turn.chunker is not None:
                    turn.chunker.mark_emitted(content)
        self._schedule_stream_keepalive(turn, turn_id=turn_id)

    def _schedule_stream_rotation(
        self,
        turn: StreamTurn,
        *,
        turn_key: str,
        turn_id: Optional[str],
    ) -> None:
        self._cancel_rotation(turn)
        if turn.finalized or turn.expired:
            return
        if self._stream_rotate_after_s <= 0:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        turn.rotation_handle = loop.call_later(
            self._stream_rotate_after_s,
            self._on_rotation_fire,
            turn_key,
            turn,
            turn_id,
        )

    def _on_rotation_fire(
        self,
        turn_key: str,
        turn: StreamTurn,
        turn_id: Optional[str],
    ) -> None:
        turn.rotation_handle = None
        if turn.finalized or turn.expired:
            return
        try:
            asyncio.ensure_future(self._rotate_stream(turn_key, turn, turn_id))
        except RuntimeError:
            pass

    async def _rotate_stream(
        self,
        turn_key: str,
        turn: StreamTurn,
        turn_id: Optional[str],
    ) -> None:
        """Finish the current stream and continue with a fresh stream id.

        Aligned with openclaw-plugin-wecom ws-monitor.js:rotateStream.
        """
        if turn.finalized or turn.expired:
            return

        self._cancel_idle_flush(turn)
        self._cancel_keepalive(turn)

        old_stream_id = turn.stream_id
        current_content = turn.pending_cumulative or turn.last_sent_content
        finish_content = current_content or "处理中..."
        await self._send_stream_reply_frame(
            turn,
            finish_content,
            finish=True,
            allow_skip=False,
            process_template_cards=False,
        )

        turn.stream_id = f"stream_{uuid.uuid4().hex[:12]}"
        turn.chunker = BlockChunker()
        turn.frame_count = 0
        turn.last_sent_content = ""
        turn.last_wire_content = THINKING_MESSAGE
        turn.base_cumulative_len += len(current_content)
        turn.pending_cumulative = ""
        turn.start_time = time.monotonic()
        logger.info(
            "xwecom: rotated stream for chat %s old=%s new=%s",
            turn.chat_id,
            old_stream_id,
            turn.stream_id,
        )

        ok = await self._send_stream_reply_frame(
            turn,
            THINKING_MESSAGE,
            finish=False,
            allow_skip=False,
        )
        if not ok:
            turn.expired = True
            self._stream_expired_chats.add(turn.chat_id)
            self._cleanup_stream_turn(turn_key, turn)
            return
        self._schedule_stream_rotation(turn, turn_key=turn_key, turn_id=turn_id)
        self._schedule_stream_keepalive(turn, turn_id=turn_id)

    async def _send_stream_reply_frame(
        self,
        turn: StreamTurn,
        content: str,
        *,
        finish: bool,
        allow_skip: bool = True,
        process_template_cards: bool = True,
    ) -> bool:
        """Wire-level frame send. Truncates to MAX_STREAM_CONTENT_LENGTH,
        translates errcode 846608 into an expired turn, and treats ack
        timeouts on the final frame as non-fatal.
        """
        if not self._client:
            return False

        # Truncate by UTF-8 byte length — WeCom rejects frames over 20KB.
        outbound_content = content or ""
        if finish and process_template_cards:
            try:
                card_result = await process_template_cards_if_needed(
                    self._client,
                    turn.frame,
                    accumulated_text=outbound_content,
                    account_id=self._account_id,
                    cache=self._template_card_cache,
                )
                if card_result is not None:
                    outbound_content = card_result.remaining_text
            except Exception as err:  # noqa: BLE001
                logger.warning("xwecom: final template card send failed: %s", err)
            outbound_content = await self._process_reply_media_directives(
                turn.chat_id,
                outbound_content,
            )
        else:
            outbound_content = mask_template_card_blocks(outbound_content)

        outbound_was_truncated = (
            len(outbound_content.encode("utf-8")) > MAX_STREAM_CONTENT_LENGTH
        )
        truncated = self._truncate_to_bytes(outbound_content, MAX_STREAM_CONTENT_LENGTH)
        if outbound_was_truncated:
            logger.warning(
                "xwecom: stream content truncated for stream_id=%s", turn.stream_id,
            )

        acquired = True
        if allow_skip:
            acquired = await self._stream_gate.try_acquire(turn.stream_id, finish=finish)
            if not acquired:
                logger.debug(
                    "xwecom: stream %s skipped intermediate frame; ack pending",
                    turn.stream_id,
                )
                return True

        try:
            resp = await self._client.reply_stream(
                turn.frame,
                turn.stream_id,
                truncated,
                finish,
            )
        except asyncio.TimeoutError:
            if finish:
                # WeCom usually already rendered the content; the ack just
                # didn't arrive in time.  Treat as success.
                logger.warning(
                    "xwecom: final-frame ack timeout for stream %s (treating as ok)",
                    turn.stream_id,
                )
                return True
            logger.warning(
                "xwecom: intermediate-frame ack timeout for stream %s",
                turn.stream_id,
            )
            return False
        except RuntimeError as exc:
            # SDK raises RuntimeError when the WS isn't connected.
            logger.warning("xwecom: stream send failed (%s)", exc)
            return False
        except Exception as exc:
            logger.warning("xwecom: stream send raised: %s", exc)
            return False
        finally:
            if acquired and allow_skip:
                await self._stream_gate.release(turn.stream_id)

        errcode = self._extract_errcode(resp)
        if errcode == STREAM_EXPIRED_ERRCODE:
            logger.info(
                "xwecom: stream %s expired (errcode=%s) — falling back to send()",
                turn.stream_id, errcode,
            )
            turn.expired = True
            self._stream_expired_chats.add(turn.chat_id)
            return False
        if errcode and errcode != 0:
            errmsg = ""
            if isinstance(resp, dict):
                errmsg = resp.get("errmsg") or ""
            logger.warning(
                "xwecom: stream %s errcode=%s (%s)",
                turn.stream_id, errcode, errmsg,
            )
            return False
        logger.info(
            "xwecom: stream %s frame sent (finish=%s, len=%d)",
            turn.stream_id, finish, len(truncated),
        )
        if truncated:
            turn.last_wire_content = truncated
        if finish:
            await self._stream_gate.clear(turn.stream_id)
            if (
                process_template_cards
                and outbound_was_truncated
                and not turn.full_content_fallback_sent
            ):
                await self._send_full_content_fallback(turn, outbound_content)
        return True

    @staticmethod
    def _extract_errcode(resp: Any) -> Optional[int]:
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

    @staticmethod
    def _truncate_to_bytes(text: str, max_bytes: int) -> str:
        """Keep the TAIL of the text within max_bytes (UTF-8).

        Aligned with openclaw's truncateUtf8Bytes: in cumulative streaming,
        the user should see the latest content, not the beginning.
        """
        encoded = text.encode("utf-8")
        if len(encoded) <= max_bytes:
            return text
        # Take the last max_bytes, then skip any leading continuation bytes
        # (0b10xxxxxx) to land on a valid UTF-8 char boundary.
        cut = encoded[len(encoded) - max_bytes:]
        i = 0
        while i < len(cut) and (cut[i] & 0xC0) == 0x80:
            i += 1
        return cut[i:].decode("utf-8", errors="ignore")

    @staticmethod
    def _split_text_by_byte_limit(text: str, max_bytes: int) -> List[str]:
        """Split text into UTF-8 byte-limited chunks.

        Aligned with openclaw-plugin-wecom utils.ts:splitTextByByteLimit.
        """
        if not text:
            return []
        if len(text.encode("utf-8")) <= max_bytes:
            return [text]

        chunks: List[str] = []
        remaining = text
        while remaining:
            if len(remaining.encode("utf-8")) <= max_bytes:
                chunks.append(remaining)
                break

            lo = 0
            hi = len(remaining)
            while lo < hi:
                mid = (lo + hi + 1) // 2
                if len(remaining[:mid].encode("utf-8")) <= max_bytes:
                    lo = mid
                else:
                    hi = mid - 1

            split_at = lo
            search_start = max(0, int(split_at * 0.8))
            last_newline = remaining.rfind("\n", 0, split_at)
            if last_newline >= search_start:
                split_at = last_newline + 1
            if split_at <= 0:
                split_at = max(1, lo)

            chunks.append(remaining[:split_at])
            remaining = remaining[split_at:]
        return chunks

    @staticmethod
    def _split_reply_media_from_text(text: str) -> Tuple[str, List[str]]:
        """Extract MEDIA:/FILE: directive lines from reply text.

        Aligned with openclaw-plugin-wecom ws-monitor.js:splitReplyMediaFromText.
        """
        if not text:
            return "", []

        media_urls: List[str] = []
        kept_lines: List[str] = []
        for line in text.split("\n"):
            match = REPLY_MEDIA_DIRECTIVE_RE.match(line)
            if not match:
                kept_lines.append(line)
                continue
            media_url = match.group(1).strip()
            if len(media_url) >= 2 and media_url[0] == "`" and media_url[-1] == "`":
                media_url = media_url[1:-1].strip()
            if media_url:
                media_urls.append(media_url)

        visible = re.sub(r"\n{3,}", "\n\n", "\n".join(kept_lines)).strip()
        return visible, media_urls

    async def _process_reply_media_directives(
        self,
        chat_id: str,
        text: str,
    ) -> str:
        """Upload/send reply media directives and return visible text only."""
        if not self._client:
            return text
        visible, media_urls = self._split_reply_media_from_text(text)
        if not media_urls:
            return visible

        failure_notes: List[str] = []
        for media_url in media_urls:
            result = await upload_and_send_media(
                self._client,
                media_url,
                chat_id,
            )
            if result.ok:
                continue
            reason = (
                getattr(result, "reject_reason", None)
                or getattr(result, "error", None)
                or "unknown error"
            )
            failure_notes.append(f"文件发送失败：{media_url}\n{reason}")

        if failure_notes:
            suffix = "\n\n".join(failure_notes)
            return f"{visible}\n\n{suffix}".strip() if visible else suffix
        return visible

    async def _send_full_content_fallback(
        self,
        turn: StreamTurn,
        full_content: str,
    ) -> None:
        """Proactively send full text when final stream content was truncated.

        Aligned with OpenClaw's dmContent/full-text fallback intent, adapted to
        Hermes' AI Bot WS path by using proactive send_message on the same chat.
        """
        if not self._client:
            return
        chat_id = (turn.frame.get("body") or {}).get("chatid") or turn.chat_id
        if not chat_id:
            return
        turn.full_content_fallback_sent = True
        chunks = self._split_text_by_byte_limit(full_content, MAX_MESSAGE_LENGTH)
        if not chunks:
            return
        header = (
            "上方流式气泡因企业微信单帧长度限制只显示了最新部分，"
            "以下为完整回复：\n\n"
        )
        for idx, chunk in enumerate(chunks):
            content = f"{header}{chunk}" if idx == 0 else chunk
            try:
                await self._client.send_message(
                    chat_id,
                    {"msgtype": "markdown", "markdown": {"content": content}},
                )
            except Exception as err:  # noqa: BLE001
                logger.warning(
                    "xwecom: full-content fallback send failed for stream %s: %s",
                    turn.stream_id,
                    err,
                )
                return

    def _cleanup_stream_turn(self, turn_key: str, turn: StreamTurn) -> None:
        self._cancel_idle_flush(turn)
        self._cancel_keepalive(turn)
        self._cancel_rotation(turn)
        self._stream_turns.pop(turn_key, None)

    # ── Idle flush ──────────────────────────────────────────────────────────

    def _cancel_idle_flush(self, turn: StreamTurn) -> None:
        handle = turn.idle_flush_handle
        if handle is not None:
            try:
                handle.cancel()
            except Exception:
                pass
            turn.idle_flush_handle = None

    def _cancel_keepalive(self, turn: StreamTurn) -> None:
        handle = turn.keepalive_handle
        if handle is not None:
            try:
                handle.cancel()
            except Exception:
                pass
            turn.keepalive_handle = None

    def _cancel_rotation(self, turn: StreamTurn) -> None:
        handle = turn.rotation_handle
        if handle is not None:
            try:
                handle.cancel()
            except Exception:
                pass
            turn.rotation_handle = None

    def _arm_idle_flush(
        self,
        turn: StreamTurn,
        *,
        turn_id: Optional[str],
    ) -> None:
        """Arm (or reset) the 250ms idle-flush timer.

        When the LLM pauses, the chunker's min_chars gate would hold a
        half-sentence forever. The official plugin's
        ``blockStreamingCoalesce.idleMs = 250`` solves this by force-draining
        the buffer after 250ms of silence.

        Each call cancels any pending timer and re-arms, so the flush only
        fires after a genuine 250ms of silence — aligned with openclaw's
        debounce semantics.
        """
        self._cancel_idle_flush(turn)
        if turn.finalized or turn.expired:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        turn.idle_flush_handle = loop.call_later(
            BLOCK_STREAM_IDLE_FLUSH,
            self._on_idle_flush_fire,
            turn,
            turn_id,
        )

    def _on_idle_flush_fire(
        self,
        turn: StreamTurn,
        turn_id: Optional[str],
    ) -> None:
        turn.idle_flush_handle = None
        if turn.finalized or turn.expired:
            return
        if turn.chunker is None:
            return
        if turn.frame_count >= MAX_INTERMEDIATE_FRAMES:
            return
        try:
            asyncio.ensure_future(self._idle_flush_send(turn, turn_id))
        except RuntimeError:
            pass

    async def _idle_flush_send(
        self,
        turn: StreamTurn,
        turn_id: Optional[str],
    ) -> None:
        if turn.finalized or turn.expired:
            return
        chunker = turn.chunker
        if chunker is None:
            return
        if turn.frame_count >= MAX_INTERMEDIATE_FRAMES:
            return
        # Force-emit whatever cumulative the consumer last pushed.  The
        # chunker is stateless on cumulative text, so we replay the
        # latest snapshot the turn observed and mark it emitted ourselves.
        cumulative = turn.pending_cumulative
        if not cumulative or len(cumulative) <= chunker.emitted_length:
            return
        ok = await self._send_stream_reply_frame(turn, cumulative, finish=False)
        if not ok:
            return
        chunker.mark_emitted(cumulative)
        turn.frame_count += 1
        turn.last_sent_content = cumulative
        del turn_id

    # ── Media sending ───────────────────────────────────────────────────────

    async def send_media(
        self,
        chat_id: str,
        media_bytes: bytes,
        filename: str,
        mime_type: str = "",
    ) -> SendResult:
        """Upload and send a media file."""
        if not self._client:
            return SendResult(success=False, error="Not connected")

        # Detect type and check size
        media_type = detect_media_type(mime_type, filename)
        ok, final_type, error = check_file_size(media_bytes, media_type, filename)
        if not ok:
            return SendResult(success=False, error=error)

        # Upload
        media_id = await upload_media_chunked(self._client, media_bytes, filename, final_type)
        if not media_id:
            return SendResult(success=False, error="Media upload failed")

        # Send
        body = {
            "msgtype": final_type,
            final_type: {"media_id": media_id},
        }
        try:
            await self._client.send_message(chat_id, body)
            return SendResult(success=True, message_id=f"media_{media_id}")
        except Exception as e:
            return SendResult(success=False, error=f"Media send failed: {e}")


# ── Plugin registration ─────────────────────────────────────────────────────


def check_requirements() -> bool:
    """Check if runtime dependencies are available."""
    try:
        import websockets  # noqa: F401
        import aiohttp  # noqa: F401
        import cryptography  # noqa: F401

        return True
    except ImportError:
        return False


def validate_config(config: Any) -> bool:
    """Validate that minimum config is present."""
    extra = getattr(config, "extra", {}) or {}
    bot_id = os.getenv("XWECOM_BOT_ID") or extra.get("bot_id")
    secret = os.getenv("XWECOM_SECRET") or extra.get("secret")
    if bot_id and secret:
        return True
    if _standalone_agent_app(extra) is not None:
        return True
    callback_enabled = XWeComAdapter._truthy(
        extra.get("callback_enabled") or os.getenv("XWECOM_CALLBACK_ENABLED")
    )
    if not callback_enabled:
        return False
    return bool(
        (extra.get("corp_id") or os.getenv("XWECOM_CORP_ID"))
        and (
            extra.get("callback_token")
            or extra.get("token")
            or os.getenv("XWECOM_CALLBACK_TOKEN")
        )
        and (extra.get("encoding_aes_key") or os.getenv("XWECOM_ENCODING_AES_KEY"))
    )


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Seed PlatformConfig.extra from env vars."""
    bot_id = os.getenv("XWECOM_BOT_ID", "").strip()
    secret = os.getenv("XWECOM_SECRET", "").strip()
    callback_enabled = XWeComAdapter._truthy(os.getenv("XWECOM_CALLBACK_ENABLED"))
    agent_env = {
        "corp_id": os.getenv("XWECOM_CORP_ID", "").strip(),
        "corp_secret": os.getenv("XWECOM_CORP_SECRET", "").strip(),
        "agent_id": os.getenv("XWECOM_AGENT_ID", "").strip(),
    }
    has_agent_outbound = all(agent_env.values())
    if not (bot_id and secret) and not callback_enabled and not has_agent_outbound:
        return None
    seed: Dict[str, Any] = {}
    if bot_id and secret:
        seed.update({"bot_id": bot_id, "secret": secret})
    ws_url = os.getenv("XWECOM_WEBSOCKET_URL")
    if ws_url:
        seed["websocket_url"] = ws_url
    if has_agent_outbound:
        seed.update(agent_env)
    if callback_enabled:
        seed["callback_enabled"] = True
        env_map = {
            "callback_host": "XWECOM_CALLBACK_HOST",
            "callback_port": "XWECOM_CALLBACK_PORT",
            "callback_path": "XWECOM_CALLBACK_PATH",
            "corp_id": "XWECOM_CORP_ID",
            "corp_secret": "XWECOM_CORP_SECRET",
            "agent_id": "XWECOM_AGENT_ID",
            "callback_token": "XWECOM_CALLBACK_TOKEN",
            "encoding_aes_key": "XWECOM_ENCODING_AES_KEY",
        }
        for key, env_name in env_map.items():
            value = os.getenv(env_name)
            if value:
                seed[key] = value
    home = os.getenv("XWECOM_HOME_CHANNEL")
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("XWECOM_HOME_CHANNEL_NAME", "Home"),
        }
    return seed


def _standalone_agent_app(extra: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the first app configured for Agent HTTP outbound delivery."""
    for app in XWeComAdapter._normalize_callback_apps(extra):
        if app.get("corp_id") and app.get("corp_secret") and app.get("agent_id"):
            return app
    return None


def _resolve_wecom_target(raw: str) -> Optional[Dict[str, str]]:
    """Resolve OpenClaw-style WeCom outbound targets."""
    clean = str(raw or "").strip()
    if not clean:
        return None
    clean = re.sub(
        r"^(wecom-agent|xwecom|wecom|wechatwork|wework|qywx):",
        "",
        clean,
        flags=re.IGNORECASE,
    ).strip()

    prefix_map = {
        "party": "toparty",
        "dept": "toparty",
        "tag": "totag",
        "group": "chatid",
        "chat": "chatid",
        "appchat": "chatid",
        "user": "touser",
    }
    for prefix, field in prefix_map.items():
        marker = f"{prefix}:"
        if clean.lower().startswith(marker):
            value = clean[len(marker):].strip()
            return {field: value} if value else None

    if re.match(r"^(wr|wc)", clean, re.IGNORECASE):
        return {"chatid": clean}
    if clean.isdigit():
        return {"toparty": clean}
    return {"touser": clean}


async def _standalone_send_agent(
    app: Dict[str, Any],
    chat_id: str,
    message: str,
) -> Dict[str, Any]:
    """Send a standalone message through WeCom Agent HTTP API."""
    if ClientSession is None:
        return {"error": "aiohttp is required for Agent HTTP send"}
    target = _resolve_wecom_target(chat_id)
    if target is None:
        return {"error": f"Cannot resolve xwecom target from {chat_id!r}"}

    try:
        async with ClientSession() as session:
            async with session.get(
                "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                params={
                    "corpid": app.get("corp_id"),
                    "corpsecret": app.get("corp_secret"),
                },
            ) as resp:
                token_data = await resp.json(content_type=None)
            if token_data.get("errcode") != 0:
                return {"error": f"Agent token refresh failed: {token_data}"}
            token = str(token_data["access_token"])

            if target.get("chatid"):
                url = "https://qyapi.weixin.qq.com/cgi-bin/appchat/send"
                payload = {
                    "chatid": target["chatid"],
                    "msgtype": "text",
                    "text": {"content": message[:2048]},
                }
            else:
                url = "https://qyapi.weixin.qq.com/cgi-bin/message/send"
                payload = {
                    key: value
                    for key, value in {
                        "touser": target.get("touser"),
                        "toparty": target.get("toparty"),
                        "totag": target.get("totag"),
                    }.items()
                    if value
                }
                payload.update(
                    {
                        "msgtype": "text",
                        "agentid": int(str(app.get("agent_id") or 0)),
                        "text": {"content": message[:2048]},
                        "safe": 0,
                    }
                )

            async with session.post(
                url,
                params={"access_token": token},
                json=payload,
            ) as resp:
                send_data = await resp.json(content_type=None)
            if send_data.get("errcode") != 0:
                return {"error": f"Agent send failed: {send_data}"}
            message_id = str(send_data.get("msgid") or f"agent_{int(time.time())}")
            return {
                "success": True,
                "message_id": message_id,
                "raw_response": send_data,
                "transport": "agent_http",
            }
    except Exception as err:  # noqa: BLE001
        return {"error": f"Agent send failed: {err}"}


async def _standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Standalone sender for out-of-process cron delivery."""
    extra = getattr(pconfig, "extra", {}) or {}
    agent_app = _standalone_agent_app(extra)
    if agent_app is not None:
        return await _standalone_send_agent(agent_app, chat_id, message)

    bot_id = extra.get("bot_id") or os.getenv("XWECOM_BOT_ID", "")
    secret = extra.get("secret") or os.getenv("XWECOM_SECRET", "")
    ws_url = extra.get("websocket_url") or os.getenv(
        "XWECOM_WEBSOCKET_URL", "wss://openws.work.weixin.qq.com"
    )

    if not (bot_id and secret):
        return {
            "error": (
                "XWECOM_CORP_ID/XWECOM_CORP_SECRET/XWECOM_AGENT_ID or "
                "XWECOM_BOT_ID/XWECOM_SECRET required"
            )
        }

    lock_acquired = False
    if acquire_scoped_lock is not None:
        lock_result = acquire_scoped_lock("xwecom", bot_id)
        lock_acquired, existing = XWeComAdapter._interpret_scoped_lock_result(
            lock_result
        )
        if not lock_acquired:
            owner_pid = existing.get("pid") if existing else None
            owner_suffix = f" (PID {owner_pid})" if owner_pid else ""
            return {
                "error": (
                    "Standalone xwecom send skipped: token already in use"
                    f"{owner_suffix}. Stop the other gateway first."
                )
            }

    client: Optional[WSClient] = None
    try:
        opts = WSClientOptions(
            bot_id=bot_id,
            secret=secret,
            ws_url=ws_url,
            heartbeat_interval=30000,
            max_reconnect_attempts=3,
        )
        client = WSClient(opts)
        await client.connect()
        # Wait briefly for authentication
        await asyncio.sleep(2)

        body = {"msgtype": "markdown", "markdown": {"content": message}}
        await client.send_message(chat_id, body)

        return {"success": True, "message_id": f"cron_{int(time.time())}"}
    except Exception as e:
        return {"error": f"Standalone send failed: {e}"}
    finally:
        if client is not None:
            try:
                client.disconnect()
            except Exception:
                pass
        if lock_acquired and release_scoped_lock is not None:
            release_scoped_lock("xwecom", bot_id)


def register(ctx: Any) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="xwecom",
        label="XWeCom (企业微信 · Official SDK)",
        adapter_factory=lambda cfg: XWeComAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        env_enablement_fn=_env_enablement,
        required_env=["XWECOM_BOT_ID", "XWECOM_SECRET"],
        install_hint="pip install websockets aiohttp cryptography pyee",
        cron_deliver_env_var="XWECOM_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="XWECOM_ALLOWED_USERS",
        allow_all_env="XWECOM_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=(
            "You are on WeCom (企业微信 / Enterprise WeChat). Markdown formatting "
            "is supported. You CAN send media files natively — to deliver a file "
            "to the user, include MEDIA:/absolute/path/to/file in your response. "
            "Images (.jpg, .png, .webp) are sent as photos (up to 10 MB), other "
            "files (.pdf, .docx, .xlsx) arrive as downloadable documents (up to 20 MB), "
            "and videos (.mp4) play inline. Voice messages must be in AMR format."
        ),
        emoji="💼",
    )
