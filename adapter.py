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
import time
import uuid
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

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
    )
    from .media import (
        check_file_size,
        detect_media_type,
        download_and_decrypt,
        upload_media_chunked,
    )
    from .message_parser import parse_message_content, parse_message_simple
    from .message_sender import THINKING_MESSAGE
    from .monitor import (
        BufferedBlockDispatcher,
        SessionRecorder,
        SessionRecord,
        handle_disconnected_event,
        handle_enter_chat_event,
    )
    from .policy import check_dm_policy, check_group_policy
    from .state_manager import get_state_manager
    from .stream import BlockChunker, BlockStreamManager, StreamExpiredError
    from .template_card import TemplateCardCache
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
    )
    from media import (  # type: ignore[no-redef]
        check_file_size,
        detect_media_type,
        download_and_decrypt,
        upload_media_chunked,
    )
    from message_parser import parse_message_content, parse_message_simple  # type: ignore[no-redef]
    from message_sender import THINKING_MESSAGE  # type: ignore[no-redef]
    from monitor import (  # type: ignore[no-redef]
        BufferedBlockDispatcher,
        SessionRecorder,
        SessionRecord,
        handle_disconnected_event,
        handle_enter_chat_event,
    )
    from policy import check_dm_policy, check_group_policy  # type: ignore[no-redef]
    from state_manager import get_state_manager  # type: ignore[no-redef]
    from stream import BlockChunker, BlockStreamManager, StreamExpiredError  # type: ignore[no-redef]
    from template_card import TemplateCardCache  # type: ignore[no-redef]

logger = logging.getLogger(__name__)


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
        "idle_flush_handle",
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
        self.idle_flush_handle: Optional[asyncio.TimerHandle] = None
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

        # Internal state
        self._client: Optional[WSClient] = None
        self._stream_mgr = BlockStreamManager()
        self._dedup = MessageDeduplicator()
        self._lock_acquired = False

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

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Establish WebSocket connection to WeCom."""
        if not self._bot_id or not self._secret:
            logger.error("xwecom: bot_id and secret are required")
            return False

        # Token lock — prevent two profiles from using same credential
        if acquire_scoped_lock is not None:
            if not acquire_scoped_lock("xwecom", self._bot_id):
                logger.error("xwecom: Token already in use by another profile")
                return False
            self._lock_acquired = True

        opts = WSClientOptions(
            bot_id=self._bot_id,
            secret=self._secret,
            ws_url=self._ws_url,
            heartbeat_interval=30000,
            max_reconnect_attempts=-1,  # Infinite reconnection
            reply_ack_timeout=5.0,
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
        self._client.on("error", lambda e: logger.error(f"xwecom: error - {e}", exc_info=isinstance(e, BaseException)))

        try:
            await self._client.connect()
            self._mark_connected()
            logger.info("xwecom: adapter connected and ready")
            return True
        except Exception as e:
            logger.error(f"xwecom: connection failed - {e}")
            if self._lock_acquired and release_scoped_lock is not None:
                release_scoped_lock("xwecom", self._bot_id)
                self._lock_acquired = False
            return False

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

        if self._client:
            try:
                # SDK's disconnect() is synchronous — don't await it.
                self._client.disconnect()
            except Exception as e:
                logger.warning(f"xwecom: disconnect error - {e}")
            self._client = None

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
        if not self._client:
            return SendResult(success=False, error="Not connected")

        body = {"msgtype": "markdown", "markdown": {"content": content}}
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

        await self.handle_message(event)

    async def _on_event(self, frame: Dict[str, Any]) -> None:
        """Handle WeCom events (enter_chat, etc.)."""
        body = frame.get("body") or {}
        event_obj = body.get("event") or {}
        event_type = event_obj.get("eventtype", "unknown")
        logger.debug(f"xwecom: received event: {event_type}")

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

        # ── Seed frame ──────────────────────────────────────────────────
        if not turn.seeded:
            ok = await self._send_stream_reply_frame(turn, THINKING_MESSAGE, finish=False)
            if not ok:
                self._cleanup_stream_turn(turn_key, turn)
                return False
            turn.seeded = True
            # Consumer's explicit seed: empty text, not finalizing — done.
            if not text and not finalize:
                return True

        # ── Finalize path ───────────────────────────────────────────────
        if finalize:
            self._cancel_idle_flush(turn)
            # Drain the chunker so the final frame carries the latest tail.
            if turn.chunker is not None:
                drained = turn.chunker.drain(text)
                if drained is not None:
                    text = drained

            final_text = text or ""
            # WeCom silently drops a final frame whose content matches the
            # last intermediate frame.  Append a zero-width space to force
            # a content diff and make sure the bubble closes.
            if final_text and final_text == turn.last_sent_content:
                final_text = final_text + "​"

            ok = await self._send_stream_reply_frame(turn, final_text, finish=True)
            turn.finalized = True
            self._cleanup_stream_turn(turn_key, turn)
            # Final-frame ack timeout is non-fatal — WeCom usually already
            # rendered the content by the time we hit the timeout.  Treat
            # any return as success here.
            return True

        # ── Intermediate frame via the block chunker ────────────────────
        if turn.chunker is None:
            turn.chunker = BlockChunker()
        turn.pending_cumulative = text

        if turn.frame_count >= MAX_INTERMEDIATE_FRAMES:
            # Frame cap reached — keep accumulating silently. The finalize
            # frame will carry the rest.
            return True

        if turn.chunker.should_emit(text):
            self._cancel_idle_flush(turn)
            ok = await self._send_stream_reply_frame(turn, text, finish=False)
            if not ok:
                return False
            turn.chunker.mark_emitted(text)
            turn.frame_count += 1
            turn.last_sent_content = text
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

    async def _send_stream_reply_frame(
        self,
        turn: StreamTurn,
        content: str,
        *,
        finish: bool,
    ) -> bool:
        """Wire-level frame send. Truncates to MAX_STREAM_CONTENT_LENGTH,
        translates errcode 846608 into an expired turn, and treats ack
        timeouts on the final frame as non-fatal.
        """
        if not self._client:
            return False

        # Truncate by UTF-8 byte length — WeCom rejects frames over 20KB.
        truncated = self._truncate_to_bytes(content or "", MAX_STREAM_CONTENT_LENGTH)
        if len(truncated) != len(content or ""):
            logger.warning(
                "xwecom: stream content truncated for stream_id=%s", turn.stream_id,
            )

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

    def _cleanup_stream_turn(self, turn_key: str, turn: StreamTurn) -> None:
        self._cancel_idle_flush(turn)
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
    return bool(bot_id and secret)


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Seed PlatformConfig.extra from env vars."""
    bot_id = os.getenv("XWECOM_BOT_ID", "").strip()
    secret = os.getenv("XWECOM_SECRET", "").strip()
    if not (bot_id and secret):
        return None
    seed: Dict[str, Any] = {"bot_id": bot_id, "secret": secret}
    ws_url = os.getenv("XWECOM_WEBSOCKET_URL")
    if ws_url:
        seed["websocket_url"] = ws_url
    home = os.getenv("XWECOM_HOME_CHANNEL")
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("XWECOM_HOME_CHANNEL_NAME", "Home"),
        }
    return seed


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
    bot_id = extra.get("bot_id") or os.getenv("XWECOM_BOT_ID", "")
    secret = extra.get("secret") or os.getenv("XWECOM_SECRET", "")
    ws_url = extra.get("websocket_url") or os.getenv(
        "XWECOM_WEBSOCKET_URL", "wss://openws.work.weixin.qq.com"
    )

    if not (bot_id and secret):
        return {"error": "XWECOM_BOT_ID and XWECOM_SECRET required"}

    opts = WSClientOptions(
        bot_id=bot_id,
        secret=secret,
        ws_url=ws_url,
        heartbeat_interval=30000,
        max_reconnect_attempts=3,
    )
    client = WSClient(opts)

    try:
        await client.connect()
        # Wait briefly for authentication
        await asyncio.sleep(2)

        body = {"msgtype": "markdown", "markdown": {"content": message}}
        await client.send_message(chat_id, body)

        client.disconnect()
        return {"success": True, "message_id": f"cron_{int(time.time())}"}
    except Exception as e:
        try:
            client.disconnect()
        except Exception:
            pass
        return {"error": f"Standalone send failed: {e}"}


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
