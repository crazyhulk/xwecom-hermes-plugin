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
        DEDUP_MAX_SIZE,
        DEDUP_TTL_SECONDS,
        MAX_MESSAGE_LENGTH,
        STREAM_EXPIRED_ERRCODE,
    )
    from .media import (
        check_file_size,
        detect_media_type,
        download_and_decrypt,
        upload_media_chunked,
    )
    from .policy import check_dm_policy, check_group_policy
    from .stream import BlockChunker, BlockStreamManager, StreamExpiredError
except ImportError:
    from sdk import WSClient, WSClientOptions  # type: ignore[no-redef]
    from constants import (  # type: ignore[no-redef]
        DEDUP_MAX_SIZE,
        DEDUP_TTL_SECONDS,
        MAX_MESSAGE_LENGTH,
        STREAM_EXPIRED_ERRCODE,
    )
    from media import (  # type: ignore[no-redef]
        check_file_size,
        detect_media_type,
        download_and_decrypt,
        upload_media_chunked,
    )
    from policy import check_dm_policy, check_group_policy  # type: ignore[no-redef]
    from stream import BlockChunker, BlockStreamManager, StreamExpiredError  # type: ignore[no-redef]

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


class XWeComAdapter(BasePlatformAdapter):
    """WeCom adapter using official Python SDK."""

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

    async def connect(self) -> bool:
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
        self._client.on("error", lambda e: logger.error(f"xwecom: error - {e}"))

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
        if self._client:
            try:
                await self._client.disconnect()
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
        """Handle inbound user messages from WeCom."""
        data = frame.get("data", {})
        msg_id = data.get("msgid", "") or frame.get("req_id", "")

        # Dedup
        if msg_id and self._dedup.is_duplicate(msg_id):
            logger.debug(f"xwecom: duplicate message {msg_id}, skipping")
            return

        # Extract sender info
        sender = data.get("sender", {})
        user_id = sender.get("userid", "")
        user_name = sender.get("name", user_id)
        chat_id = data.get("chatid", "")

        # Determine chat type
        is_group = self._is_group_chat(chat_id)

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
            # For DM, chat_id might be the user_id itself
            effective_chat = chat_id or user_id
            if not check_dm_policy(self._dm_policy, self._allow_from, user_id):
                logger.debug(f"xwecom: DM rejected by policy: {user_id}")
                return
            chat_id = effective_chat

        # Parse message content
        text, images = self._parse_message_content(data)

        # Download media attachments
        cached_images: List[str] = []
        for img_info in images:
            img_data = await download_and_decrypt(
                self._client, img_info.get("url", ""), img_info.get("aes_key")
            )
            if img_data and cache_image_from_bytes:
                try:
                    path = cache_image_from_bytes(
                        img_data, img_info.get("filename", "image.png")
                    )
                    if path:
                        cached_images.append(str(path))
                except Exception as e:
                    logger.warning(f"xwecom: failed to cache image: {e}")

        # Build MessageEvent
        source = self.build_source(
            chat_id=chat_id,
            chat_name=data.get("chat_name", chat_id),
            chat_type="group" if is_group else "dm",
            user_id=user_id,
            user_name=user_name,
        )

        msg_type = MessageType.TEXT
        if cached_images and not text:
            msg_type = MessageType.IMAGE

        event = MessageEvent(
            text=text or "",
            message_type=msg_type,
            source=source,
            message_id=msg_id,
            images=cached_images if cached_images else None,
        )

        # Store frame ref for potential stream reply
        if not hasattr(event, "metadata"):
            event.metadata = {}
        event.metadata["_xwecom_frame"] = frame  # type: ignore[attr-defined]

        await self.handle_message(event)

    async def _on_event(self, frame: Dict[str, Any]) -> None:
        """Handle WeCom events (enter_chat, etc.)."""
        # Events don't require processing in the adapter layer
        event_type = frame.get("data", {}).get("event_type", "unknown")
        logger.debug(f"xwecom: received event: {event_type}")

    # ── Message parsing ─────────────────────────────────────────────────────

    def _parse_message_content(
        self, data: Dict[str, Any]
    ) -> Tuple[str, List[Dict[str, str]]]:
        """Parse message body into text and image references.

        Returns: (text, [{"url": ..., "aes_key": ..., "filename": ...}])
        """
        msgtype = data.get("msgtype", "text")
        text = ""
        images: List[Dict[str, str]] = []

        if msgtype == "text":
            text = data.get("text", {}).get("content", "")
        elif msgtype == "image":
            img = data.get("image", {})
            images.append({
                "url": img.get("url", ""),
                "aes_key": img.get("aeskey", ""),
                "filename": img.get("file_name", "image.png"),
            })
        elif msgtype == "mixed":
            # Mixed messages contain text + images
            items = data.get("mixed", {}).get("items", [])
            text_parts = []
            for item in items:
                item_type = item.get("type", "")
                if item_type == "text":
                    text_parts.append(item.get("content", ""))
                elif item_type == "image":
                    images.append({
                        "url": item.get("url", ""),
                        "aes_key": item.get("aeskey", ""),
                        "filename": item.get("file_name", "image.png"),
                    })
            text = "\n".join(text_parts)
        elif msgtype == "file":
            file_info = data.get("file", {})
            text = f"[文件] {file_info.get('file_name', 'unknown')}"
        elif msgtype == "voice":
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

        await client.disconnect()
        return {"success": True, "message_id": f"cron_{int(time.time())}"}
    except Exception as e:
        try:
            await client.disconnect()
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
