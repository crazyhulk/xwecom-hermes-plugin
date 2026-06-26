"""Media upload/download handling — aligned with OpenClaw media-uploader.ts / media-handler.ts."""

from __future__ import annotations

import base64
import logging
import mimetypes
import os
from typing import Any, Dict, Optional, Tuple

try:
    from .constants import (
        ABSOLUTE_MAX_BYTES,
        FILE_MAX_BYTES,
        IMAGE_MAX_BYTES,
        MAX_UPLOAD_CHUNKS,
        UPLOAD_CHUNK_SIZE,
        VIDEO_MAX_BYTES,
        VOICE_MAX_BYTES,
        VOICE_SUPPORTED_MIMES,
    )
except ImportError:
    from constants import (  # type: ignore[no-redef]
        ABSOLUTE_MAX_BYTES,
        FILE_MAX_BYTES,
        IMAGE_MAX_BYTES,
        MAX_UPLOAD_CHUNKS,
        UPLOAD_CHUNK_SIZE,
        VIDEO_MAX_BYTES,
        VOICE_MAX_BYTES,
        VOICE_SUPPORTED_MIMES,
    )

logger = logging.getLogger(__name__)


def detect_media_type(mime_type: str, filename: str = "") -> str:
    """Detect WeCom media type from MIME type.

    Returns one of: image, video, voice, file
    """
    mime_lower = mime_type.lower() if mime_type else ""

    if mime_lower.startswith("image/"):
        return "image"
    if mime_lower.startswith("video/"):
        return "video"
    if mime_lower in VOICE_SUPPORTED_MIMES:
        return "voice"

    # Fallback: check extension
    ext = os.path.splitext(filename)[1].lower() if filename else ""
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"):
        return "image"
    if ext in (".mp4", ".avi", ".mov", ".wmv"):
        return "video"
    if ext == ".amr":
        return "voice"

    return "file"


def check_file_size(
    media_bytes: bytes, media_type: str, filename: str = ""
) -> Tuple[bool, str, str]:
    """Check file size limits and potentially downgrade type.

    Returns: (ok, final_media_type, error_message)
    Aligned with OpenClaw applyFileSizeLimits logic:
    - image >10MB → file
    - video >10MB → file
    - voice non-AMR → file
    - voice >2MB → file
    - file >20MB → reject
    """
    size = len(media_bytes)

    if size > ABSOLUTE_MAX_BYTES:
        return False, media_type, f"File too large ({size} bytes > {ABSOLUTE_MAX_BYTES} limit)"

    if media_type == "image" and size > IMAGE_MAX_BYTES:
        logger.info(f"Image {filename} too large ({size}B), downgrading to file")
        media_type = "file"
    elif media_type == "video" and size > VIDEO_MAX_BYTES:
        logger.info(f"Video {filename} too large ({size}B), downgrading to file")
        media_type = "file"
    elif media_type == "voice" and size > VOICE_MAX_BYTES:
        logger.info(f"Voice {filename} too large ({size}B), downgrading to file")
        media_type = "file"

    if media_type == "file" and size > FILE_MAX_BYTES:
        return False, media_type, f"File too large ({size} bytes > {FILE_MAX_BYTES} limit)"

    return True, media_type, ""


async def upload_media_chunked(
    ws_client: Any,
    media_bytes: bytes,
    filename: str,
    media_type: str,
) -> Optional[str]:
    """Upload media to WeCom via chunked upload protocol.

    Protocol:
    1. aibot_upload_media_init → get upload_id
    2. aibot_upload_media_chunk × N → upload chunks
    3. aibot_upload_media_finish → get media_id

    Returns media_id on success, None on failure.
    """
    from .sdk.types import WsCmd

    total_size = len(media_bytes)
    chunk_count = (total_size + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE

    if chunk_count > MAX_UPLOAD_CHUNKS:
        logger.error(f"Too many chunks ({chunk_count} > {MAX_UPLOAD_CHUNKS})")
        return None

    # Step 1: Init upload
    try:
        init_body = {
            "media_type": media_type,
            "file_name": filename,
            "file_size": total_size,
            "chunk_count": chunk_count,
        }
        init_resp = await ws_client._ws_manager.send_reply(
            f"upload_init_{os.urandom(4).hex()}",
            init_body,
            "aibot_upload_media_init",
        )
        upload_id = init_resp.get("data", {}).get("upload_id")
        if not upload_id:
            logger.error(f"Upload init failed: {init_resp}")
            return None
    except Exception as e:
        logger.error(f"Upload init error: {e}")
        return None

    # Step 2: Upload chunks
    for i in range(chunk_count):
        offset = i * UPLOAD_CHUNK_SIZE
        chunk = media_bytes[offset : offset + UPLOAD_CHUNK_SIZE]
        chunk_b64 = base64.b64encode(chunk).decode("ascii")

        try:
            chunk_body = {
                "upload_id": upload_id,
                "chunk_index": i,
                "chunk_data": chunk_b64,
            }
            await ws_client._ws_manager.send_reply(
                f"upload_chunk_{upload_id}_{i}",
                chunk_body,
                "aibot_upload_media_chunk",
            )
        except Exception as e:
            logger.error(f"Upload chunk {i} error: {e}")
            return None

    # Step 3: Finish upload
    try:
        finish_body = {"upload_id": upload_id}
        finish_resp = await ws_client._ws_manager.send_reply(
            f"upload_finish_{upload_id}",
            finish_body,
            "aibot_upload_media_finish",
        )
        media_id = finish_resp.get("data", {}).get("media_id")
        if not media_id:
            logger.error(f"Upload finish failed: {finish_resp}")
            return None
        logger.info(f"Media uploaded: {filename} -> {media_id}")
        return media_id
    except Exception as e:
        logger.error(f"Upload finish error: {e}")
        return None


async def download_and_decrypt(
    ws_client: Any, url: str, aes_key: Optional[str] = None
) -> Optional[bytes]:
    """Download media file and optionally decrypt with AES key.

    Uses the SDK's built-in download_file method.
    """
    if not url:
        return None
    try:
        data, filename = await ws_client.download_file(url, aes_key)
        logger.debug(f"Downloaded media: {filename}, {len(data)} bytes")
        return data
    except Exception as e:
        logger.error(f"Media download failed: {e}")
        return None
