"""Media upload/download handling — aligned with OpenClaw media-uploader.ts / media-handler.ts."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import mimetypes
import os
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

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
    from .sdk.types import WsCmd
    from .sdk.utils import generate_req_id
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
    from sdk.types import WsCmd  # type: ignore[no-redef]
    from sdk.utils import generate_req_id  # type: ignore[no-redef]

logger = logging.getLogger(__name__)


# ── MIME → WeCom media type mapping ────────────────────────────────────────


def detect_media_type(mime_type: str, filename: str = "") -> str:
    """Detect WeCom media type from MIME type.

    Returns one of: image, video, voice, file.
    Aligned with OpenClaw: src/media-uploader.ts:detectWeComMediaType
    """
    mime_lower = mime_type.lower() if mime_type else ""

    if mime_lower.startswith("image/"):
        return "image"
    if mime_lower.startswith("video/"):
        return "video"
    if mime_lower in VOICE_SUPPORTED_MIMES:
        return "voice"
    # OpenClaw treats every audio/* as voice (and downgrades non-AMR later).
    if mime_lower.startswith("audio/") or mime_lower == "application/ogg":
        return "voice"

    # Fallback: check extension
    ext = os.path.splitext(filename)[1].lower() if filename else ""
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"):
        return "image"
    if ext in (".mp4", ".avi", ".mov", ".wmv", ".webm"):
        return "video"
    if ext in (".amr", ".mp3", ".wav", ".ogg", ".aac"):
        return "voice"

    return "file"


# ── Size check + downgrade ─────────────────────────────────────────────────


@dataclass
class FileSizeCheckResult:
    """Outcome of applying the size + format downgrade ladder.

    Aligned with OpenClaw: src/media-uploader.ts:FileSizeCheckResult
    """

    final_type: str
    should_reject: bool = False
    reject_reason: Optional[str] = None
    downgraded: bool = False
    downgrade_note: Optional[str] = None


def apply_file_size_limits(
    file_size: int,
    detected_type: str,
    content_type: Optional[str] = None,
) -> FileSizeCheckResult:
    """Apply WeCom size limits and downgrade strategies.

    Aligned with OpenClaw: src/media-uploader.ts:applyFileSizeLimits

    Rules:
      - voice with non-AMR MIME → file
      - image > 10MB → file
      - video > 10MB → file
      - voice > 2MB  → file
      - file  > 20MB → reject
      - any   > 20MB → reject

    Returns FileSizeCheckResult.
    """
    size_mb = file_size / (1024 * 1024)
    fmt_mb = f"{size_mb:.2f}"

    # Absolute ceiling — reject regardless of type.
    if file_size > ABSOLUTE_MAX_BYTES:
        return FileSizeCheckResult(
            final_type=detected_type,
            should_reject=True,
            reject_reason=(
                f"文件大小 {fmt_mb}MB 超过了企业微信允许的最大限制 20MB，无法发送。"
                "请尝试压缩文件或减小文件大小。"
            ),
        )

    if detected_type == "image" and file_size > IMAGE_MAX_BYTES:
        return FileSizeCheckResult(
            final_type="file",
            downgraded=True,
            downgrade_note=f"图片大小 {fmt_mb}MB 超过 10MB 限制，已转为文件格式发送",
        )

    if detected_type == "video" and file_size > VIDEO_MAX_BYTES:
        return FileSizeCheckResult(
            final_type="file",
            downgraded=True,
            downgrade_note=f"视频大小 {fmt_mb}MB 超过 10MB 限制，已转为文件格式发送",
        )

    if detected_type == "voice":
        # WeCom voice messages must be AMR.
        if content_type and content_type.lower() not in VOICE_SUPPORTED_MIMES:
            return FileSizeCheckResult(
                final_type="file",
                downgraded=True,
                downgrade_note=(
                    f"语音格式 {content_type} 不支持，企微仅支持 AMR 格式，"
                    "已转为文件格式发送"
                ),
            )
        if file_size > VOICE_MAX_BYTES:
            return FileSizeCheckResult(
                final_type="file",
                downgraded=True,
                downgrade_note=f"语音大小 {fmt_mb}MB 超过 2MB 限制，已转为文件格式发送",
            )

    return FileSizeCheckResult(final_type=detected_type)


# ── Backward-compat tuple-style wrapper ────────────────────────────────────


def check_file_size(
    media_bytes: bytes, media_type: str, filename: str = ""
) -> Tuple[bool, str, str]:
    """Check file size limits and potentially downgrade type.

    Returns: (ok, final_media_type, error_message)
    Aligned with OpenClaw applyFileSizeLimits logic — preserved tuple form for
    backward compatibility with existing tests. The error message stays in
    English/ASCII ("too large") to match adapter-layer return contract; the
    Chinese-user-facing message lives in :class:`FileSizeCheckResult`.
    """
    size = len(media_bytes)
    # Best-effort content_type guess from filename for voice path.
    content_type, _ = mimetypes.guess_type(filename) if filename else (None, None)

    if size > ABSOLUTE_MAX_BYTES:
        return False, media_type, f"File too large ({size} bytes > {ABSOLUTE_MAX_BYTES} limit)"

    result = apply_file_size_limits(size, media_type, content_type)
    if result.downgraded and result.downgrade_note:
        logger.info(result.downgrade_note)
    # Final cap: even after downgrade, file > FILE_MAX_BYTES is rejected.
    if result.final_type == "file" and size > FILE_MAX_BYTES:
        return False, result.final_type, f"File too large ({size} bytes > {FILE_MAX_BYTES} limit)"
    return True, result.final_type, ""


# ── Resolving a media reference (URL / local path / bytes) ─────────────────


@dataclass
class ResolvedMedia:
    """Loaded media buffer with detected content_type and filename.

    Aligned with OpenClaw: src/media-uploader.ts:ResolvedMedia
    """

    buffer: bytes
    content_type: str
    file_name: str


def _extract_filename_from_url(media_url: str, content_type: Optional[str]) -> str:
    """Mirror OpenClaw's extractFileName."""
    try:
        parsed = urlparse(media_url)
        path = parsed.path or media_url
    except Exception:
        path = media_url
    parts = [p for p in path.split("/") if p]
    if parts:
        last = unquote(parts[-1])
        if "." in last:
            return last
    ext = _mime_to_extension(content_type or "application/octet-stream")
    return f"media_{int(time.time() * 1000)}{ext}"


def _mime_to_extension(mime: str) -> str:
    """MIME-to-extension table, aligned with OpenClaw mimeToExtension."""
    table = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/bmp": ".bmp",
        "image/svg+xml": ".svg",
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
        "video/x-msvideo": ".avi",
        "video/webm": ".webm",
        "audio/mpeg": ".mp3",
        "audio/ogg": ".ogg",
        "audio/wav": ".wav",
        "audio/amr": ".amr",
        "audio/aac": ".aac",
        "application/pdf": ".pdf",
        "application/zip": ".zip",
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "text/plain": ".txt",
    }
    return table.get(mime, ".bin")


def _is_local_path_allowed(
    abs_path: str, allowed_roots: Optional[List[str]]
) -> bool:
    """Check whether ``abs_path`` is inside any allowed root.

    A ``None`` or empty allowlist means "no restriction".
    """
    if not allowed_roots:
        return True
    abs_path = os.path.realpath(abs_path)
    for root in allowed_roots:
        root_abs = os.path.realpath(os.path.expanduser(root))
        try:
            common = os.path.commonpath([abs_path, root_abs])
        except ValueError:
            continue
        if common == root_abs:
            return True
    return False


async def resolve_media_file(
    media_url: str,
    *,
    media_local_roots: Optional[List[str]] = None,
    http_fetch: Optional[Any] = None,
) -> ResolvedMedia:
    """Load a media file from URL or local path into a buffer.

    Aligned with OpenClaw: src/media-uploader.ts:resolveMediaFile

    Args:
        media_url: ``http(s)://…``, ``file://…``, or absolute filesystem path.
        media_local_roots: allowlist of directories from which local reads
            are permitted. ``None`` disables the check.
        http_fetch: optional async callable ``async (url) -> (bytes, content_type, filename)``
            used to load remote URLs. If omitted, ``aiohttp`` is used.
    """
    if not media_url:
        raise ValueError("media_url is required")

    parsed = urlparse(media_url)

    # ── Local path ────────────────────────────────────────────────────
    if parsed.scheme in ("", "file"):
        local_path = parsed.path if parsed.scheme == "file" else media_url
        abs_path = os.path.realpath(os.path.expanduser(local_path))
        if not _is_local_path_allowed(abs_path, media_local_roots):
            raise PermissionError(
                f"LocalMediaAccessError: {abs_path} not in mediaLocalRoots"
            )
        if not os.path.isfile(abs_path):
            raise FileNotFoundError(f"Local media file not found: {abs_path}")
        with open(abs_path, "rb") as fh:
            buf = fh.read()
        content_type, _ = mimetypes.guess_type(abs_path)
        return ResolvedMedia(
            buffer=buf,
            content_type=content_type or "application/octet-stream",
            file_name=os.path.basename(abs_path),
        )

    # ── HTTP(S) ──────────────────────────────────────────────────────
    if parsed.scheme in ("http", "https"):
        if http_fetch is None:
            try:
                import aiohttp  # type: ignore
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError(
                    "aiohttp is required to fetch remote media without http_fetch"
                ) from exc

            async def _default_fetch(url: str) -> Tuple[bytes, str, Optional[str]]:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        resp.raise_for_status()
                        data = await resp.read()
                        ctype = resp.headers.get("Content-Type", "")
                        # crude filename extraction from Content-Disposition
                        cd = resp.headers.get("Content-Disposition", "")
                        fname = None
                        if "filename=" in cd:
                            fname = cd.split("filename=", 1)[1].strip('";')
                        return data, ctype, fname

            http_fetch = _default_fetch  # type: ignore[assignment]

        buf, content_type, fname = await http_fetch(media_url)  # type: ignore[misc]
        if not buf:
            raise RuntimeError(f"Failed to load media from {media_url}: empty buffer")
        content_type = (content_type or "").split(";", 1)[0].strip() or "application/octet-stream"
        file_name = fname or _extract_filename_from_url(media_url, content_type)
        return ResolvedMedia(buffer=buf, content_type=content_type, file_name=file_name)

    raise ValueError(f"Unsupported media URL scheme: {media_url}")


# ── Chunked upload ─────────────────────────────────────────────────────────


async def upload_media_chunked(
    ws_client: Any,
    media_bytes: bytes,
    filename: str,
    media_type: str,
) -> Optional[str]:
    """Upload media to WeCom via chunked upload protocol.

    Aligned with OpenClaw: src/media-uploader.ts uploadMedia path which the
    SDK wraps as ``wsClient.uploadMedia(buffer, opts)``.

    Protocol:
      1. ``aibot_upload_media_init`` → ``upload_id``
      2. ``aibot_upload_media_chunk`` × N
      3. ``aibot_upload_media_finish`` → ``media_id``

    Returns media_id on success, None on failure.
    """
    total_size = len(media_bytes)
    chunk_count = (total_size + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE
    md5 = hashlib.md5(media_bytes).hexdigest()

    if chunk_count > MAX_UPLOAD_CHUNKS:
        logger.error(f"Too many chunks ({chunk_count} > {MAX_UPLOAD_CHUNKS})")
        return None

    ws_manager = getattr(ws_client, "_ws_manager", None)
    if ws_manager is None:
        logger.error("ws_client has no _ws_manager — cannot upload media")
        return None

    # Step 1: Init upload
    try:
        init_body = {
            "type": media_type,
            "filename": filename,
            "total_size": total_size,
            "total_chunks": chunk_count,
            "md5": md5,
        }
        init_resp = await ws_manager.send_reply(
            generate_req_id(WsCmd.UPLOAD_MEDIA_INIT),
            init_body,
            WsCmd.UPLOAD_MEDIA_INIT,
        )
        upload_id = _frame_payload(init_resp).get("upload_id")
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
                "base64_data": chunk_b64,
            }
            await ws_manager.send_reply(
                generate_req_id(WsCmd.UPLOAD_MEDIA_CHUNK),
                chunk_body,
                WsCmd.UPLOAD_MEDIA_CHUNK,
            )
        except Exception as e:
            logger.error(f"Upload chunk {i} error: {e}")
            return None

    # Step 3: Finish upload
    try:
        finish_body = {"upload_id": upload_id}
        finish_resp = await ws_manager.send_reply(
            generate_req_id(WsCmd.UPLOAD_MEDIA_FINISH),
            finish_body,
            WsCmd.UPLOAD_MEDIA_FINISH,
        )
        media_id = _frame_payload(finish_resp).get("media_id")
        if not media_id:
            logger.error(f"Upload finish failed: {finish_resp}")
            return None
        logger.info(f"Media uploaded: {filename} -> {media_id}")
        return media_id
    except Exception as e:
        logger.error(f"Upload finish error: {e}")
        return None


def _frame_payload(frame: Dict[str, Any]) -> Dict[str, Any]:
    """Return SDK-style response payload, accepting older test/fork shapes too."""
    body = frame.get("body")
    if isinstance(body, dict):
        return body
    data = frame.get("data")
    if isinstance(data, dict):
        return data
    return {}


# ── Download helper ────────────────────────────────────────────────────────


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


# ── High-level upload + send (proactive) ───────────────────────────────────


@dataclass
class UploadAndSendResult:
    """Outcome of :func:`upload_and_send_media`.

    Aligned with OpenClaw: src/media-uploader.ts:UploadAndSendMediaResult
    """

    ok: bool
    message_id: Optional[str] = None
    final_type: Optional[str] = None
    rejected: bool = False
    reject_reason: Optional[str] = None
    downgraded: bool = False
    downgrade_note: Optional[str] = None
    error: Optional[str] = None


async def upload_and_send_media(
    ws_client: Any,
    media_url: str,
    chat_id: str,
    *,
    media_local_roots: Optional[List[str]] = None,
    http_fetch: Optional[Any] = None,
) -> UploadAndSendResult:
    """Resolve → detect → size-check → upload → send.

    Aligned with OpenClaw: src/media-uploader.ts:uploadAndSendMedia
    """
    try:
        media = await resolve_media_file(
            media_url,
            media_local_roots=media_local_roots,
            http_fetch=http_fetch,
        )
    except Exception as err:  # noqa: BLE001
        logger.error(f"[wecom] Failed to load media {media_url}: {err}")
        return UploadAndSendResult(ok=False, error=str(err))

    detected_type = detect_media_type(media.content_type, media.file_name)
    size_check = apply_file_size_limits(
        len(media.buffer), detected_type, media.content_type
    )

    if size_check.should_reject:
        logger.warning(f"[wecom] Media rejected: {size_check.reject_reason}")
        return UploadAndSendResult(
            ok=False,
            rejected=True,
            reject_reason=size_check.reject_reason,
            final_type=size_check.final_type,
        )

    final_type = size_check.final_type
    media_id = await upload_media_chunked(
        ws_client, media.buffer, media.file_name, final_type
    )
    if not media_id:
        return UploadAndSendResult(
            ok=False,
            error="Media upload failed",
            final_type=final_type,
            downgraded=size_check.downgraded,
            downgrade_note=size_check.downgrade_note,
        )

    try:
        resp = await ws_client.send_message(
            chat_id, {"msgtype": final_type, final_type: {"media_id": media_id}}
        )
    except Exception as err:  # noqa: BLE001
        return UploadAndSendResult(
            ok=False,
            error=f"Media send failed: {err}",
            final_type=final_type,
            downgraded=size_check.downgraded,
            downgrade_note=size_check.downgrade_note,
        )

    # Best-effort message_id extraction.
    message_id = None
    if isinstance(resp, dict):
        headers = resp.get("headers") or {}
        message_id = headers.get("req_id")
    if not message_id:
        message_id = f"wecom-media-{int(time.time() * 1000)}"

    return UploadAndSendResult(
        ok=True,
        message_id=message_id,
        final_type=final_type,
        downgraded=size_check.downgraded,
        downgrade_note=size_check.downgrade_note,
    )
