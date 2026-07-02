"""Tests for media uploader — resolve_media_file, apply_file_size_limits, upload_and_send_media."""

import asyncio
import base64
import hashlib
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from media import (
    ABSOLUTE_MAX_BYTES,
    FileSizeCheckResult,
    IMAGE_MAX_BYTES,
    UploadAndSendResult,
    VOICE_MAX_BYTES,
    apply_file_size_limits,
    detect_media_type,
    resolve_media_file,
    upload_and_send_media,
)


def run(coro):
    return asyncio.run(coro)


# ── apply_file_size_limits ─────────────────────────────────────────────────


class TestApplyFileSizeLimits:
    def test_image_within_limit(self):
        r = apply_file_size_limits(1000, "image", "image/png")
        assert r.final_type == "image"
        assert r.downgraded is False
        assert r.should_reject is False

    def test_image_over_limit_downgrades(self):
        r = apply_file_size_limits(IMAGE_MAX_BYTES + 1, "image", "image/png")
        assert r.final_type == "file"
        assert r.downgraded is True
        assert "图片" in (r.downgrade_note or "")

    def test_video_over_limit_downgrades(self):
        r = apply_file_size_limits(IMAGE_MAX_BYTES + 1, "video", "video/mp4")
        assert r.final_type == "file"
        assert r.downgraded is True

    def test_voice_over_2mb_downgrades(self):
        r = apply_file_size_limits(VOICE_MAX_BYTES + 1, "voice", "audio/amr")
        assert r.final_type == "file"
        assert r.downgraded is True

    def test_voice_non_amr_downgrades(self):
        r = apply_file_size_limits(1000, "voice", "audio/mp3")
        assert r.final_type == "file"
        assert r.downgraded is True
        assert "AMR" in (r.downgrade_note or "")

    def test_voice_amr_within_limit_ok(self):
        r = apply_file_size_limits(1000, "voice", "audio/amr")
        assert r.final_type == "voice"
        assert r.downgraded is False

    def test_above_absolute_max_rejects(self):
        r = apply_file_size_limits(ABSOLUTE_MAX_BYTES + 1, "file")
        assert r.should_reject is True
        assert "20MB" in (r.reject_reason or "")

    def test_file_within_absolute_ok(self):
        r = apply_file_size_limits(15 * 1024 * 1024, "file")
        assert r.should_reject is False
        assert r.final_type == "file"


# ── detect_media_type ──────────────────────────────────────────────────────


class TestDetectMediaType:
    def test_amr_voice_by_mime(self):
        assert detect_media_type("audio/amr") == "voice"

    def test_other_audio_still_voice(self):
        # Aligned with OpenClaw: every audio/* maps to voice (downgrade later if non-AMR)
        assert detect_media_type("audio/mpeg") == "voice"

    def test_image_video_file(self):
        assert detect_media_type("image/jpeg") == "image"
        assert detect_media_type("video/mp4") == "video"
        assert detect_media_type("application/pdf") == "file"

    def test_extension_fallback(self):
        assert detect_media_type("", "p.webp") == "image"
        assert detect_media_type("", "v.webm") == "video"
        assert detect_media_type("", "a.mp3") == "voice"
        assert detect_media_type("", "d.xlsx") == "file"


# ── resolve_media_file (local) ─────────────────────────────────────────────


class TestResolveMediaFile:
    def test_local_path_loads(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp.write(b"hello")
            tmp_path = tmp.name
        try:
            result = run(resolve_media_file(tmp_path, media_local_roots=None))
            assert result.buffer == b"hello"
            assert result.file_name == os.path.basename(tmp_path)
        finally:
            os.unlink(tmp_path)

    def test_local_path_outside_allowed_roots_rejected(self):
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp.write(b"x")
            tmp_path = tmp.name
        try:
            with pytest.raises(PermissionError):
                run(
                    resolve_media_file(
                        tmp_path,
                        media_local_roots=["/non/existent/path"],
                    )
                )
        finally:
            os.unlink(tmp_path)

    def test_local_path_within_allowed_root_ok(self):
        with tempfile.TemporaryDirectory() as d:
            fpath = os.path.join(d, "x.bin")
            with open(fpath, "wb") as fh:
                fh.write(b"data")
            result = run(resolve_media_file(fpath, media_local_roots=[d]))
            assert result.buffer == b"data"

    def test_missing_file(self):
        with pytest.raises(FileNotFoundError):
            run(resolve_media_file("/no/such/file.txt", media_local_roots=None))

    def test_http_uses_injected_fetch(self):
        async def fake_fetch(url):
            return (b"remote-bytes", "image/png", "img.png")

        result = run(
            resolve_media_file("https://example.com/img", http_fetch=fake_fetch)
        )
        assert result.buffer == b"remote-bytes"
        assert result.content_type == "image/png"
        assert result.file_name == "img.png"


# ── upload_and_send_media ──────────────────────────────────────────────────


class _FakeWsManager:
    """Fake _ws_manager that records init/chunk/finish calls."""

    def __init__(self, *, fail_at=None):
        self.calls = []
        self.fail_at = fail_at

    async def send_reply(self, req_id, body, cmd):
        self.calls.append({"req_id": req_id, "body": body, "cmd": cmd})
        if self.fail_at == cmd:
            return {"body": {}}
        if cmd == "aibot_upload_media_init":
            return {"body": {"upload_id": "U1"}}
        if cmd == "aibot_upload_media_chunk":
            return {"body": {"ok": True}}
        if cmd == "aibot_upload_media_finish":
            return {"body": {"media_id": "M-ABC"}}
        return {}


class _FakeClient:
    def __init__(self, *, ws_manager=None, sent=None, errcode=0):
        self._ws_manager = ws_manager or _FakeWsManager()
        self.sent = sent if sent is not None else []
        self.errcode = errcode

    async def send_message(self, chat_id, body):
        self.sent.append((chat_id, body))
        return {"errcode": self.errcode, "headers": {"req_id": "resp-r"}}


class TestUploadAndSendMedia:
    def test_happy_path_local_file(self):
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(b"PDF-body")
            tmp_path = tmp.name
        try:
            client = _FakeClient()
            result = run(
                upload_and_send_media(
                    client,
                    tmp_path,
                    "chat123",
                    media_local_roots=None,
                )
            )
            assert result.ok is True
            assert result.final_type == "file"
            assert client.sent[0][0] == "chat123"
            assert client.sent[0][1]["msgtype"] == "file"
            assert client.sent[0][1]["file"]["media_id"] == "M-ABC"
            calls = client._ws_manager.calls
            assert [call["cmd"] for call in calls] == [
                "aibot_upload_media_init",
                "aibot_upload_media_chunk",
                "aibot_upload_media_finish",
            ]
            assert calls[0]["req_id"].startswith("aibot_upload_media_init_")
            assert calls[0]["body"] == {
                "type": "file",
                "filename": os.path.basename(tmp_path),
                "total_size": len(b"PDF-body"),
                "total_chunks": 1,
                "md5": hashlib.md5(b"PDF-body").hexdigest(),
            }
            assert calls[1]["req_id"].startswith("aibot_upload_media_chunk_")
            assert calls[1]["body"] == {
                "upload_id": "U1",
                "chunk_index": 0,
                "base64_data": base64.b64encode(b"PDF-body").decode("ascii"),
            }
            assert calls[2]["req_id"].startswith("aibot_upload_media_finish_")
            assert calls[2]["body"] == {"upload_id": "U1"}
        finally:
            os.unlink(tmp_path)

    def test_rejected_when_too_large(self):
        # Forge a file 21MB
        big = b"x" * (ABSOLUTE_MAX_BYTES + 1)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(big)
            tmp_path = tmp.name
        try:
            client = _FakeClient()
            result = run(
                upload_and_send_media(
                    client,
                    tmp_path,
                    "chat",
                    media_local_roots=None,
                )
            )
            assert result.ok is False
            assert result.rejected is True
            assert "20MB" in (result.reject_reason or "")
        finally:
            os.unlink(tmp_path)

    def test_load_failure_returns_error(self):
        client = _FakeClient()
        result = run(
            upload_and_send_media(
                client,
                "/path/does/not/exist.pdf",
                "chat",
                media_local_roots=None,
            )
        )
        assert result.ok is False
        assert result.error is not None
