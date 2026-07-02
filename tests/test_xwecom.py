"""Tests for xwecom plugin — policy, stream, media, and adapter logic."""

import asyncio
import os
import sys
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Add parent to path so we can import the plugin
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Policy tests ────────────────────────────────────────────────────────────


class TestDMPolicy:
    """Test DM access control policies."""

    def test_open_allows_all(self):
        from policy import check_dm_policy

        assert check_dm_policy("open", [], "any_user") is True
        assert check_dm_policy("open", ["other"], "any_user") is True

    def test_disabled_rejects_all(self):
        from policy import check_dm_policy

        assert check_dm_policy("disabled", [], "any_user") is False
        assert check_dm_policy("disabled", ["any_user"], "any_user") is False

    def test_allowlist_filters(self):
        from policy import check_dm_policy

        assert check_dm_policy("allowlist", ["user1", "user2"], "user1") is True
        assert check_dm_policy("allowlist", ["user1", "user2"], "user3") is False

    def test_allowlist_wildcard(self):
        from policy import check_dm_policy

        assert check_dm_policy("allowlist", ["*"], "anyone") is True

    def test_allowlist_case_insensitive(self):
        from policy import check_dm_policy

        assert check_dm_policy("allowlist", ["User1"], "user1") is True

    def test_pairing_same_as_allowlist(self):
        from policy import check_dm_policy

        assert check_dm_policy("pairing", ["user1"], "user1") is True
        assert check_dm_policy("pairing", ["user1"], "user2") is False

    def test_prefix_stripping(self):
        from policy import check_dm_policy

        assert check_dm_policy("allowlist", ["wecom:user:user1"], "user1") is True
        assert check_dm_policy("allowlist", ["user:user1"], "user1") is True


class TestGroupPolicy:
    """Test Group access control policies."""

    def test_open_allows_all(self):
        from policy import check_group_policy

        assert check_group_policy("open", [], "group1", "user1") is True

    def test_disabled_rejects_all(self):
        from policy import check_group_policy

        assert check_group_policy("disabled", [], "group1", "user1") is False

    def test_allowlist_filters_groups(self):
        from policy import check_group_policy

        assert check_group_policy("allowlist", ["group1"], "group1", "user1") is True
        assert check_group_policy("allowlist", ["group1"], "group2", "user1") is False

    def test_per_group_sender_allowlist(self):
        from policy import check_group_policy

        groups_config = {"group1": {"allow_from": ["user1", "user2"]}}
        assert (
            check_group_policy("allowlist", ["group1"], "group1", "user1", groups_config)
            is True
        )
        assert (
            check_group_policy("allowlist", ["group1"], "group1", "user3", groups_config)
            is False
        )


# ── Stream tests ────────────────────────────────────────────────────────────


class TestBlockChunker:
    """Test block chunking for stream replies."""

    def test_below_min_chars_no_emit(self):
        from stream import BlockChunker

        chunker = BlockChunker(min_chars=120, max_chars=360)
        # 50 chars — below minimum
        text = "x" * 50
        assert chunker.should_emit(text) is False

    def test_above_max_chars_force_emit(self):
        from stream import BlockChunker

        chunker = BlockChunker(min_chars=120, max_chars=360)
        text = "x" * 400
        assert chunker.should_emit(text) is True

    def test_sentence_boundary_between_min_max(self):
        from stream import BlockChunker

        chunker = BlockChunker(min_chars=10, max_chars=100)
        text = "Hello world. This is a test."
        assert chunker.should_emit(text) is True  # Has period, > min

    def test_no_boundary_between_min_max(self):
        from stream import BlockChunker

        chunker = BlockChunker(min_chars=10, max_chars=100)
        text = "Hello world this is a test without ending"
        assert chunker.should_emit(text) is False  # No period, between min/max

    def test_force_always_emits(self):
        from stream import BlockChunker

        chunker = BlockChunker(min_chars=120, max_chars=360)
        text = "short"
        assert chunker.should_emit(text, force=True) is True

    def test_mark_emitted_advances(self):
        from stream import BlockChunker

        chunker = BlockChunker(min_chars=10, max_chars=50)
        text1 = "First sentence. "
        chunker.mark_emitted(text1)
        assert chunker.emitted_length == len(text1)

        # New content below min
        text2 = text1 + "short"
        assert chunker.should_emit(text2) is False

    def test_reset(self):
        from stream import BlockChunker

        chunker = BlockChunker()
        chunker.mark_emitted("some text")
        chunker.reset()
        assert chunker.emitted_length == 0


class TestBlockStreamManager:
    """Test stream session management."""

    def test_create_session(self):
        from stream import BlockStreamManager

        mgr = BlockStreamManager()
        session = mgr.create_session("req_123")
        assert session.req_id == "req_123"
        assert session.stream_id.startswith("stream_")
        assert session.frame_count == 0
        assert session.finished is False

    def test_get_session(self):
        from stream import BlockStreamManager

        mgr = BlockStreamManager()
        session = mgr.create_session("req_123")
        retrieved = mgr.get_session("req_123")
        assert retrieved is session

    def test_finish_session(self):
        from stream import BlockStreamManager

        mgr = BlockStreamManager()
        mgr.create_session("req_123")
        finished = mgr.finish_session("req_123")
        assert finished.finished is True
        assert mgr.get_session("req_123") is None

    def test_can_send_frame(self):
        from stream import BlockStreamManager, MAX_INTERMEDIATE_FRAMES

        mgr = BlockStreamManager()
        session = mgr.create_session("req_123")
        assert mgr.can_send_frame(session) is True
        session.frame_count = MAX_INTERMEDIATE_FRAMES
        assert mgr.can_send_frame(session) is False


# ── Media tests ─────────────────────────────────────────────────────────────


class TestMediaTypeDetection:
    """Test media type detection."""

    def test_image_by_mime(self):
        from media import detect_media_type

        assert detect_media_type("image/png") == "image"
        assert detect_media_type("image/jpeg") == "image"
        assert detect_media_type("image/gif") == "image"

    def test_video_by_mime(self):
        from media import detect_media_type

        assert detect_media_type("video/mp4") == "video"
        assert detect_media_type("video/quicktime") == "video"

    def test_voice_by_mime(self):
        from media import detect_media_type

        assert detect_media_type("audio/amr") == "voice"

    def test_fallback_to_file(self):
        from media import detect_media_type

        assert detect_media_type("application/pdf") == "file"
        assert detect_media_type("text/plain") == "file"

    def test_detection_by_extension(self):
        from media import detect_media_type

        assert detect_media_type("", "photo.jpg") == "image"
        assert detect_media_type("", "video.mp4") == "video"
        assert detect_media_type("", "voice.amr") == "voice"
        assert detect_media_type("", "doc.pdf") == "file"


class TestFileSizeCheck:
    """Test file size validation and downgrade logic."""

    def test_image_within_limit(self):
        from media import check_file_size

        ok, media_type, err = check_file_size(b"x" * 1000, "image")
        assert ok is True
        assert media_type == "image"

    def test_image_over_limit_downgrades(self):
        from media import check_file_size, IMAGE_MAX_BYTES

        data = b"x" * (IMAGE_MAX_BYTES + 1)
        ok, media_type, err = check_file_size(data, "image")
        assert ok is True
        assert media_type == "file"  # downgraded

    def test_file_over_absolute_limit_rejected(self):
        from media import check_file_size, ABSOLUTE_MAX_BYTES

        data = b"x" * (ABSOLUTE_MAX_BYTES + 1)
        ok, media_type, err = check_file_size(data, "file")
        assert ok is False
        assert "too large" in err.lower()

    def test_voice_over_limit_downgrades(self):
        from media import check_file_size, VOICE_MAX_BYTES

        data = b"x" * (VOICE_MAX_BYTES + 1)
        ok, media_type, err = check_file_size(data, "voice")
        assert ok is True
        assert media_type == "file"


# ── Deduplicator tests ──────────────────────────────────────────────────────


class TestMessageDeduplicator:
    """Test message deduplication."""

    def test_first_message_not_duplicate(self):
        from adapter import MessageDeduplicator

        dedup = MessageDeduplicator()
        assert dedup.is_duplicate("msg_1") is False

    def test_same_message_is_duplicate(self):
        from adapter import MessageDeduplicator

        dedup = MessageDeduplicator()
        dedup.is_duplicate("msg_1")
        assert dedup.is_duplicate("msg_1") is True

    def test_different_messages_not_duplicate(self):
        from adapter import MessageDeduplicator

        dedup = MessageDeduplicator()
        dedup.is_duplicate("msg_1")
        assert dedup.is_duplicate("msg_2") is False

    def test_expired_entries_cleaned(self):
        from adapter import MessageDeduplicator

        dedup = MessageDeduplicator(ttl=0.1)
        dedup.is_duplicate("msg_1")
        time.sleep(0.2)
        # After TTL, should not be considered duplicate
        assert dedup.is_duplicate("msg_1") is False


# ── Adapter unit tests ──────────────────────────────────────────────────────


class TestXWeComAdapterParsing:
    """Test adapter message parsing logic."""

    def test_parse_text_message(self):
        # We need to test _parse_message_content without gateway imports
        # So test the logic directly
        from adapter import XWeComAdapter

        # Mock the parent class init
        with patch("adapter.BasePlatformAdapter.__init__", return_value=None):
            adapter = XWeComAdapter.__new__(XWeComAdapter)
            text, images = adapter._parse_message_content({
                "msgtype": "text",
                "text": {"content": "Hello world"},
            })
            assert text == "Hello world"
            assert images == []

    def test_parse_image_message(self):
        from adapter import XWeComAdapter

        with patch("adapter.BasePlatformAdapter.__init__", return_value=None):
            adapter = XWeComAdapter.__new__(XWeComAdapter)
            text, images = adapter._parse_message_content({
                "msgtype": "image",
                "image": {
                    "url": "https://example.com/img.png",
                    "aeskey": "test_key",
                    "file_name": "photo.png",
                },
            })
            assert text == ""
            assert len(images) == 1
            assert images[0]["url"] == "https://example.com/img.png"
            assert images[0]["aes_key"] == "test_key"

    def test_parse_mixed_message(self):
        from adapter import XWeComAdapter

        with patch("adapter.BasePlatformAdapter.__init__", return_value=None):
            adapter = XWeComAdapter.__new__(XWeComAdapter)
            text, images = adapter._parse_message_content({
                "msgtype": "mixed",
                "mixed": {
                    "items": [
                        {"type": "text", "content": "Look at this:"},
                        {"type": "image", "url": "https://img.com/1.png", "aeskey": "k1"},
                    ]
                },
            })
            assert text == "Look at this:"
            assert len(images) == 1

    def test_is_group_chat(self):
        from adapter import XWeComAdapter

        # Short IDs are DMs
        assert XWeComAdapter._is_group_chat("zhangsan") is False
        # Long IDs or with @ are groups
        assert XWeComAdapter._is_group_chat("a" * 33) is True
        assert XWeComAdapter._is_group_chat("group@123") is True
        # Empty is not group
        assert XWeComAdapter._is_group_chat("") is False

    def test_coerce_list(self):
        from adapter import XWeComAdapter

        assert XWeComAdapter._coerce_list(None) == []
        assert XWeComAdapter._coerce_list("a,b,c") == ["a", "b", "c"]
        assert XWeComAdapter._coerce_list(["x", "y"]) == ["x", "y"]
        assert XWeComAdapter._coerce_list("single") == ["single"]

    def test_adapter_reply_ack_timeout_defaults_to_30s(self):
        from adapter import XWeComAdapter
        from gateway.config import PlatformConfig

        adapter = XWeComAdapter(PlatformConfig(extra={}))

        assert adapter._reply_ack_timeout_s == 30.0

    def test_adapter_reply_ack_timeout_can_be_configured(self):
        from adapter import XWeComAdapter
        from gateway.config import PlatformConfig

        adapter = XWeComAdapter(PlatformConfig(extra={"reply_ack_timeout_s": 45}))

        assert adapter._reply_ack_timeout_s == 45.0

    def test_interpret_scoped_lock_result_supports_legacy_bool(self):
        from adapter import XWeComAdapter

        assert XWeComAdapter._interpret_scoped_lock_result(True) == (True, None)
        assert XWeComAdapter._interpret_scoped_lock_result(False) == (False, None)

    def test_interpret_scoped_lock_result_supports_tuple_api(self):
        from adapter import XWeComAdapter

        existing = {"pid": 12345}

        assert XWeComAdapter._interpret_scoped_lock_result((True, existing)) == (
            True,
            existing,
        )
        assert XWeComAdapter._interpret_scoped_lock_result((False, existing)) == (
            False,
            existing,
        )


# ── Integration-style tests ─────────────────────────────────────────────────


class TestPluginRegistration:
    """Test that plugin registration works correctly."""

    def test_check_requirements(self):
        from adapter import check_requirements

        # Should be True if websockets/aiohttp/cryptography installed
        result = check_requirements()
        assert isinstance(result, bool)

    def test_validate_config_with_env(self):
        from adapter import validate_config

        with patch.dict(os.environ, {"XWECOM_BOT_ID": "test", "XWECOM_SECRET": "sec"}):
            config = MagicMock()
            config.extra = {}
            assert validate_config(config) is True

    def test_validate_config_with_agent_outbound_env(self):
        from adapter import validate_config

        with patch.dict(
            os.environ,
            {
                "XWECOM_CORP_ID": "wxCORP",
                "XWECOM_CORP_SECRET": "secret",
                "XWECOM_AGENT_ID": "1000002",
            },
            clear=True,
        ):
            config = MagicMock()
            config.extra = {}
            assert validate_config(config) is True

    def test_validate_config_without_env(self):
        from adapter import validate_config

        with patch.dict(os.environ, {}, clear=True):
            # Remove any XWECOM vars
            for key in list(os.environ.keys()):
                if key.startswith("XWECOM_"):
                    del os.environ[key]
            config = MagicMock()
            config.extra = {}
            assert validate_config(config) is False

    def test_env_enablement_with_vars(self):
        from adapter import _env_enablement

        with patch.dict(
            os.environ,
            {
                "XWECOM_BOT_ID": "bot_123",
                "XWECOM_SECRET": "secret_456",
                "XWECOM_HOME_CHANNEL": "chat_789",
            },
        ):
            result = _env_enablement()
            assert result is not None
            assert result["bot_id"] == "bot_123"
            assert result["secret"] == "secret_456"
            assert result["home_channel"]["chat_id"] == "chat_789"

    def test_env_enablement_with_agent_outbound_vars(self):
        from adapter import _env_enablement

        with patch.dict(
            os.environ,
            {
                "XWECOM_CORP_ID": "wxCORP",
                "XWECOM_CORP_SECRET": "secret",
                "XWECOM_AGENT_ID": "1000002",
            },
            clear=True,
        ):
            result = _env_enablement()

        assert result is not None
        assert result["corp_id"] == "wxCORP"
        assert result["corp_secret"] == "secret"
        assert result["agent_id"] == "1000002"
        assert "callback_enabled" not in result

    def test_env_enablement_without_vars(self):
        from adapter import _env_enablement

        with patch.dict(os.environ, {}, clear=True):
            for key in list(os.environ.keys()):
                if key.startswith("XWECOM_"):
                    del os.environ[key]
            result = _env_enablement()
            assert result is None

    def test_register_calls_ctx(self):
        from adapter import register

        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()
        kwargs = ctx.register_platform.call_args[1]
        assert kwargs["name"] == "xwecom"
        assert kwargs["label"] == "XWeCom (企业微信 · Official SDK)"
        assert kwargs["required_env"] == ["XWECOM_BOT_ID", "XWECOM_SECRET"]
        assert kwargs["cron_deliver_env_var"] == "XWECOM_HOME_CHANNEL"
        assert kwargs["max_message_length"] == 4000
        assert kwargs["emoji"] == "💼"
        assert kwargs["standalone_sender_fn"] is not None
        assert kwargs["allowed_users_env"] == "XWECOM_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "XWECOM_ALLOW_ALL_USERS"

    def test_resolve_wecom_target(self):
        from adapter import _resolve_wecom_target

        assert _resolve_wecom_target("wecom:user:alice") == {"touser": "alice"}
        assert _resolve_wecom_target("party:1") == {"toparty": "1"}
        assert _resolve_wecom_target("tag:2") == {"totag": "2"}
        assert _resolve_wecom_target("chat:wr123") == {"chatid": "wr123"}
        assert _resolve_wecom_target("wc123") == {"chatid": "wc123"}
        assert _resolve_wecom_target("123") == {"toparty": "123"}
        assert _resolve_wecom_target("alice") == {"touser": "alice"}

    @pytest.mark.asyncio
    async def test_standalone_send_prefers_agent_http(self):
        from adapter import _standalone_send

        config = MagicMock()
        config.extra = {
            "bot_id": "bot_123",
            "secret": "secret_456",
            "corp_id": "wxCORP",
            "corp_secret": "agent_secret",
            "agent_id": "1000002",
        }
        post_calls = []

        class FakeResponse:
            def __init__(self, data):
                self._data = data

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def json(self, content_type=None):
                return self._data

        class FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def get(self, url, **kwargs):
                return FakeResponse({"errcode": 0, "access_token": "ACCESS"})

            def post(self, url, **kwargs):
                post_calls.append((url, kwargs))
                return FakeResponse({"errcode": 0, "msgid": "MSG1"})

        with (
            patch("adapter.ClientSession", return_value=FakeSession()),
            patch("adapter.acquire_scoped_lock") as acquire_lock,
            patch("adapter.WSClient") as ws_client,
        ):
            result = await _standalone_send(config, "user:alice", "hello")

        assert result["success"] is True
        assert result["message_id"] == "MSG1"
        assert result["transport"] == "agent_http"
        assert post_calls[0][0].endswith("/cgi-bin/message/send")
        assert post_calls[0][1]["json"]["touser"] == "alice"
        assert post_calls[0][1]["json"]["agentid"] == 1000002
        acquire_lock.assert_not_called()
        ws_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_standalone_send_refuses_when_scoped_lock_is_held(self):
        from adapter import _standalone_send

        config = MagicMock()
        config.extra = {"bot_id": "bot_123", "secret": "secret_456"}

        with (
            patch("adapter.acquire_scoped_lock", return_value=(False, {"pid": 12345})),
            patch("adapter.WSClient") as ws_client,
        ):
            result = await _standalone_send(config, "chat_1", "hello")

        assert "token already in use (PID 12345)" in result["error"]
        ws_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_standalone_send_releases_scoped_lock_after_send(self):
        from adapter import _standalone_send

        config = MagicMock()
        config.extra = {"bot_id": "bot_123", "secret": "secret_456"}
        client = MagicMock()
        client.connect = AsyncMock()
        client.send_message = AsyncMock()

        with (
            patch("adapter.acquire_scoped_lock", return_value=(True, None)),
            patch("adapter.release_scoped_lock") as release_lock,
            patch("adapter.WSClient", return_value=client),
            patch("adapter.asyncio.sleep", new=AsyncMock()),
        ):
            result = await _standalone_send(config, "chat_1", "hello")

        assert result["success"] is True
        client.connect.assert_awaited_once()
        client.send_message.assert_awaited_once()
        client.disconnect.assert_called_once()
        release_lock.assert_called_once_with("xwecom", "bot_123")


# ── SDK patch verification ──────────────────────────────────────────────────


class TestSDKPatches:
    """Verify SDK patches are applied correctly."""

    def test_reply_ack_timeout_configurable(self):
        from sdk.types import WSClientOptions

        opts = WSClientOptions(
            bot_id="test",
            secret="test",
            reply_ack_timeout=10.0,
        )
        assert opts.reply_ack_timeout == 10.0

    def test_reply_ack_timeout_default(self):
        from sdk.types import WSClientOptions

        opts = WSClientOptions(bot_id="test", secret="test")
        assert opts.reply_ack_timeout == 5.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
