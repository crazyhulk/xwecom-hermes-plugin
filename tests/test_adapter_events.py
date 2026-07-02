"""Adapter integration tests for WeCom event callbacks and state wiring."""

import asyncio
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _make_adapter():
    from adapter import XWeComAdapter
    from message_sender import NonBlockingStreamGate
    from monitor import DEFAULT_MESSAGE_PROCESS_TIMEOUT_S, SessionRecorder
    from state_manager import get_state_manager
    from template_card import TemplateCardCache

    with patch("adapter.BasePlatformAdapter.__init__", return_value=None):
        adapter = XWeComAdapter.__new__(XWeComAdapter)
    adapter._client = MagicMock()
    adapter._dedup = MagicMock()
    adapter._dedup.is_duplicate = MagicMock(return_value=False)
    adapter._dm_policy = "open"
    adapter._group_policy = "open"
    adapter._allow_from = []
    adapter._group_allow_from = []
    adapter._groups_config = {}
    adapter._last_chat_req_ids = {}
    adapter._last_chat_frames = {}
    adapter._stream_expired_chats = set()
    adapter._stream_turns = {}
    adapter._stream_gate = NonBlockingStreamGate()
    adapter._state = get_state_manager()
    adapter._account_id = "acc"
    adapter._session_recorder = SessionRecorder()
    adapter._template_card_cache = TemplateCardCache()
    adapter._message_timeout_s = DEFAULT_MESSAGE_PROCESS_TIMEOUT_S
    adapter._welcome_text = ""
    adapter._stream_keepalive_interval_s = 240.0
    adapter._stream_rotate_after_s = 300.0
    adapter._text_batch_delay_s = 0.0
    adapter._text_batch_split_delay_s = 0.0
    adapter._pending_text_batches = {}
    adapter._pending_text_batch_tasks = {}
    adapter._callback_enabled = False
    adapter._callback_apps = []
    adapter._callback_chat_apps = {}
    adapter._callback_seen_messages = {}
    adapter._callback_access_tokens = {}
    adapter._callback_http_session = None
    adapter.build_source = MagicMock(return_value={})
    adapter.handle_message = AsyncMock()
    return adapter


class TestAdapterEventCallbacks:
    @pytest.mark.asyncio
    async def test_auth_change_event_is_forwarded_to_hermes(self):
        adapter = _make_adapter()
        frame = {
            "headers": {"req_id": "REQ-AUTH"},
            "body": {
                "msgid": "EV1",
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "alice"},
                "event": {
                    "eventtype": "auth_change_event",
                    "auth_change_event": {"auth_list": [1, 2]},
                },
            },
        }

        await adapter._on_event(frame)

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert "auth_change_event" in event.text
        assert adapter._last_chat_req_ids["alice"] == "REQ-AUTH"
        assert adapter._state.get_reqid_for_chat("alice", "acc") == "REQ-AUTH"

    @pytest.mark.asyncio
    async def test_enter_chat_sends_welcome_and_does_not_dispatch(self):
        adapter = _make_adapter()
        adapter._welcome_text = "welcome"
        adapter._client.reply_welcome = AsyncMock(return_value={"errcode": 0})
        frame = {
            "headers": {"req_id": "REQ-ENTER"},
            "body": {
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "alice"},
                "event": {"eventtype": "enter_chat"},
            },
        }

        await adapter._on_event(frame)

        adapter._client.reply_welcome.assert_awaited_once()
        adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_disconnected_event_marks_displaced(self):
        adapter = _make_adapter()
        adapter._client.disconnect = MagicMock()
        frame = {
            "headers": {"req_id": "REQ-DISC"},
            "body": {
                "msgtype": "event",
                "from": {"userid": "alice"},
                "event": {"eventtype": "disconnected_event"},
            },
        }

        await adapter._on_event(frame)

        assert adapter._client is None
        assert adapter._state.get_connection_state("acc") == "displaced"


class TestAdapterTemplateCardOutbound:
    @pytest.mark.asyncio
    async def test_proactive_send_extracts_template_card(self):
        adapter = _make_adapter()
        sent = []

        async def send_message(chat_id, body):
            sent.append((chat_id, body))
            return {"errcode": 0}

        adapter._client.send_message = AsyncMock(side_effect=send_message)
        text = (
            "Before\n"
            "```json\n"
            '{"card_type": "text_notice", "main_title": {"title": "T"}}\n'
            "```\n"
            "After"
        )

        result = await adapter.send("chat1", text)

        assert result.success is True
        assert len(sent) == 2
        assert sent[0][1]["msgtype"] == "template_card"
        assert sent[1][1]["msgtype"] == "markdown"
        assert "```" not in sent[1][1]["markdown"]["content"]
        assert "Before" in sent[1][1]["markdown"]["content"]
        assert "After" in sent[1][1]["markdown"]["content"]

    @pytest.mark.asyncio
    async def test_stream_masks_intermediate_and_sends_card_on_final(self):
        adapter = _make_adapter()
        frame = {
            "headers": {"req_id": "REQ1"},
            "body": {"chatid": "chat1", "from": {"userid": "alice"}},
        }
        adapter._client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client.send_message = AsyncMock(return_value={"errcode": 0})
        adapter._last_chat_req_ids["chat1"] = "REQ1"
        adapter._last_chat_frames["chat1"] = frame

        await adapter.send_stream_frame("", chat_id="chat1")
        text = (
            f"{'Before. ' * 30}\n"
            "```json\n"
            '{"card_type": "text_notice", "main_title": {"title": "T"}}\n'
            "```\n"
            "After."
        )
        await adapter.send_stream_frame(text, chat_id="chat1")
        mid_content = adapter._client.reply_stream.await_args.args[2]
        assert "card_type" not in mid_content
        assert "正在生成卡片消息" in mid_content

        await adapter.send_stream_frame(text, chat_id="chat1", finalize=True)

        adapter._client.send_message.assert_awaited_once()
        card_body = adapter._client.send_message.await_args.args[1]
        assert card_body["msgtype"] == "template_card"
        final_content = adapter._client.reply_stream.await_args.args[2]
        assert "```" not in final_content
        assert "Before" in final_content
        assert "After" in final_content


class TestAdapterReplyMediaDirectives:
    def test_split_reply_media_directives(self):
        from adapter import XWeComAdapter

        visible, media_urls = XWeComAdapter._split_reply_media_from_text(
            "报告如下\n- FILE:/tmp/report.pdf\n1. MEDIA: `/tmp/a.png`\n结束"
        )

        assert visible == "报告如下\n结束"
        assert media_urls == ["/tmp/report.pdf", "/tmp/a.png"]

    @pytest.mark.asyncio
    async def test_proactive_send_consumes_media_directives(self):
        adapter = _make_adapter()
        adapter._client.send_message = AsyncMock(return_value={"errcode": 0})
        uploaded = []

        async def fake_upload(client, media_url, chat_id, **kwargs):
            uploaded.append((media_url, chat_id))
            return SimpleNamespace(ok=True)

        with patch("adapter.upload_and_send_media", side_effect=fake_upload):
            result = await adapter.send(
                "chat1",
                "文件已生成\nFILE:/tmp/report.pdf\nMEDIA: `/tmp/chart.png`",
            )

        assert result.success is True
        assert uploaded == [("/tmp/report.pdf", "chat1"), ("/tmp/chart.png", "chat1")]
        adapter._client.send_message.assert_awaited_once()
        body = adapter._client.send_message.await_args.args[1]
        assert body["markdown"]["content"] == "文件已生成"

    @pytest.mark.asyncio
    async def test_stream_final_consumes_media_directives_and_reports_failure(self):
        adapter = _make_adapter()
        frame = {
            "headers": {"req_id": "REQ1"},
            "body": {"chatid": "chat1", "from": {"userid": "alice"}},
        }
        adapter._client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client.send_message = AsyncMock(return_value={"errcode": 0})
        adapter._last_chat_req_ids["chat1"] = "REQ1"
        adapter._last_chat_frames["chat1"] = frame

        async def fake_upload(client, media_url, chat_id, **kwargs):
            return SimpleNamespace(ok=False, error="missing file")

        await adapter.send_stream_frame("", chat_id="chat1")
        with patch("adapter.upload_and_send_media", side_effect=fake_upload):
            await adapter.send_stream_frame(
                "请查收\nFILE:/tmp/missing.pdf",
                chat_id="chat1",
                finalize=True,
            )

        final_content = adapter._client.reply_stream.await_args.args[2]
        assert "FILE:" not in final_content
        assert "请查收" in final_content
        assert "文件发送失败" in final_content
        assert "missing file" in final_content


class TestAdapterCallbackInbound:
    @pytest.mark.asyncio
    async def test_build_event_from_callback_scopes_chat_and_records_app(self):
        from callback import ParsedCallbackMessage

        adapter = _make_adapter()
        app = {"name": "default", "corp_id": "wxCORP"}
        parsed = ParsedCallbackMessage(
            msg_id="MSG1",
            sender_id="alice",
            chat_id="alice",
            is_group_chat=False,
            text="hello",
        )

        event = await adapter._build_event_from_callback(app, parsed)

        assert event is not None
        assert event.text == "hello"
        assert event.message_id == "MSG1"
        adapter.build_source.assert_called_once()
        assert adapter.build_source.call_args.kwargs["chat_id"] == "wxCORP:alice"
        assert adapter._callback_chat_apps["wxCORP:alice"] == "default"

    @pytest.mark.asyncio
    async def test_callback_image_media_is_downloaded_and_cached(self):
        from callback import ParsedCallbackMessage

        adapter = _make_adapter()
        app = {
            "name": "default",
            "corp_id": "wxCORP",
            "corp_secret": "secret",
            "agent_id": "1000002",
        }
        parsed = ParsedCallbackMessage(
            msg_id="IMG1",
            sender_id="alice",
            chat_id="alice",
            is_group_chat=False,
            media_id="MEDIA1",
            media_type="image",
        )
        adapter._get_callback_access_token = AsyncMock(return_value="ACCESS")

        class FakeResponse:
            status = 200
            headers = {
                "content-type": "image/png",
                "content-disposition": 'attachment; filename="photo.png"',
            }

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def read(self):
                return b"\x89PNG\r\n\x1a\nimage-bytes"

        class FakeSession:
            closed = False

            def get(self, url, **kwargs):
                return FakeResponse()

        adapter._callback_http_session = FakeSession()

        with patch("adapter.cache_image_from_bytes", return_value="/tmp/callback.png"):
            event = await adapter._build_event_from_callback(app, parsed)

        assert event is not None
        assert event.media_urls == ["/tmp/callback.png"]
        assert event.media_types == ["image/png"]
        assert event.text == "[image消息]"

    def test_callback_deduplicator_uses_ttl(self):
        adapter = _make_adapter()

        with patch("adapter.time.time", return_value=1000):
            assert adapter._is_duplicate_callback("MSG1") is False
            assert adapter._is_duplicate_callback("MSG1") is True

        with patch("adapter.time.time", return_value=1401):
            assert adapter._is_duplicate_callback("MSG1") is False

    @pytest.mark.asyncio
    async def test_send_routes_callback_chat_to_agent_api(self):
        adapter = _make_adapter()
        app = {
            "name": "default",
            "corp_id": "wxCORP",
            "corp_secret": "secret",
            "agent_id": "1000002",
        }
        adapter._callback_apps = [app]
        adapter._callback_chat_apps = {"wxCORP:alice": "default"}
        adapter._get_callback_access_token = AsyncMock(return_value="ACCESS")
        post_calls = []

        class FakeResponse:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def json(self, content_type=None):
                return {"errcode": 0, "msgid": "OUT1"}

        class FakeSession:
            closed = False

            def post(self, url, **kwargs):
                post_calls.append((url, kwargs))
                return FakeResponse()

        adapter._callback_http_session = FakeSession()

        result = await adapter.send("wxCORP:alice", "hello")

        assert result.success is True
        assert result.message_id == "OUT1"
        assert post_calls[0][1]["params"] == {"access_token": "ACCESS"}
        assert post_calls[0][1]["json"]["touser"] == "alice"


class TestAdapterTextBatching:
    @pytest.mark.asyncio
    async def test_rapid_plain_text_events_are_merged(self):
        adapter = _make_adapter()
        adapter._text_batch_delay_s = 0.01
        adapter._text_batch_split_delay_s = 0.01

        event1 = SimpleNamespace(
            text="第一段",
            message_type="text",
            source={"chat_id": "chat1", "user_id": "alice"},
            message_id="M1",
            media_urls=[],
        )
        event2 = SimpleNamespace(
            text="第二段",
            message_type="text",
            source={"chat_id": "chat1", "user_id": "alice"},
            message_id="M2",
            media_urls=[],
        )

        await adapter._dispatch_event_to_hermes(event1)
        await adapter._dispatch_event_to_hermes(event2)
        await asyncio.sleep(0.03)

        adapter.handle_message.assert_awaited_once()
        merged = adapter.handle_message.await_args.args[0]
        assert merged.text == "第一段\n第二段"

    @pytest.mark.asyncio
    async def test_batched_dispatch_closes_session_after_flush(self):
        adapter = _make_adapter()
        adapter._text_batch_delay_s = 0.01
        adapter._text_batch_split_delay_s = 0.01
        adapter._session_recorder.close = AsyncMock()
        event = SimpleNamespace(
            text="第一段",
            message_type="text",
            source={"chat_id": "chat1", "user_id": "alice"},
            message_id="M1",
            media_urls=[],
        )

        handled_now = await adapter._dispatch_event_to_hermes(event)

        assert handled_now is False
        adapter._session_recorder.close.assert_not_awaited()
        await asyncio.sleep(0.03)
        adapter._session_recorder.close.assert_awaited_once_with("M1")

    @pytest.mark.asyncio
    async def test_media_event_bypasses_text_batch(self):
        adapter = _make_adapter()
        adapter._text_batch_delay_s = 10.0
        event = SimpleNamespace(
            text="图片",
            message_type="text",
            source={"chat_id": "chat1", "user_id": "alice"},
            message_id="M1",
            media_urls=["/tmp/image.png"],
        )

        await adapter._dispatch_event_to_hermes(event)

        adapter.handle_message.assert_awaited_once_with(event)
