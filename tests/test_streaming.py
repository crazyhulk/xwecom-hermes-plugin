"""Tests for xwecom native streaming support.

Covers the ``send_stream_frame`` lifecycle that ``GatewayStreamConsumer``
exercises: seed → intermediate (via :class:`BlockChunker`) → finalize, plus
the failure modes (no req_id, expired stream, frame cap).
"""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Match the existing test suite's import style — sibling modules are
# imported by name; the conftest.py in this dir wires up the gateway
# stubs ``adapter`` needs.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _make_adapter():
    """Build an XWeComAdapter without running ``BasePlatformAdapter.__init__``.

    The Hermes runtime isn't here, so we bypass the base init and seed
    the streaming fields the consumer would otherwise rely on.
    """
    from adapter import XWeComAdapter
    from message_sender import NonBlockingStreamGate
    from monitor import DEFAULT_MESSAGE_PROCESS_TIMEOUT_S, SessionRecorder
    from state_manager import get_state_manager
    from template_card import TemplateCardCache

    with patch("adapter.BasePlatformAdapter.__init__", return_value=None):
        adapter = XWeComAdapter.__new__(XWeComAdapter)
    adapter._client = None
    adapter._last_chat_req_ids = {}
    adapter._last_chat_frames = {}
    adapter._reply_frames = {}
    adapter._stream_turns = {}
    adapter._stream_expired_chats = set()
    adapter._stream_gate = NonBlockingStreamGate()
    adapter._state = get_state_manager()
    adapter._account_id = "test"
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
    return adapter


def _bind_chat(adapter, chat_id="chat1", req_id="REQ1"):
    frame = {"headers": {"req_id": req_id}, "body": {}}
    adapter._last_chat_req_ids[chat_id] = req_id
    adapter._last_chat_frames[chat_id] = frame
    return frame


class TestSupportsNativeStreaming:
    """Advertise native streaming to Hermes runtimes that support the seam."""

    def test_class_attribute_is_true(self):
        from adapter import XWeComAdapter
        assert XWeComAdapter.SUPPORTS_NATIVE_STREAMING is True

    def test_instance_method_returns_true(self):
        adapter = _make_adapter()
        assert adapter.supports_native_streaming() is True
        assert adapter.supports_native_streaming(chat_type="group") is True
        assert adapter.supports_native_streaming(chat_type="dm") is True


class TestTextByteChunking:
    def test_split_text_by_byte_limit_preserves_content(self):
        from adapter import XWeComAdapter

        text = "第一行\n" + ("中文内容" * 20) + "\n最后一行"
        chunks = XWeComAdapter._split_text_by_byte_limit(text, 60)
        assert "".join(chunks) == text
        assert len(chunks) > 1
        assert all(len(chunk.encode("utf-8")) <= 60 for chunk in chunks)


class TestSendStreamFrameRejects:
    """Guard rails: bad inputs / missing state must return False fast so
    the consumer can fall back to :meth:`XWeComAdapter.send`.
    """

    async def test_missing_chat_id_returns_false(self):
        adapter = _make_adapter()
        ok = await adapter.send_stream_frame("hi", chat_id="")
        assert ok is False

    async def test_disconnected_returns_false(self):
        adapter = _make_adapter()
        adapter._client = None
        ok = await adapter.send_stream_frame("", chat_id="chat1")
        assert ok is False

    async def test_no_cached_req_id_returns_false(self):
        adapter = _make_adapter()
        adapter._client = MagicMock()  # connected, but no inbound bound
        ok = await adapter.send_stream_frame("", chat_id="chat1")
        assert ok is False

    async def test_finalize_without_open_turn_returns_false(self):
        adapter = _make_adapter()
        adapter._client = MagicMock()
        _bind_chat(adapter)
        ok = await adapter.send_stream_frame("done", chat_id="chat1", finalize=True)
        # Nothing was ever opened — there's nothing to finalize.
        assert ok is False


class TestSeedFrame:
    """First call (empty text) sends the WeCom thinking placeholder."""

    async def test_seed_calls_reply_stream_with_thinking_message(self):
        from message_sender import THINKING_MESSAGE

        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        frame = _bind_chat(adapter)

        ok = await adapter.send_stream_frame("", chat_id="chat1")
        assert ok is True
        assert client.reply_stream.await_count == 1
        args = client.reply_stream.await_args.args
        # Signature: (frame, stream_id, content, finish)
        assert args[0] is frame
        assert isinstance(args[1], str) and args[1].startswith("stream_")
        assert args[2] == THINKING_MESSAGE
        assert args[3] is False

    async def test_seed_marks_turn_seeded(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        await adapter.send_stream_frame("", chat_id="chat1")

        assert len(adapter._stream_turns) == 1
        turn = next(iter(adapter._stream_turns.values()))
        assert turn.seeded is True
        assert turn.finalized is False


class TestIntermediateFrames:
    """Chunker-gated cumulative-text emission."""

    async def test_long_cumulative_emits_one_intermediate_frame(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        # Seed first — consumer always does this.
        await adapter.send_stream_frame("", chat_id="chat1")
        assert client.reply_stream.await_count == 1

        # Now feed a chunk that crosses BLOCK_STREAM_MIN_CHARS *and*
        # contains sentence terminators — the chunker should emit.
        long_text = "这是一段足够长的文字。" * 30  # ~330 chars, sentence-aligned
        ok = await adapter.send_stream_frame(long_text, chat_id="chat1")
        assert ok is True
        assert client.reply_stream.await_count == 2
        args = client.reply_stream.await_args.args
        assert args[3] is False  # finish=False
        assert args[2] == long_text  # cumulative content

    async def test_short_text_holds_until_idle_flush(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        await adapter.send_stream_frame("", chat_id="chat1")
        # A few chars — chunker should not emit.
        ok = await adapter.send_stream_frame("hi", chat_id="chat1")
        assert ok is True
        # Only the seed frame so far.
        assert client.reply_stream.await_count == 1

    async def test_pending_ack_skips_intermediate_frame(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        await adapter.send_stream_frame("", chat_id="chat1")
        turn = next(iter(adapter._stream_turns.values()))
        await adapter._stream_gate.try_acquire(turn.stream_id, finish=False)

        client.reply_stream.reset_mock()
        long_text = "这是一段足够长的文字。" * 30
        ok = await adapter.send_stream_frame(long_text, chat_id="chat1")

        assert ok is True
        assert client.reply_stream.await_count == 0

        ok = await adapter.send_stream_frame(
            long_text + "结尾", chat_id="chat1", finalize=True
        )
        assert ok is True
        assert client.reply_stream.await_count == 1
        assert client.reply_stream.await_args.args[3] is True

    async def test_keepalive_replays_last_non_empty_content(self):
        adapter = _make_adapter()
        adapter._stream_keepalive_interval_s = 0.01
        adapter._stream_rotate_after_s = 0
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        await adapter.send_stream_frame("", chat_id="chat1")
        long_text = "这是一段足够长的文字。" * 30
        await adapter.send_stream_frame(long_text, chat_id="chat1")
        sent_before = client.reply_stream.await_count

        await asyncio.sleep(0.03)

        assert client.reply_stream.await_count > sent_before
        assert client.reply_stream.await_args.args[2] == long_text
        assert client.reply_stream.await_args.args[3] is False
        turn = next(iter(adapter._stream_turns.values()))
        turn.finalized = True
        adapter._cancel_keepalive(turn)
        await asyncio.sleep(0)

    async def test_rotation_finishes_old_stream_and_continues_with_delta(self):
        adapter = _make_adapter()
        adapter._stream_keepalive_interval_s = 0
        adapter._stream_rotate_after_s = 0.01
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)

        await adapter.send_stream_frame("", chat_id="chat1")
        first_visible = "第一段足够长的文字。" * 30
        await adapter.send_stream_frame(first_visible, chat_id="chat1")
        turn = next(iter(adapter._stream_turns.values()))
        old_stream_id = turn.stream_id

        await asyncio.sleep(0.03)

        turn = next(iter(adapter._stream_turns.values()))
        assert turn.stream_id != old_stream_id
        calls = [call.args for call in client.reply_stream.await_args_list]
        assert any(args[1] == old_stream_id and args[3] is True for args in calls)
        assert calls[-1][1] == turn.stream_id
        assert calls[-1][2] == "<think></think>"
        assert calls[-1][3] is False

        client.reply_stream.reset_mock()
        second_visible = first_visible + ("第二段足够长的文字。" * 30)
        await adapter.send_stream_frame(second_visible, chat_id="chat1")

        assert client.reply_stream.await_count == 1
        assert "第一段" not in client.reply_stream.await_args.args[2]
        assert "第二段" in client.reply_stream.await_args.args[2]


class TestFinalize:
    """The closing ``finish=True`` frame."""

    async def test_finalize_sends_finish_true_and_clears_turn(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        await adapter.send_stream_frame("", chat_id="chat1")  # seed

        ok = await adapter.send_stream_frame(
            "final cumulative text", chat_id="chat1", finalize=True,
        )
        assert ok is True
        last = client.reply_stream.await_args.args
        assert last[3] is True  # finish=True
        # Turn cleaned up so the next inbound message starts fresh.
        assert adapter._stream_turns == {}

    async def test_finalize_truncation_sends_full_content_fallback(self):
        from constants import MAX_STREAM_CONTENT_LENGTH

        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        client.send_message = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        await adapter.send_stream_frame("", chat_id="chat1")

        full_text = "开头内容\n" + ("中间内容。" * 5000) + "\n结尾内容"
        ok = await adapter.send_stream_frame(
            full_text, chat_id="chat1", finalize=True
        )

        assert ok is True
        final_content = client.reply_stream.await_args.args[2]
        assert len(final_content.encode("utf-8")) <= MAX_STREAM_CONTENT_LENGTH
        assert "结尾内容" in final_content
        assert "开头内容" not in final_content
        assert client.send_message.await_count > 0
        fallback_text = client.send_message.await_args_list[0].args[1]["markdown"]["content"]
        assert "完整回复" in fallback_text
        assert "开头内容" in fallback_text


class TestFrameCap:
    """Past ``MAX_INTERMEDIATE_FRAMES`` we silently drop intermediates
    and let the finalize frame carry the rest."""

    async def test_intermediate_dropped_after_cap(self):
        from constants import MAX_INTERMEDIATE_FRAMES

        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        await adapter.send_stream_frame("", chat_id="chat1")  # seed

        # Force the turn to the cap as if we already sent that many frames.
        turn = next(iter(adapter._stream_turns.values()))
        turn.frame_count = MAX_INTERMEDIATE_FRAMES

        client.reply_stream.reset_mock()
        client.reply_stream.return_value = {"errcode": 0}

        long_text = "这是一段够长的文字。" * 60
        ok = await adapter.send_stream_frame(long_text, chat_id="chat1")
        assert ok is True  # consumer keeps going; we just don't ship
        assert client.reply_stream.await_count == 0


class TestStreamExpiredErrcode:
    """WeCom returns 846608 when no update has happened for 6 minutes.
    We mark the chat expired and refuse new turns until a fresh inbound
    message arrives (handled in ``_on_message``)."""

    async def test_expired_errcode_marks_chat_and_returns_false(self):
        from constants import STREAM_EXPIRED_ERRCODE

        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": STREAM_EXPIRED_ERRCODE})
        adapter._client = client
        _bind_chat(adapter)

        ok = await adapter.send_stream_frame("", chat_id="chat1")
        assert ok is False
        assert "chat1" in adapter._stream_expired_chats

    async def test_expired_chat_blocks_new_turn(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)
        adapter._stream_expired_chats.add("chat1")

        ok = await adapter.send_stream_frame("", chat_id="chat1")
        assert ok is False
        # We never even tried to send.
        assert client.reply_stream.await_count == 0


class TestOnMessageBindsReqId:
    """The streaming code depends on ``_on_message`` having cached the
    most recent inbound req_id + frame.  Verify the binding happens."""

    async def test_on_message_stores_chat_to_req_id(self):
        adapter = _make_adapter()
        # Patch out the rest of the pipeline so we can drive _on_message
        # directly without a connected gateway / SDK.
        adapter._dedup = MagicMock()
        adapter._dedup.is_duplicate = MagicMock(return_value=False)
        adapter._dm_policy = "open"
        adapter._allow_from = []
        adapter._client = MagicMock()
        adapter.handle_message = AsyncMock()
        adapter.build_source = MagicMock(return_value={})

        frame = {
            "headers": {"req_id": "REQ-XYZ"},
            "body": {
                "msgid": "M1",
                "chattype": "single",
                "from": {"userid": "alice"},
                "msgtype": "text",
                "text": {"content": "hello"},
            },
        }
        await adapter._on_message(frame)

        # DM falls back to user_id as chat_id.
        assert adapter._last_chat_req_ids.get("alice") == "REQ-XYZ"
        assert adapter._last_chat_frames.get("alice") is frame


# ── BlockChunker.drain (simplified API) ──────────────────────────────────────

class TestBlockChunkerDrainAPI:
    """Tests for the drain(cumulative_text) method used by the finalize path."""

    def test_drain_returns_text_when_pending(self):
        from stream import BlockChunker

        c = BlockChunker()
        result = c.drain("hello world")
        assert result == "hello world"

    def test_drain_returns_none_when_empty(self):
        from stream import BlockChunker

        c = BlockChunker()
        assert c.drain("") is None

    def test_drain_returns_none_after_already_emitted(self):
        from stream import BlockChunker

        c = BlockChunker()
        c.mark_emitted("hello")
        assert c.drain("hello") is None

    def test_drain_returns_new_content_after_partial_emit(self):
        from stream import BlockChunker

        c = BlockChunker()
        c.mark_emitted("first part")
        result = c.drain("first part — and more")
        assert result == "first part — and more"

    def test_drain_advances_emitted_len(self):
        from stream import BlockChunker

        c = BlockChunker()
        c.drain("first part")
        assert c.emitted_length == len("first part")
        # New content after drain
        result = c.drain("first part — and more")
        assert result == "first part — and more"
        assert c.emitted_length == len("first part — and more")

    def test_drain_returns_none_for_shorter_text(self):
        from stream import BlockChunker

        c = BlockChunker()
        c.drain("long content here")
        # Shorter text (anomaly) — should return None
        assert c.drain("short") is None


class TestFinalizeWithChunker:
    """End-to-end: finalize path no longer crashes when chunker has pending content."""

    @pytest.mark.asyncio
    async def test_finalize_drains_chunker_pending(self):
        """adapter.send_stream_frame(finalize=True) should not AttributeError."""
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(return_value={"errcode": 0})
        adapter._client = client
        _bind_chat(adapter)

        # Seed
        ok = await adapter.send_stream_frame("", chat_id="chat1")
        assert ok is True

        # Push an intermediate frame that creates the chunker
        intermediate_text = "A" * 200  # above min_chars so it emits
        ok = await adapter.send_stream_frame(
            intermediate_text, chat_id="chat1"
        )
        assert ok is True

        # Now finalize — this used to crash with AttributeError
        full_text = intermediate_text + " — and the conclusion."
        ok = await adapter.send_stream_frame(
            full_text, finalize=True, chat_id="chat1"
        )
        assert ok is True

    @pytest.mark.asyncio
    async def test_finalize_preserves_non_timeout_send_failure(self):
        adapter = _make_adapter()
        client = MagicMock()
        client.reply_stream = AsyncMock(
            side_effect=[
                {"errcode": 0},
                {"errcode": 40001, "errmsg": "invalid request"},
            ]
        )
        adapter._client = client
        _bind_chat(adapter)

        assert await adapter.send_stream_frame("", chat_id="chat1") is True

        ok = await adapter.send_stream_frame(
            "complete answer",
            finalize=True,
            chat_id="chat1",
        )

        assert ok is False
        assert adapter._stream_turns == {}
