"""Tests for monitor module — buffered dispatcher, enter_chat, disconnected, sessions."""

import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from monitor import (
    BufferedBlockDispatcher,
    EnterChatContext,
    SessionRecord,
    SessionRecorder,
    handle_disconnected_event,
    handle_enter_chat_event,
    run_with_message_timeout,
)


def run(coro):
    return asyncio.run(coro)


# ── BufferedBlockDispatcher ────────────────────────────────────────────────


class TestBufferedBlockDispatcher:
    def test_single_message_flushed_after_window(self):
        received = []

        async def handler(batch):
            received.append(batch)

        async def _go():
            disp = BufferedBlockDispatcher(handler, window_s=0.05, flush_max_s=1.0)
            await disp.submit("c", {"id": 1})
            await asyncio.sleep(0.15)

        run(_go())
        assert len(received) == 1
        assert received[0][0]["id"] == 1

    def test_burst_coalesced_into_one_flush(self):
        received = []

        async def handler(batch):
            received.append(batch)

        async def _go():
            disp = BufferedBlockDispatcher(handler, window_s=0.08, flush_max_s=1.0)
            await disp.submit("c", {"id": 1})
            await disp.submit("c", {"id": 2})
            await disp.submit("c", {"id": 3})
            await asyncio.sleep(0.20)

        run(_go())
        assert len(received) == 1
        ids = [m["id"] for m in received[0]]
        assert ids == [1, 2, 3]

    def test_different_chats_isolated(self):
        received = []

        async def handler(batch):
            received.append(batch)

        async def _go():
            disp = BufferedBlockDispatcher(handler, window_s=0.05, flush_max_s=1.0)
            await disp.submit("c1", {"id": 1})
            await disp.submit("c2", {"id": 2})
            await asyncio.sleep(0.15)

        run(_go())
        assert len(received) == 2

    def test_force_flush_when_max_exceeded(self):
        received = []

        async def handler(batch):
            received.append(batch)

        async def _go():
            disp = BufferedBlockDispatcher(handler, window_s=10.0, flush_max_s=0.05)
            await disp.submit("c", {"id": 1})
            await asyncio.sleep(0.10)
            await disp.submit("c", {"id": 2})
            await asyncio.sleep(0.10)

        run(_go())
        # Both submits should land somewhere — at least 1 flush happened
        assert len(received) >= 1
        ids_seen = [m["id"] for batch in received for m in batch]
        assert 1 in ids_seen
        assert 2 in ids_seen

    def test_flush_all_drains(self):
        received = []

        async def handler(batch):
            received.append(batch)

        async def _go():
            disp = BufferedBlockDispatcher(handler, window_s=10.0, flush_max_s=10.0)
            await disp.submit("c", {"id": 1})
            await disp.flush_all()

        run(_go())
        assert received and received[0][0]["id"] == 1

    def test_handler_error_does_not_propagate(self):
        async def handler(batch):
            raise RuntimeError("nope")

        async def _go():
            disp = BufferedBlockDispatcher(handler, window_s=0.02, flush_max_s=1.0)
            await disp.submit("c", {"id": 1})
            await asyncio.sleep(0.06)

        # Should not raise
        run(_go())


# ── handle_enter_chat_event ────────────────────────────────────────────────


class _FakeWelcomeClient:
    def __init__(self, *, has_reply_welcome=True, reply_welcome_fails=False):
        self.has_reply_welcome = has_reply_welcome
        self.reply_welcome_fails = reply_welcome_fails
        self.welcome_calls = []
        self.send_calls = []
        if has_reply_welcome:
            async def reply_welcome(frame, body):
                if self.reply_welcome_fails:
                    raise RuntimeError("fail")
                self.welcome_calls.append((frame, body))
                return {"errcode": 0}

            self.reply_welcome = reply_welcome

    async def send_message(self, chat_id, body):
        self.send_calls.append((chat_id, body))
        return {"errcode": 0}


class TestHandleEnterChat:
    def test_non_enter_event_returns_false(self):
        frame = {"body": {"event": {"eventtype": "template_card_event"}}}
        out = run(
            handle_enter_chat_event(frame, _FakeWelcomeClient(), welcome_text="hi")
        )
        assert out is False

    def test_no_welcome_text_returns_false(self):
        frame = {
            "body": {
                "from": {"userid": "u"},
                "event": {"eventtype": "enter_chat"},
            }
        }
        out = run(handle_enter_chat_event(frame, _FakeWelcomeClient()))
        assert out is False

    def test_uses_reply_welcome_when_available(self):
        client = _FakeWelcomeClient(has_reply_welcome=True)
        frame = {
            "body": {
                "chatid": "C",
                "from": {"userid": "u"},
                "event": {"eventtype": "enter_chat"},
            }
        }
        out = run(handle_enter_chat_event(frame, client, welcome_text="welcome!"))
        assert out is True
        assert len(client.welcome_calls) == 1

    def test_falls_back_to_send_message_on_failure(self):
        client = _FakeWelcomeClient(has_reply_welcome=True, reply_welcome_fails=True)
        frame = {
            "body": {
                "chatid": "C",
                "from": {"userid": "u"},
                "event": {"eventtype": "enter_chat"},
            }
        }
        out = run(handle_enter_chat_event(frame, client, welcome_text="welcome!"))
        assert out is True
        assert len(client.send_calls) == 1

    def test_welcome_builder_can_return_dynamic(self):
        client = _FakeWelcomeClient(has_reply_welcome=True)
        frame = {
            "body": {
                "chatid": "C",
                "from": {"userid": "alice"},
                "event": {"eventtype": "enter_chat"},
            }
        }

        def builder(ctx: EnterChatContext) -> str:
            return f"hi {ctx.user_id}"

        out = run(handle_enter_chat_event(frame, client, welcome_builder=builder))
        assert out is True
        assert client.welcome_calls[0][1]["markdown"]["content"] == "hi alice"


# ── handle_disconnected_event ──────────────────────────────────────────────


class _FakeDisconnectingClient:
    def __init__(self):
        self.disconnect_called = False

    def disconnect(self):
        self.disconnect_called = True


class TestHandleDisconnected:
    def test_non_match_returns_false(self):
        frame = {"body": {"event": {"eventtype": "other"}}}
        out = run(handle_disconnected_event(frame, _FakeDisconnectingClient()))
        assert out is False

    def test_invokes_disconnect_and_kicked_cb(self):
        client = _FakeDisconnectingClient()
        kicked_reasons = []

        async def on_kicked(reason):
            kicked_reasons.append(reason)

        frame = {"body": {"event": {"eventtype": "disconnected_event"}}}
        out = run(handle_disconnected_event(frame, client, on_kicked=on_kicked))
        assert out is True
        assert client.disconnect_called is True
        assert kicked_reasons and "Kicked" in kicked_reasons[0]


# ── SessionRecorder ────────────────────────────────────────────────────────


class TestSessionRecorder:
    def test_open_and_get(self):
        rec = SessionRecorder()
        record = SessionRecord(
            chat_id="c",
            user_id="u",
            message_id="m",
            req_id="r",
            stream_id="s",
        )
        run(rec.open(record))
        assert run(rec.get("m")) is record

    def test_close_marks_finished(self):
        rec = SessionRecorder()
        record = SessionRecord(
            chat_id="c", user_id="u", message_id="m", req_id="r", stream_id="s"
        )
        run(rec.open(record))
        run(rec.close("m"))
        got = run(rec.get("m"))
        assert got.status == "finished"
        assert got.finished_at is not None

    def test_close_with_error(self):
        rec = SessionRecorder()
        record = SessionRecord(
            chat_id="c", user_id="u", message_id="m", req_id="r", stream_id="s"
        )
        run(rec.open(record))
        run(rec.close("m", error="boom"))
        got = run(rec.get("m"))
        assert got.status == "failed"
        assert got.last_error == "boom"

    def test_active_returns_processing_only(self):
        rec = SessionRecorder()
        r1 = SessionRecord(
            chat_id="c", user_id="u", message_id="a", req_id="r", stream_id="s"
        )
        r2 = SessionRecord(
            chat_id="c", user_id="u", message_id="b", req_id="r", stream_id="s"
        )
        run(rec.open(r1))
        run(rec.open(r2))
        run(rec.close("a"))
        active = run(rec.active())
        ids = [r.message_id for r in active]
        assert ids == ["b"]


# ── run_with_message_timeout ──────────────────────────────────────────────


class TestRunWithMessageTimeout:
    def test_success_returns_value(self):
        async def _go():
            return 42

        out = run(run_with_message_timeout(_go(), timeout_s=1.0))
        assert out == 42

    def test_timeout_triggers_callback_then_raises(self):
        called = []

        async def _slow():
            await asyncio.sleep(0.5)
            return 1

        async def _on_timeout():
            called.append("hit")

        async def _go():
            with pytest.raises(asyncio.TimeoutError):
                await run_with_message_timeout(
                    _slow(), timeout_s=0.05, on_timeout=_on_timeout
                )

        run(_go())
        assert called == ["hit"]
