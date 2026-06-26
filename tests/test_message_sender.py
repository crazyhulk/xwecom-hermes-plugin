"""Tests for message_sender — stream gate, classification, timeout, thinking."""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from constants import STREAM_EXPIRED_ERRCODE, STREAM_NOT_SUBSCRIBED_ERRCODE
from message_sender import (
    NonBlockingStreamGate,
    REPLY_SEND_TIMEOUT_S,
    STREAM_LIFETIME_S,
    StreamLifetimeWatcher,
    StreamNotSubscribedError,
    THINKING_MESSAGE,
    classify_stream_error,
    send_thinking_reply,
    send_we_com_reply,
    send_we_com_reply_non_blocking,
)
from stream import StreamExpiredError


# ── Helpers ────────────────────────────────────────────────────────────────


class _FakeClient:
    """Mock ws_client supporting send_message + reply_stream."""

    def __init__(self, *, errcode=0, errmsg="", raises=None, delay=0.0):
        self.errcode = errcode
        self.errmsg = errmsg
        self.raises = raises
        self.delay = delay
        self.sent = []
        self.streams = []

    async def reply_stream(self, frame, stream_id, content, finish):
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.raises:
            raise self.raises
        self.streams.append({"stream_id": stream_id, "content": content, "finish": finish})
        return {"errcode": self.errcode, "errmsg": self.errmsg}

    async def send_message(self, chat_id, body):
        if self.raises:
            raise self.raises
        self.sent.append({"chat_id": chat_id, "body": body})
        return {"errcode": self.errcode, "errmsg": self.errmsg}


def run(coro):
    return asyncio.run(coro)


# ── classify_stream_error ──────────────────────────────────────────────────


class TestClassifyStreamError:
    def test_classify_846608_by_attribute(self):
        class E(Exception):
            errcode = STREAM_EXPIRED_ERRCODE

        out = classify_stream_error(E("nope"))
        assert isinstance(out, StreamExpiredError)

    def test_classify_846609_by_attribute(self):
        class E(Exception):
            errcode = STREAM_NOT_SUBSCRIBED_ERRCODE

        out = classify_stream_error(E("nope"))
        assert isinstance(out, StreamNotSubscribedError)

    def test_classify_by_message_substring(self):
        e = Exception(f"server returned errcode {STREAM_EXPIRED_ERRCODE}")
        assert isinstance(classify_stream_error(e), StreamExpiredError)

    def test_unknown_returns_none(self):
        assert classify_stream_error(RuntimeError("generic")) is None


# ── send_we_com_reply ──────────────────────────────────────────────────────


class TestSendWeComReply:
    def test_empty_text_returns_empty(self):
        client = _FakeClient()
        out = run(
            send_we_com_reply(
                client,
                {"headers": {"req_id": "r"}, "body": {}},
                text="",
                stream_id="s1",
            )
        )
        assert out == ""
        assert client.streams == []

    def test_normal_call_returns_stream_id(self):
        client = _FakeClient()
        out = run(
            send_we_com_reply(
                client,
                {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                text="hi",
                stream_id="s1",
            )
        )
        assert out == "s1"
        assert client.streams[0]["content"] == "hi"

    def test_event_uses_send_message(self):
        client = _FakeClient()
        out = run(
            send_we_com_reply(
                client,
                {"headers": {"req_id": "r"}, "body": {"msgtype": "event", "chatid": "C"}},
                text="hi",
                stream_id="s1",
                finish=True,
            )
        )
        assert out == "s1"
        assert client.sent[0]["chat_id"] == "C"
        assert client.streams == []

    def test_event_non_final_skips(self):
        client = _FakeClient()
        out = run(
            send_we_com_reply(
                client,
                {"headers": {"req_id": "r"}, "body": {"msgtype": "event", "chatid": "C"}},
                text="hi",
                stream_id="s1",
                finish=False,
            )
        )
        assert out == "s1"
        # Neither sent nor streamed for non-final event frame
        assert client.streams == []
        assert client.sent == []

    def test_846608_response_raises_stream_expired(self):
        client = _FakeClient(errcode=STREAM_EXPIRED_ERRCODE, errmsg="expired")
        with pytest.raises(StreamExpiredError):
            run(
                send_we_com_reply(
                    client,
                    {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                    text="hi",
                    stream_id="s1",
                )
            )

    def test_846609_response_raises_not_subscribed(self):
        client = _FakeClient(errcode=STREAM_NOT_SUBSCRIBED_ERRCODE, errmsg="dropped")
        with pytest.raises(StreamNotSubscribedError):
            run(
                send_we_com_reply(
                    client,
                    {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                    text="hi",
                    stream_id="s1",
                )
            )

    def test_raised_846608_is_reclassified(self):
        class E(Exception):
            errcode = STREAM_EXPIRED_ERRCODE

        client = _FakeClient(raises=E("boom"))
        with pytest.raises(StreamExpiredError):
            run(
                send_we_com_reply(
                    client,
                    {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                    text="hi",
                    stream_id="s1",
                )
            )

    def test_timeout_raises_timeout_error(self):
        client = _FakeClient(delay=1.0)
        with pytest.raises(asyncio.TimeoutError):
            run(
                send_we_com_reply(
                    client,
                    {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                    text="hi",
                    stream_id="s1",
                    timeout=0.05,
                )
            )


# ── NonBlockingStreamGate ──────────────────────────────────────────────────


class TestNonBlockingStreamGate:
    def test_first_call_acquires(self):
        gate = NonBlockingStreamGate()
        assert run(gate.try_acquire("s1", finish=False)) is True

    def test_second_call_while_in_flight_skips(self):
        gate = NonBlockingStreamGate()
        assert run(gate.try_acquire("s1", finish=False)) is True
        # Not released — second intermediate should be skipped
        assert run(gate.try_acquire("s1", finish=False)) is False

    def test_release_allows_next(self):
        gate = NonBlockingStreamGate()
        run(gate.try_acquire("s1", finish=False))
        run(gate.release("s1"))
        assert run(gate.try_acquire("s1", finish=False)) is True

    def test_final_frame_always_passes(self):
        gate = NonBlockingStreamGate()
        run(gate.try_acquire("s1", finish=False))  # in-flight
        # Final must still acquire
        assert run(gate.try_acquire("s1", finish=True)) is True


class TestSendWeComReplyNonBlocking:
    def test_skipped_when_in_flight(self):
        client = _FakeClient(delay=0.05)
        gate = NonBlockingStreamGate()

        async def _go():
            # Pre-mark in flight without releasing
            await gate.try_acquire("s1", finish=False)
            res = await send_we_com_reply_non_blocking(
                client,
                {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                text="x",
                stream_id="s1",
                gate=gate,
                finish=False,
            )
            return res

        assert run(_go()) == "skipped"

    def test_final_frame_always_sends(self):
        client = _FakeClient()
        gate = NonBlockingStreamGate()

        async def _go():
            await gate.try_acquire("s1", finish=False)  # simulate in-flight
            res = await send_we_com_reply_non_blocking(
                client,
                {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                text="bye",
                stream_id="s1",
                gate=gate,
                finish=True,
            )
            return res

        assert run(_go()) == "s1"


# ── send_thinking_reply ────────────────────────────────────────────────────


class TestThinkingReply:
    def test_sends_thinking_placeholder(self):
        client = _FakeClient()
        run(
            send_thinking_reply(
                client,
                {"headers": {"req_id": "r"}, "body": {"msgtype": "text"}},
                stream_id="s",
            )
        )
        assert client.streams[0]["content"] == THINKING_MESSAGE
        assert client.streams[0]["finish"] is False


# ── StreamLifetimeWatcher ──────────────────────────────────────────────────


class TestStreamLifetimeWatcher:
    def test_default_lifetime_is_6_min(self):
        assert STREAM_LIFETIME_S == 360.0

    def test_is_expired_false_initially(self):
        w = StreamLifetimeWatcher(lifetime_s=1.0)
        w.mark_start("s")
        assert w.is_expired("s") is False

    def test_remaining_decreases(self):
        w = StreamLifetimeWatcher(lifetime_s=10.0)
        w.mark_start("s")
        assert 0 < w.remaining("s") <= 10.0

    def test_cancel_removes(self):
        w = StreamLifetimeWatcher(lifetime_s=10.0)
        w.mark_start("s")
        w.cancel("s")
        assert w.remaining("s") == 10.0  # back to default

    def test_unknown_stream_not_expired(self):
        w = StreamLifetimeWatcher(lifetime_s=0.5)
        assert w.is_expired("never-started") is False
