"""The ack-tracked wire (``xwecom/streaming.py``) against a fake connection that acks every frame IN SEND
ORDER after a delay — what the live WeCom API does (measured: one ack per frame, final or not, carrying
only the req_id). Asserts what reaches the wire."""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
import types

_ROOT = pathlib.Path(__file__).resolve().parents[1] / "xwecom"
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_messages import messages  # noqa: E402

_pkg = types.ModuleType("xw_wire"); _pkg.__path__ = [str(_ROOT)]  # noqa: E702
sys.modules["xw_wire"] = _pkg
sys.modules["xw_wire.messages"] = messages
_spec = importlib.util.spec_from_file_location("xw_wire.streaming", _ROOT / "streaming.py")
streaming = importlib.util.module_from_spec(_spec)
sys.modules["xw_wire.streaming"] = streaming
_spec.loader.exec_module(streaming)


class Conn(streaming.StreamWireMixin):
    """An adapter's wire half over a fake socket. Acks come back in order, each ``ack_delay`` after its
    frame; ``drop_ack`` (by send index) delays that ack past every timeout (it still arrives, late)."""
    name = "xw-test"

    def __init__(self, ack_delay=0.01, late_ack=(), late_by=0.0):
        self._reply_queues = {}
        self.sent: list[dict] = []
        self.ack_delay, self.late_ack, self.late_by = ack_delay, set(late_ack), late_by
        self._acks = asyncio.Queue()
        self._pump = None

    def _require_ws(self):
        pass

    async def _send_json(self, payload):
        index = len(self.sent)
        self.sent.append(payload)
        delay = self.ack_delay + (self.late_by if index in self.late_ack else 0.0)
        due = asyncio.get_running_loop().time() + delay
        await self._acks.put((due, payload["headers"]["req_id"]))
        if self._pump is None:
            self._pump = asyncio.get_running_loop().create_task(self._deliver())

    async def _deliver(self):
        while True:  # in order: an ack never overtakes an earlier frame's ack
            due, req_id = await self._acks.get()
            await asyncio.sleep(max(0.0, due - asyncio.get_running_loop().time()))
            (getattr(self, "_dispatch_ack", None) or self._resolve_reply_ack)(req_id, {"headers": {"req_id": req_id}, "errcode": 0, "errmsg": "ok"})

    def frames(self, stream_id):
        return [p["body"]["stream"]["content"] for p in self.sent if p["body"]["stream"]["id"] == stream_id]


def run(coro):
    return asyncio.run(coro)


def test_a_late_ack_never_stops_the_live_bubble_from_drawing():
    """Live 15:17: a confirmed frame on another stream timed out (its ack came ~5s late); the old design
    then poisoned the req_id and skipped every later intermediate frame: the turn bubble froze."""
    async def go():
        conn = Conn(ack_delay=0.01, late_ack={1}, late_by=0.5)
        conn._REPLY_ACK_TIMEOUT = 0.1
        await conn.stream_frame("R", "turn", "⠋ 🔧 web_search (0s)", finish=False)
        outcome = await conn.stream_frame("R", "other", "🔍 Searching…", finish=True)  # ack late
        for i in range(1, 4):
            await conn.stream_frame("R", "turn", f"⠙ 🔧 web_search ({i}s)", finish=False)
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.6)
        return conn, outcome
    conn, outcome = run(go())
    assert conn.frames("turn") == ["⠋ 🔧 web_search (0s)", "⠙ 🔧 web_search (1s)", "⠙ 🔧 web_search (2s)",
                                   "⠙ 🔧 web_search (3s)"]
    assert outcome.ok  # its ack did arrive (on the retry or late): not a failure
    assert not conn._reply_queues  # every ack matched a frame; nothing left dangling


def test_intermediate_frames_never_wait_for_or_depend_on_acks():
    """No ack ever comes. Every redraw still goes out immediately (pre-migration native stream behaviour)."""
    async def go():
        conn = Conn(ack_delay=60.0)
        start = asyncio.get_running_loop().time()
        outcomes = [await conn.stream_frame("R", "a", f"frame {i}", finish=False) for i in range(5)]
        return conn, outcomes, asyncio.get_running_loop().time() - start
    conn, outcomes, waited = run(go())
    assert waited < 0.2 and all(o.ok for o in outcomes)
    assert conn.frames("a") == [f"frame {i}" for i in range(5)]


def test_finish_frames_on_two_streams_resolve_in_send_order():
    async def go():
        conn = Conn(ack_delay=0.02)
        tasks = [asyncio.create_task(conn.stream_frame("R", "a", "a1", finish=False)),
                 asyncio.create_task(conn.stream_frame("R", "b", "b-final", finish=True)),
                 asyncio.create_task(conn.stream_frame("R", "a", "a-final", finish=True))]
        return conn, await asyncio.gather(*tasks)
    conn, outcomes = run(go())
    assert [p["body"]["stream"]["content"] for p in conn.sent] == ["a1", "b-final", "a-final"]
    assert all(o.ok for o in outcomes) and not conn._reply_queues


def test_a_finish_frame_without_an_ack_is_indeterminate_not_failed():
    async def go():
        conn = Conn(ack_delay=60.0)
        conn._REPLY_ACK_TIMEOUT = 0.05
        return await conn.stream_frame("R", "a", "answer", finish=True)
    outcome = run(go())
    assert outcome.ok and outcome.indeterminate  # counts as delivered, as before the migration


def test_dead_req_id_on_a_finish_frame_is_reported():
    async def go():
        conn = Conn()

        async def dead_ack():
            while True:
                due, req_id = await conn._acks.get()
                await asyncio.sleep(0.01)
                conn._resolve_reply_ack(req_id, {"headers": {"req_id": req_id}, "errcode": 846604, "errmsg": "x"})
        conn._pump = asyncio.get_running_loop().create_task(dead_ack())
        return await conn.stream_frame("R", "a", "answer", finish=True)
    outcome = run(go())
    assert outcome.req_dead and not outcome.ok


def test_an_ack_with_nothing_waiting_is_unrouted():
    conn = Conn()
    assert conn._resolve_reply_ack("R", {"errcode": 0}) is False


class Adapterish(Conn):
    """``adapter._send_reply_request`` (the passive markdown / media / approval-card path) bound onto the
    fake wire: the real method, read from xwecom/adapter.py, so the test exercises the code that ships."""
    _pending_responses: dict = {}

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._pending_responses = {}

    async def _request(self, cmd, req_id, body, timeout):  # adapter._request, verbatim semantics
        future = self._pending_responses[req_id] = asyncio.get_running_loop().create_future()
        try:
            await self._send_json({"cmd": cmd, "headers": {"req_id": req_id}, "body": body})
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending_responses.pop(req_id, None)

    def _dispatch_ack(self, req_id, payload):  # adapter._dispatch_payload's ack routing, verbatim order
        if self._resolve_reply_ack(req_id, payload):
            return
        future = self._pending_responses.get(req_id)
        if future is not None and not future.done():
            future.set_result(payload)

    @staticmethod
    def _require_reply_req_id(reply_req_id):
        return str(reply_req_id)


def _bind_real_send_reply_request():
    import ast
    src = (_ROOT / "adapter.py").read_text()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_send_reply_request")
    code = ast.get_source_segment(src, fn)
    ns = {"asyncio": asyncio, "Any": object, "APP_CMD_RESPONSE": streaming.APP_CMD_RESPONSE,
          "REQUEST_TIMEOUT_SECONDS": 15.0}
    import textwrap
    lines = code.splitlines()
    exec("from typing import Any\n" + lines[0] + "\n" + textwrap.dedent("\n".join(lines[1:])).replace("\n", "\n    ").join(["    ", ""]), ns)
    Adapterish._send_reply_request = ns["_send_reply_request"]


def test_a_passive_reply_takes_its_own_ack_so_a_finish_frame_still_sees_a_dead_req_id():
    """F4: an approval card / passive markdown on the turn's req_id, then the turn's finish frame. Acks come
    back in order: the card's ok, then 846604 for the finish. The finish must see the 846604 — not have
    its ack taken by the card's correlation."""
    _bind_real_send_reply_request()

    async def go():
        conn = Adapterish()
        codes = iter([0, 846604])

        async def pump():
            while True:
                due, req_id = await conn._acks.get()
                await asyncio.sleep(0.01)
                conn._dispatch_ack(req_id, {"headers": {"req_id": req_id}, "errcode": next(codes), "errmsg": "x"})
        conn._pump = asyncio.get_running_loop().create_task(pump())
        card = asyncio.create_task(conn._send_reply_request("R", {"msgtype": "template_card"}, timeout=1.0))
        await asyncio.sleep(0)
        finish = asyncio.create_task(conn.stream_frame("R", "turn", "answer", finish=True))
        return await card, await finish
    conn_timeout = streaming.StreamWireMixin._REPLY_ACK_TIMEOUT
    streaming.StreamWireMixin._REPLY_ACK_TIMEOUT = 0.5
    try:
        card, finish = run(go())
    finally:
        streaming.StreamWireMixin._REPLY_ACK_TIMEOUT = conn_timeout
    assert card["errcode"] == 0
    assert finish.req_dead
