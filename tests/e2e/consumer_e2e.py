"""E2E: Hermes' REAL GatewayStreamConsumer drives the REAL xwecom adapter (loaded through Hermes plugin
discovery) against a simulated WeCom server that behaves like the measured API.

Checks what the user would actually see in WeCom — not internal state.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import tempfile

home = pathlib.Path(tempfile.mkdtemp(prefix="xw-home-"))
os.environ["HERMES_HOME"] = str(home)
(home / "plugins").mkdir()
shutil.copytree(pathlib.Path(__file__).resolve().parents[2] / "xwecom", home / "plugins" / "xwecom")
(home / "config.yaml").write_text("plugins:\n  enabled:\n    - xwecom\n")
os.environ.update(XWECOM_BOT_ID="bot", XWECOM_SECRET="sec")

from hermes_cli.plugins import discover_plugins  # noqa: E402
discover_plugins(force=True)
from gateway.platform_registry import platform_registry  # noqa: E402
from gateway.config import PlatformConfig  # noqa: E402
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig  # noqa: E402


class FakeWeComServer:
    """Screen model of the measured API: a finished stream acks updates but never renders them; a stream
    rejects every frame with 846608 once 600s old (monotonic clock offset is injectable)."""

    def __init__(self):
        self.screen: dict[str, str] = {}
        self.order: list[str] = []
        self.finished: set[str] = set()
        self.markdown: list[str] = []
        self.frames = 0

    def visible(self):
        return [self.screen[s].replace("​", "") for s in self.order] + [f"[md] {m}" for m in self.markdown]

    async def handle(self, adapter, payload):
        cmd, req_id, body = payload["cmd"], payload["headers"]["req_id"], payload.get("body") or {}
        ack = {"headers": {"req_id": req_id}, "errcode": 0, "errmsg": "ok"}
        if cmd == "aibot_respond_msg" and body.get("msgtype") == "stream":
            self.frames += 1
            st = body["stream"]
            if st["id"] not in self.finished:
                if st["id"] not in self.screen:
                    self.order.append(st["id"])
                self.screen[st["id"]] = st["content"]
                if st["finish"]:
                    self.finished.add(st["id"])
        elif cmd in ("aibot_respond_msg", "aibot_send_msg") and body.get("msgtype") == "markdown":
            self.markdown.append(body["markdown"]["content"])
        await asyncio.sleep(0.005)  # ack latency
        await adapter._dispatch_payload(ack)


async def scenario(name, steps):
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={"bot_id": "bot", "secret": "sec"}))
    server = FakeWeComServer()

    class _WS:
        closed = False

        async def send_json(self, payload):
            asyncio.get_running_loop().create_task(server.handle(adapter, payload))

    adapter._ws = _WS()
    adapter._running = True
    adapter._remember_chat_req_id("user1", "REQ-1")  # an inbound message from user1 arrived
    consumer = GatewayStreamConsumer(adapter, "user1", StreamConsumerConfig(edit_interval=0.05, buffer_threshold=1, cursor=" ▉", transport="auto", chat_type="dm"))
    task = asyncio.create_task(consumer.run())
    await steps(consumer, adapter)
    consumer.finish()
    await asyncio.wait_for(task, 10)
    await asyncio.sleep(0.1)
    print(f"── {name}: {server.frames} frames, delivered={consumer.final_content_delivered}")
    for i, v in enumerate(server.visible()):
        print(f"   bubble {i + 1}: {v!r}")
    return server, consumer


async def main():
    async def simple(c, a):
        for chunk in ["你好", "，这是", "一段", "流式回复。"]:
            c.on_delta(chunk)
            await asyncio.sleep(0.08)

    s, c = await scenario("simple streamed reply", simple)
    assert s.visible() == ["你好，这是一段流式回复。"], s.visible()
    assert c.final_content_delivered

    async def with_tool(c, a):
        c.on_delta("先查一下。")
        await asyncio.sleep(0.15)
        c.on_delta(None)  # tool boundary
        await asyncio.sleep(0.15)
        c.on_delta("查到了：结果是 42。")
        await asyncio.sleep(0.15)

    s, c = await scenario("reply → tool boundary → reply", with_tool)
    assert "".join(s.visible()).replace(" ▉", "") .count("先查一下。") == 1
    assert all("▉" not in v for v in s.visible()), "cursor left in a sealed bubble"
    assert all(sid in s.finished for sid in s.order), "a bubble was left open (would spin forever)"

    async def interrupted(c, a):
        c.on_delta("说到一半")
        await asyncio.sleep(0.15)
        from gateway.platforms.base import ProcessingOutcome
        ev = type("E", (), {"source": type("S", (), {"chat_id": "user1"})()})()
        await a.on_processing_complete(ev, ProcessingOutcome.FAILURE)

    s, c = await scenario("turn ends before finalize", interrupted)
    assert all(sid in s.finished for sid in s.order), "interrupted turn left a bubble open"

    print("\nALL E2E CHECKS PASSED")


try:
    asyncio.run(main())
finally:
    shutil.rmtree(home, ignore_errors=True)
