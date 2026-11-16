"""E2E: the turn bubble under a Chinese UI (``display.language: zh``). The adapter reads core's status
templates through the real ``agent.i18n`` for the active language, so the zh heartbeat / busy ack fold into
the turn bubble while a zh-looking plugin post that is not a status keeps its own message.

Runs in its own process: the language must be set in config.yaml before the adapter (and its compiled
status patterns) exists.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import tempfile

home = pathlib.Path(tempfile.mkdtemp(prefix="xw-home-zh-"))
os.environ["HERMES_HOME"] = str(home)
(home / "plugins").mkdir()
shutil.copytree(pathlib.Path(__file__).resolve().parents[2] / "xwecom", home / "plugins" / "xwecom")
(home / "config.yaml").write_text("display:\n  language: zh\nplugins:\n  enabled:\n    - xwecom\n")
os.environ.update(XWECOM_BOT_ID="bot", XWECOM_SECRET="sec")

from hermes_cli.plugins import discover_plugins  # noqa: E402
discover_plugins(force=True)
from agent.i18n import get_language, t  # noqa: E402
from gateway.platform_registry import platform_registry  # noqa: E402
from gateway.config import PlatformConfig  # noqa: E402
from gateway.platforms.base import MessageEvent, MessageType, ProcessingOutcome  # noqa: E402
from gateway.run_busy import GatewayBusySessionMixin  # noqa: E402
from gateway.platforms.base import _reply_anchor_for_event  # noqa: E402


class Screen:
    def __init__(self):
        self.screen, self.order, self.finished, self.markdown, self.history = {}, [], set(), [], {}

    def visible(self):
        return [self.screen[s].replace("​", "") for s in self.order] + [f"[md] {m}" for m in self.markdown]

    async def handle(self, adapter, payload):
        cmd, req_id, body = payload["cmd"], payload["headers"]["req_id"], payload.get("body") or {}
        if cmd == "aibot_respond_msg" and body.get("msgtype") == "stream":
            st = body["stream"]
            if st["id"] not in self.finished:
                if st["id"] not in self.screen:
                    self.order.append(st["id"])
                self.screen[st["id"]] = st["content"]
                self.history.setdefault(st["id"], []).append(st["content"])
                if st["finish"]:
                    self.finished.add(st["id"])
        elif body.get("msgtype") == "markdown":
            self.markdown.append(body["markdown"]["content"])
        await asyncio.sleep(0.005)
        await adapter._dispatch_payload({"headers": {"req_id": req_id}, "errcode": 0, "errmsg": "ok"})


class BusyRunner(GatewayBusySessionMixin):
    _reply_anchor_for_event = staticmethod(_reply_anchor_for_event)

    def _thread_metadata_for_source(self, source, reply_to_message_id=None):
        return None


async def main():
    assert get_language() == "zh", get_language()
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={"bot_id": "bot", "secret": "sec"}))
    server = Screen()

    class _WS:
        closed = False

        async def send_json(self, payload):
            asyncio.get_running_loop().create_task(server.handle(adapter, payload))

    adapter._ws, adapter._running = _WS(), True
    adapter._start_ticker()
    for mid, req in (("m1", "REQ-1"), ("m2", "REQ-2")):
        adapter._reply_req_ids[mid] = req
        adapter._remember_chat_req_id("user1", req)
    src = adapter.build_source(chat_id="user1", chat_type="dm", user_id="user1", user_name="user1", message_id="m1")
    adapter._remember_inbound_session("m1", src)
    adapter._remember_inbound_session("m2", src)
    e1 = MessageEvent(text="你好", message_type=MessageType.TEXT, source=src, message_id="m1")
    e2 = MessageEvent(text="换个问题", message_type=MessageType.TEXT, source=src, message_id="m2")

    await adapter.on_processing_start(e1)
    adapter.llm_request_started("user1", "m1")
    await asyncio.sleep(0.2)
    heartbeat = t("gateway.progress.working_heartbeat", minutes=3, detail=" — 第 1/90 轮，terminal")
    ack = t("gateway.busy.ack", head=t("gateway.busy.redirected_head"),
            detail=t("gateway.busy.ack_detail_wrap", detail="已用时 3 分钟"), tail=t("gateway.busy.redirected_tail"))
    post = "⏳ 正在处理——部署失败，请立即运行 rollback.sh"  # a plugin post, not a core status
    hb = await adapter.send("user1", heartbeat, metadata={"_interim_send": True})
    await BusyRunner()._send_busy_reply(e2, adapter, ack)
    own = await adapter.send("user1", post, metadata={"_interim_send": True})
    await asyncio.sleep(1.2)
    frames = server.history[server.order[0]]
    await adapter.send("user1", "这是回答。", reply_to="m2", metadata={"notify": True})
    await adapter.on_processing_complete(e1, ProcessingOutcome.SUCCESS)
    await asyncio.sleep(0.3)
    await adapter._stop_ticker()

    print("zh heartbeat:", heartbeat)
    print("zh busy ack: ", ack)
    for i, v in enumerate(server.visible()):
        print(f"   bubble {i + 1}: {v!r}")
    assert hb.message_id.startswith("xwt_") and not own.message_id.startswith("xwt_")
    assert any(heartbeat in f and ack in f for f in frames), "zh heartbeat + busy ack must show in the turn bubble"
    shown = server.visible()
    assert "这是回答。" in shown and post in shown, shown  # the answer lands in the turn bubble; the post is its own
    assert not any(heartbeat in v or ack in v for v in shown), "transient status left on screen"
    assert all(s in server.finished for s in server.order)
    print("\nZH TURN-BUBBLE E2E PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        shutil.rmtree(home, ignore_errors=True)
