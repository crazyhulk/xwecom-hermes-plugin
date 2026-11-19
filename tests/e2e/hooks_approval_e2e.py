"""E2E for the parts no single unit test covers: the tool timer fed by the REAL pre/post_tool_call hook
dispatch (on an agent-like worker thread with the gateway's session context), and an approval card click
resolving a REAL pending gateway approval."""
from __future__ import annotations

import asyncio
import contextvars
import os
import pathlib
import shutil
import tempfile
import threading

home = pathlib.Path(tempfile.mkdtemp(prefix="xw-home-"))
os.environ["HERMES_HOME"] = str(home)
(home / "plugins").mkdir()
shutil.copytree(pathlib.Path(__file__).resolve().parents[2] / "xwecom", home / "plugins" / "xwecom")
(home / "config.yaml").write_text("plugins:\n  enabled:\n    - xwecom\n")
os.environ.update(XWECOM_BOT_ID="bot", XWECOM_SECRET="sec")

from hermes_cli.plugins import discover_plugins  # noqa: E402
discover_plugins(force=True)
from hermes_cli.lifecycle import invoke_hook  # noqa: E402
from gateway.platform_registry import platform_registry  # noqa: E402
from gateway.config import PlatformConfig  # noqa: E402
from gateway import session_context  # noqa: E402

sent: list[dict] = []


async def tool_timer():
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={"bot_id": "bot", "secret": "sec"}))

    class _WS:
        closed = False

        async def send_json(self, payload):
            sent.append(payload)
            if payload["cmd"] == "aibot_respond_msg":
                async def ack():
                    await asyncio.sleep(0.005)
                    await adapter._dispatch_payload({"headers": {"req_id": payload["headers"]["req_id"]}, "errcode": 0, "errmsg": "ok"})
                asyncio.get_running_loop().create_task(ack())

    adapter._ws, adapter._running = _WS(), True
    adapter._remember_chat_req_id("user1", "REQ-1")
    adapter._start_ticker()
    import importlib
    hooks_mod = importlib.import_module(type(adapter).__module__.rsplit(".", 1)[0] + ".hooks")
    hooks_mod.attach(adapter)

    # Hermes' tool-progress bubble: an editable message.
    res = await adapter.send("user1", "🔧 terminal: `sleep 5`", metadata={"expect_edits": True})
    assert res.success, res

    # The agent runs the tool on a worker thread that copied the turn's context (session env bound).
    # It names an inbound message that has no turn bubble, so the legacy overlay timer draws (a hook with NO
    # message id is an internal turn's and opens its own turn bubble — tests/test_turn_internal.py).
    tokens = [session_context._SESSION_PLATFORM.set("xwecom"), session_context._SESSION_CHAT_ID.set("user1"),
              session_context._SESSION_MESSAGE_ID.set("m-without-turn")]
    ctx = contextvars.copy_context()

    def run_tool():
        ctx.run(invoke_hook, "pre_tool_call", tool_name="terminal", args={}, tool_call_id="call_1", session_id="s", task_id="t", turn_id="u", api_request_id="", conversation_id="")
    t = threading.Thread(target=run_tool)
    t.start(); t.join()
    await asyncio.sleep(4.2)  # timer shows after 2s and ticks every second
    frames = [p["body"]["stream"]["content"] for p in sent if p["body"].get("msgtype") == "stream"]
    timer_frames = [f for f in frames if "terminal " in f and ("s" in f.split("terminal ")[-1])]
    print("timer frames seen:", timer_frames[-3:])
    assert any(f.endswith(("terminal 2s", "terminal 3s", "terminal 4s")) for f in timer_frames), frames

    def finish_tool():
        ctx.run(invoke_hook, "post_tool_call", tool_name="terminal", args={}, result="ok", tool_call_id="call_1", session_id="s", task_id="t", turn_id="u", api_request_id="", conversation_id="", duration_ms=4000, status="ok", error_type=None, error_message=None, middleware_trace=[])
    t = threading.Thread(target=finish_tool)
    t.start(); t.join()
    await asyncio.sleep(1.3)
    last = [p["body"]["stream"]["content"] for p in sent if p["body"].get("msgtype") == "stream"][-1]
    print("after tool finished:", repr(last))
    assert "⠋" not in last and "terminal 4s" not in last and last.startswith("🔧 terminal"), last
    await adapter.edit_message("user1", res.message_id, "🔧 terminal: `sleep 5` ✓", finalize=True)
    for tok in reversed(tokens):
        tok.var.reset(tok)
    await adapter._stop_ticker()
    print("TOOL TIMER OK")


async def approval_card():
    adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra={"bot_id": "bot", "secret": "sec", "dm_policy": "open"}))

    class _WS:
        closed = False

        async def send_json(self, payload):
            sent.append(payload)
            if payload["cmd"] == "aibot_respond_msg":
                async def ack():
                    await asyncio.sleep(0.005)
                    await adapter._dispatch_payload({"headers": {"req_id": payload["headers"]["req_id"]}, "errcode": 0, "errmsg": "ok"})
                asyncio.get_running_loop().create_task(ack())

    adapter._ws, adapter._running = _WS(), True
    adapter._remember_chat_req_id("user1", "REQ-2")

    from tools import approval
    from tools.approval_gateway_wait import _ApprovalEntry
    entry = _ApprovalEntry({"command": "rm -rf /tmp/x", "request_id": "rq-1"})
    approval._gateway_queues.setdefault("sess-1", []).append(entry)

    res = await adapter.send_exec_approval("user1", "rm -rf /tmp/x", "sess-1", description="dangerous", request_id="rq-1")
    assert res.success, res
    card = next(p for p in sent if p["body"].get("msgtype") == "template_card")["body"]["template_card"]
    print("card buttons:", [(b["text"], b["key"]) for b in card["button_list"]])
    # A click arrives as an event callback for the card's task id.
    await adapter._dispatch_payload({"cmd": "aibot_event_callback", "headers": {"req_id": "EVT-1"}, "body": {
        "from": {"userid": "user1"}, "event": {"eventtype": "template_card_event",
                                               "template_card_event": {"task_id": card["task_id"], "event_key": "once"}}}})
    print("approval result:", entry.result, "| event set:", entry.event.is_set())
    assert entry.result == "once" and entry.event.is_set()
    update = next(p for p in sent if p["cmd"] == "aibot_respond_update_msg")
    print("card updated to:", [b["text"] for b in update["body"]["template_card"]["button_list"]])
    print("APPROVAL CARD OK")


async def main():
    await tool_timer()
    await approval_card()


try:
    asyncio.run(main())
finally:
    shutil.rmtree(home, ignore_errors=True)
