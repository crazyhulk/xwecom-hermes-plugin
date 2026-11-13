"""The "full-capability IM" face the adapter shows to Hermes.

Hermes picks its richest delivery path for an adapter that can edit: edit-based streaming, an editable
tool-progress bubble, finalize edits. WeCom cannot edit, so this mixin emulates it on top of reply
streams (``messages.MessageBook``):

* ``send(..., metadata={"expect_edits": True})`` opens a live bubble and returns its virtual id; a plain
  ``send`` is a finished bubble on the same reply when one is available, else a proactive markdown send.
* ``edit_message(id, text, finalize=)`` re-renders that bubble (and rotates / continues it as needed).
* ``on_processing_complete`` seals whatever the turn left open (an interrupted turn never finalizes).
* the tool timer redraws an overlay — ``⠹ terminal 12s`` — under the live bubble of the chat that is
  running a tool, between Hermes' own edits (fed by ``pre_tool_call`` / ``post_tool_call`` hooks).
* exec approvals render as a WeCom ``button_interaction`` template card; a click resolves the request
  through the published ``resolve_gateway_approval_card`` and updates the card in place.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from gateway.platforms.base import ExecApprovalPrompt, SendResult

from .messages import MessageBook
from .streaming import APP_CMD_RESPONSE_UPDATE

logger = logging.getLogger(__name__)

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
TIMER_TICK_SECONDS = 1.0
TIMER_SHOW_AFTER_SECONDS = 2.0  # quick tools never flash a timer
ROTATION_CHECK_SECONDS = 15.0
# Cursors Hermes appends to interim stream bodies (default " ▉"); stripped when the plugin seals a bubble
# itself. A custom ``streaming.cursor`` is read from config at startup.
_STREAM_CURSORS: tuple[str, ...] = (" ▉", "▉")


@dataclass
class _ToolRun:
    name: str
    started: float


@dataclass
class _ChatActivity:
    tools: dict[str, _ToolRun] = field(default_factory=dict)  # tool_call_id -> run


def _fmt_elapsed(seconds: float) -> str:
    seconds = int(seconds)
    return f"{seconds}s" if seconds < 60 else f"{seconds // 60}m{seconds % 60:02d}s"


class EmulatedEditingMixin:
    """Needs from the adapter: ``_cached_reply_req_id``, ``_send_json``, ``_send_reply_request``,
    ``_send_markdown`` (non-stream fallback), ``name``, ``config``."""

    SUPPORTS_MESSAGE_EDITING = True
    REQUIRES_EDIT_FINALIZE = True  # every bubble must be closed with finish=true

    def _init_emulation(self) -> None:
        self._book = MessageBook(self)
        self._activity: dict[str, _ChatActivity] = {}
        self._approval_cards: dict[str, dict[str, Any]] = {}  # task_id -> card state
        self._ticker_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ── lifecycle ───────────────────────────────────────────────────────
    def _start_ticker(self) -> None:
        # Tool hooks run on agent worker threads and hop onto this loop (hooks._dispatch).
        self._loop = asyncio.get_running_loop()
        if self._ticker_task is None or self._ticker_task.done():
            self._ticker_task = asyncio.create_task(self._ticker())

    async def _stop_ticker(self) -> None:
        task, self._ticker_task = self._ticker_task, None
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _ticker(self) -> None:
        """One loop drives both the tool-timer redraw and the 10-minute bubble rotation."""
        last_rotation_check = 0.0
        while True:
            await asyncio.sleep(TIMER_TICK_SECONDS)
            try:
                await self._redraw_timers()
                now = time.monotonic()
                if now - last_rotation_check >= ROTATION_CHECK_SECONDS:
                    last_rotation_check = now
                    await self._book.rotate_due()
                    self._book.forget_closed()
            except Exception as exc:  # a tick must never kill the loop
                logger.debug("[%s] ticker error: %s", self.name, exc)

    # ── Hermes edit surface ─────────────────────────────────────────────
    def prefers_fresh_final_streaming(self, content: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        return False  # a "fresh final" would need delete_message, which WeCom does not have

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[dict[str, Any]] = None) -> SendResult:
        if not chat_id:
            return SendResult(success=False, error="chat_id is required")
        metadata = dict(metadata or {})
        expect_edits = bool(metadata.get("expect_edits"))
        req_id = None if metadata.get("force_proactive_send") else self._cached_reply_req_id(chat_id, reply_to or metadata.get("reply_to_message_id"))
        if req_id and (expect_edits or not metadata.get("is_approval_prompt")):
            msg = await self._book.create(chat_id, req_id, content, finalize=not expect_edits)
            if msg is not None:
                return SendResult(success=True, message_id=msg.message_id)
            logger.info("[%s] stream bubble unavailable for chat %s; falling back to markdown", self.name, chat_id)
        return await self._send_markdown(chat_id, content, reply_to=reply_to, metadata=metadata)

    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False,
                           metadata: Optional[dict[str, Any]] = None) -> SendResult:
        msg = self._book.get(message_id)
        if msg is None:
            return SendResult(success=False, error="unknown message (not an editable xwecom bubble)")
        if not await self._book.update(msg, content, finalize=finalize):
            return SendResult(success=False, error="WeCom stream unavailable")
        return SendResult(success=True, message_id=message_id)

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        return False  # WeCom bots cannot recall a message

    async def on_processing_complete(self, event: Any, outcome: Any) -> None:
        """The turn is over: drop its tool timers and seal any bubble it left open."""
        chat_id = str(getattr(getattr(event, "source", None), "chat_id", "") or "")
        self._activity.pop(chat_id, None)
        for msg in self._book.open_messages(chat_id=chat_id):
            await self._book.close(msg, strip_suffixes=_STREAM_CURSORS)
        parent = getattr(super(), "on_processing_complete", None)
        if parent is not None:
            await parent(event, outcome)

    # ── tool timer ──────────────────────────────────────────────────────
    def tool_started(self, chat_id: str, tool_call_id: str, tool_name: str) -> None:
        if chat_id:
            self._activity.setdefault(chat_id, _ChatActivity()).tools[tool_call_id or uuid.uuid4().hex] = _ToolRun(tool_name or "tool", time.monotonic())

    def tool_finished(self, chat_id: str, tool_call_id: str) -> None:
        activity = self._activity.get(chat_id)
        if activity is not None:
            activity.tools.pop(tool_call_id, None)

    def _overlay_for(self, chat_id: str, now: float) -> str:
        activity = self._activity.get(chat_id)
        if not activity or not activity.tools:
            return ""
        frame = SPINNER[int(now) % len(SPINNER)]
        lines = [f"{frame} {run.name} {_fmt_elapsed(now - run.started)}"
                 for run in activity.tools.values() if now - run.started >= TIMER_SHOW_AFTER_SECONDS]
        return "\n".join(lines)

    async def _redraw_timers(self) -> None:
        now = time.monotonic()
        for chat_id in list(self._activity):
            overlay = self._overlay_for(chat_id, now)
            # Draw on the newest open bubble of the chat (the tool-progress bubble while tools run).
            bubbles = self._book.open_messages(chat_id=chat_id)
            if bubbles:
                await self._book.set_overlay(bubbles[-1], overlay)
            if not overlay and not (self._activity.get(chat_id) and self._activity[chat_id].tools):
                self._activity.pop(chat_id, None)

    # ── exec approval card ──────────────────────────────────────────────
    _CARD_STYLE = {"primary": 1, "danger": 3}

    async def _send_exec_approval_prompt(self, prompt: ExecApprovalPrompt) -> SendResult:
        req_id = self._cached_reply_req_id(prompt.chat_id, None)
        if not req_id:
            return SendResult(success=False, error="no reply context for an approval card")
        task_id = f"hermes_ap_{uuid.uuid4().hex[:12]}"
        card = {
            "card_type": "button_interaction",
            "main_title": {"title": "⚠️ Hermes", "desc": prompt.text[:512]},
            "task_id": task_id,
            "button_list": [{"text": label, "style": self._CARD_STYLE.get(style, 2), "key": choice}
                            for label, choice, style in prompt.actions],
        }
        try:
            response = await self._send_reply_request(req_id, {"msgtype": "template_card", "template_card": card})
        except Exception as exc:
            return SendResult(success=False, error=f"approval card failed: {exc}")
        if response.get("errcode") not in (0, None):
            return SendResult(success=False, error=f"approval card rejected: {response.get('errmsg')}")
        self._approval_cards[task_id] = {"session_key": prompt.session_key, "request_id": prompt.request_id,
                                         "chat_id": prompt.chat_id, "card": card, "labels": {c: l for l, c, _ in prompt.actions}}
        return SendResult(success=True, message_id=task_id)

    async def _on_template_card_event(self, payload: dict[str, Any]) -> bool:
        """Button click → resolve the approval it was rendered for, then grey the card out. True if handled."""
        body = payload.get("body") or {}
        event = (body.get("event") or {}).get("template_card_event") or {}
        task_id, choice = str(event.get("task_id") or ""), str(event.get("event_key") or "")
        state = self._approval_cards.get(task_id)
        if state is None:
            return False
        clicker = str((body.get("from") or {}).get("userid") or "")
        resolved = 0
        try:
            from tools.approval import resolve_gateway_approval_card
            resolved = resolve_gateway_approval_card(state["session_key"], choice, state["request_id"],
                                                     actor_id=clicker or None, extra=self.config.extra)
        except Exception as exc:
            logger.error("[%s] approval resolve failed: %s", self.name, exc)
        if resolved:
            self._approval_cards.pop(task_id, None)
        label = state["labels"].get(choice, choice) if resolved else "已失效 / expired"
        card = dict(state["card"], button_list=[{"text": f"✓ {label}", "style": 2, "key": "done"}],
                    replace_text=f"✓ {label}")
        req_id = str((payload.get("headers") or {}).get("req_id") or "")
        try:
            await self._send_json({"cmd": APP_CMD_RESPONSE_UPDATE, "headers": {"req_id": req_id},
                                   "body": {"response_type": "update_template_card", "template_card": card,
                                            **({"userids": [clicker]} if clicker else {})}})
        except Exception as exc:
            logger.debug("[%s] approval card update failed: %s", self.name, exc)
        logger.info("[%s] approval click task=%s choice=%s resolved=%d by=%s", self.name, task_id, choice, resolved, clicker)
        return True
