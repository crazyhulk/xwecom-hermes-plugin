"""The turn bubble: one WeCom bubble per inbound message that the turn's reply, its status area (thinking /
tools, ``turn.py``) and Hermes' own status lines share.

Routing is deliberately conservative — a send joins a turn bubble only when it provably belongs to that
turn; anything else keeps the pre-turn path (its own bubble, else markdown). An extra bubble is harmless;
content merged into another user's turn, folded into a transient line, or dropped is not.

Which sends join a turn (``_send_kind``):

* **reply** (a body segment): anchored (``reply_to`` / ``reply_to_message_id``) on the turn's opener or an
  inbound message folded into it, and not marked ``_interim_send``. That is the stream consumer (it always
  carries ``reply_to_message_id``) and the base adapter's final send (``reply_to`` = the event's reply
  anchor, which a redirect moves onto the folded message).
* **status** (one status line): a known core status send — the long-running heartbeat, the stuck warning,
  the approval-timeout notice, the drain notices and the busy acks (redirect / queue / steer / interrupt) —
  recognised by matching the text against core's own i18n templates for exactly those keys
  (``turn.STATUS_KEYS`` / ``BUSY_HEADS`` / ``BUSY_TAILS``), not by its shape. Any other ``_interim_send``
  (clarify prose, a full command before an approval card, a plugin post, an answer tail, commentary) is
  content: it keeps its own message, so it can never be dropped.
* a busy ack (``run_busy._send_busy_reply``) is unmarked and anchored on the NEW inbound message; when its
  text is a busy-ack template and that message's session has a live turn, the message is folded into the
  turn (a redirected answer anchored on it then lands in the same bubble).
* approval prompts, ``force_proactive_send``, unanchored sends (cron, ``send_message``), sends anchored on
  an unknown or other-session message, and marked sends anchored elsewhere (``/status`` while busy) are
  never the turn's.

**Internal turns.** Core injects a background-process completion as an ``internal`` event with no message
id and admits it without the adapter lifecycle: no ``on_processing_start`` / ``on_processing_complete``, hooks
with an empty message id, every send unanchored. Such a turn still gets ONE bubble (``_open_internal_turn``):

* it OPENS only from a model call whose ``user_message`` carries core's internal-notification marker
  (``compat.is_internal_wake``), keyed by that hook's session key and ``turn_id``. Nothing else opens one —
  not a tool hook, not a model call without the marker (the background review fork and subagent
  auto-resumes run with the same empty message id and session key), not a status / progress lane (they run
  with no session context), not an unanchored reply (cron delivers unanchored with the same markers).
* hooks, lanes and unanchored replies JOIN a live internal turn of the chat; once it has settled text, a hook
  carrying another ``turn_id`` (the review fork) is ignored.
* it goes on the chat's cached req_id (none: no turn, the old path), draws nothing until it has a tool line or
  text, never draws "💭 Thinking", and closes by settling (finished text, then quiet), or — never drawn —
  ``UNDRAWN_INTERNAL_IDLE`` after its last event, its notices then sent the old way.
* any new user turn of its session hands it over, which closes it at once (nothing else ever would).
* caveat: while it is live, a cron / ``send_message`` delivery that is unanchored and marked
  (``notify``) to the same chat joins it — they cannot be told apart from its own reply.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from typing import Any, Optional

from gateway.platforms.base import SendResult

from . import rescue
from .compat import core_send_lane, core_templates, source_session_key, tool_line_prefixes
from .turn import BUSY_HEADS, BUSY_TAILS, STATUS_KEYS, Turn, busy_ack_pattern, matches_status, template_pattern

logger = logging.getLogger(__name__)

# Cursors Hermes appends to interim stream bodies (default " ▉"); stripped from the shown body.
STREAM_CURSORS: tuple[str, ...] = (" ▉", "▉")
OPEN_AFTER_SECONDS = 3.0  # a turn with no model call / tool / send by then still gets its thinking bubble
TURN_IDLE_TIMEOUT = 40 * 60.0  # no event and no running tool for longer than the agent's own 30-min timeout
# A turn whose last event was finished reply text (a complete send or a finalize edit) with no model
# call or tool after it closes this soon: the backstop when no on_processing_complete arrives.
SETTLED_IDLE_TIMEOUT = 20.0
# A complete (notify) reply is sealed this soon if nothing (model call, tool, more text) follows it: its finish
# frame is the only one whose ack is awaited, so this is what bounds how long an answer sent into a dead
# req_id stays invisible before the rescue (G2). Core's final send comes last in a turn, so a short quiet
# window rarely cuts a turn that still has work to do — and if it does, the turn reopens a continuation.
SETTLE_AFTER_FINAL_SECONDS = 3.0
MAX_INBOUND_SESSIONS = 1000
# An internal turn that drew nothing (no tool, no text) this long after its last event closes; any notice it
# collected goes out on its own. Long enough for a slow first model call, short enough not to hold the session.
UNDRAWN_INTERNAL_IDLE = 60.0
LASTING_STATUS_KEYS = ("gateway.exec_approval.timed_out_notice",)  # still true after the turn: kept in the tail


def _anchor(reply_to: Optional[str], metadata: dict[str, Any]) -> str:
    return str(reply_to or metadata.get("reply_to_message_id") or "")


def _status_patterns() -> tuple[list["re.Pattern[str]"], list["re.Pattern[str]"], list["re.Pattern[str]"]]:
    """(status, lasting, busy-ack) patterns from core's templates (active language + English)."""
    status = [template_pattern(t) for key in STATUS_KEYS for t in core_templates(key)]
    lasting = [template_pattern(t) for key in LASTING_STATUS_KEYS for t in core_templates(key)]
    heads = [t for key in BUSY_HEADS for t in core_templates(f"gateway.busy.{key}")]
    tails = [t for key in BUSY_TAILS for t in core_templates(f"gateway.busy.{key}")]
    return status, lasting, [busy_ack_pattern(heads, tails)] if heads else []


class TurnBubbleMixin:
    """Needs from the adapter: ``_book`` (MessageBook), ``_cached_reply_req_id``, ``_reply_req_ids``,
    ``_send_outside_turn``, ``name``."""

    def _init_turns(self) -> None:
        self._turns: dict[str, list[Turn]] = {}  # chat_id -> turns (oldest first)
        self._turn_ids: dict[str, tuple[Turn, Optional[int]]] = {}  # fake id -> (turn, segment | None=status)
        self._inbound_sessions: dict[str, str] = {}  # inbound msg id -> adapter session key
        self._pending_inbound: dict[str, Any] = {}  # inbound msg id -> MessageEvent seen at intake
        self._closed_openers: dict[str, None] = {}  # message ids whose turn already closed (no reopen), bounded
        self._status_re, self._lasting_re, self._busy_ack_re = _status_patterns()

    def _now(self) -> float:
        """The turn clock: the message book's (monotonic in production, injectable in tests)."""
        return self._book._clock()

    # ── session identity ────────────────────────────────────────────────
    def _remember_inbound_session(self, message_id: str, source: Any) -> None:
        """Called at intake: which session an inbound message belongs to (chat, + sender in per-user
        group sessions). Lets a busy ack anchored on it find the right turn in a shared group."""
        if not message_id:
            return
        self._inbound_sessions[message_id] = source_session_key(self, source)
        while len(self._inbound_sessions) > MAX_INBOUND_SESSIONS:
            self._inbound_sessions.pop(next(iter(self._inbound_sessions)))

    def _live_turns(self, chat_id: str) -> list[Turn]:
        return [t for t in self._turns.get(chat_id, ()) if not t.closed]

    def _newest_of_session(self, chat_id: str, session: str) -> Optional[Turn]:
        return next((t for t in reversed(self._live_turns(chat_id)) if t.session == session), None)

    def _turn_for_message(self, chat_id: str, message_id: str) -> Optional[Turn]:
        """The live turn *message_id* opened, else the one it was folded into."""
        live = self._live_turns(chat_id)
        return (next((t for t in reversed(live) if t.opener_id == message_id), None)
                or next((t for t in reversed(live) if message_id in t.aliases), None)) if message_id else None

    def _sole_session_turn(self, chat_id: str) -> Optional[Turn]:
        """The newest live turn when every live turn of the chat is one session's; None when ambiguous."""
        live = self._live_turns(chat_id)
        return live[-1] if live and len({t.session for t in live}) == 1 else None

    def _session_of_message(self, chat_id: str, message_id: str) -> Optional[str]:
        if not message_id:
            return None
        session = self._inbound_sessions.get(message_id)
        if session is None:
            owner = self._turn_for_message(chat_id, message_id)
            session = owner.session if owner is not None else None
        return session

    def _turn_for_hook(self, chat_id: str, message_id: str, session: str = "") -> Optional[Turn]:
        """Hooks name the message whose context the agent runs under. A queued follow-up keeps the
        chain's first message id, so route to the NEWEST live turn of that message's session. With no
        message id (an internal turn) the hook's session key names it."""
        known = self._session_of_message(chat_id, message_id)
        if known is not None:
            return self._newest_of_session(chat_id, known)
        if not message_id and session:
            return self._newest_of_session(chat_id, session)
        return self._sole_session_turn(chat_id)

    def _open_internal_turn(self, chat_id: str, session: str, turn_id: str) -> Optional[Turn]:
        """A new internal turn bubble (see the module doc) for a woken session with no live turn; None when the
        session is unknown or the chat has no cached req_id."""
        if not session or self._newest_of_session(chat_id, session) is not None:
            return None
        req_id = self._cached_reply_req_id(chat_id, None)
        if not req_id:
            logger.info("[%s] no internal turn bubble: no req_id for chat=%s", self.name, chat_id)
            return None
        now = self._now()
        turn = Turn(chat_id=chat_id, opener_id=f"internal:{uuid.uuid4().hex[:12]}", session=session,
                    msg=self._book.register(chat_id, req_id), started=now, thinking_since=now, internal=True,
                    turn_id=turn_id, last_activity=now)
        self._turns.setdefault(chat_id, []).append(turn)
        logger.info("[%s] internal turn opened chat=%s req_id=%s session=%s turn_id=%s", self.name, chat_id, req_id,
                    session, turn_id)
        return turn

    @staticmethod
    def _foreign_to(turn: Turn, turn_id: str) -> bool:
        """A hook of another run (the review fork, a subagent resume) reaching an internal turn whose text
        already settled: it must not restart the turn."""
        return turn.internal and turn.text_settled and bool(turn_id and turn.turn_id) and turn_id != turn.turn_id

    # ── lifecycle ───────────────────────────────────────────────────────
    async def _start_turn(self, event: Any, *, trigger: str = "on_processing_start") -> None:
        """Open the turn of *event*. Idempotent: intake, ``on_processing_start`` (the adapter lifecycle, or
        the authority executor's ``producer_turn`` for a busy follow-up) and the first ``pre_api_request``
        may all ask for the same message; only the first opens it, a later one attaches the event."""
        source = getattr(event, "source", None)
        chat_id = str(getattr(source, "chat_id", "") or "")
        opener = str(getattr(event, "message_id", "") or "")
        # Only a turn this message OPENED counts as already open. A message folded into another turn by its
        # busy ack (an alias) that now starts executing is a queued follow-up: it gets its own turn (redirect
        # never starts one — its text runs inside the older turn).
        existing = next((t for t in self._live_turns(chat_id) if t.opener_id == opener), None) if opener else None
        if existing is not None:
            if existing.event is None:
                existing.event = event
            return
        if trigger == "on_processing_start" and opener:
            for older in self._live_turns(chat_id):
                older.aliases.discard(opener)
        if opener and opener in self._closed_openers:
            return  # that message's turn already ended: a late lifecycle call must not reopen it
        if not chat_id or getattr(event, "reply_expected", None) is False or getattr(event, "internal", False):
            logger.info("[%s] no turn bubble (chat=%s reply_expected=%s internal=%s)", self.name, chat_id,
                        getattr(event, "reply_expected", None), getattr(event, "internal", False))
            return  # nothing will be said, or a synthetic wake with no inbound message to reply under
        req_id = self._cached_reply_req_id(chat_id, opener)
        if not req_id:
            logger.info("[%s] no turn bubble: no req_id for chat=%s opener=%s", self.name, chat_id, opener)
            return
        now = self._now()
        session = self._inbound_sessions.get(opener) or source_session_key(self, source)
        if trigger == "on_processing_start":
            # Only the lifecycle says a NEW turn of this session started executing (a queued follow-up).
            # A reply or hook for a message proves nothing about the older turn — it may still be running
            # (a busy ack is anchored on the new message while the old turn goes on).
            for older in self._live_turns(chat_id):
                if older.session == session:
                    await self._hand_over(older, now)
        elif any(t.session == session and not t.internal for t in self._live_turns(chat_id)):
            return  # a backstop open never stacks a second turn on a session that has a live one
        else:
            for older in self._live_turns(chat_id):
                if older.session == session:  # an internal turn: the session runs this message now
                    await self._hand_over(older, now)
        turn = Turn(chat_id=chat_id, opener_id=opener, session=session, msg=self._book.register(chat_id, req_id),
                    started=now, thinking_since=now, event=event,
                    is_group=str(getattr(source, "chat_type", "") or "") == "group", last_activity=now)
        self._turns.setdefault(chat_id, []).append(turn)
        logger.info("[%s] turn opened (%s) chat=%s opener=%s req_id=%s session=%s", self.name, trigger, chat_id,
                    opener, req_id, session)

    def _note_inbound(self, event: Any) -> None:
        """Intake (``adapter._on_message``, before ``handle_message``): remember the event so the turn can
        open from a signal the plugin owns even when no lifecycle hook brackets the turn. Commands get no
        turn (their reply is one message)."""
        get_command = getattr(event, "get_command", None)
        if callable(get_command) and get_command():
            return
        opener = str(getattr(event, "message_id", "") or "")
        if opener:
            self._pending_inbound[opener] = event
            while len(self._pending_inbound) > MAX_INBOUND_SESSIONS:
                self._pending_inbound.pop(next(iter(self._pending_inbound)))

    async def _open_from_hook(self, chat_id: str, message_id: str) -> Optional[Turn]:
        """Backstop: a model call / tool for a message whose turn no lifecycle hook opened (an execution
        path that does not bracket the adapter): open it from the intake record."""
        event = self._pending_inbound.get(message_id) if message_id else None
        if event is None or str(getattr(getattr(event, "source", None), "chat_id", "") or "") != chat_id:
            return None
        await self._start_turn(event, trigger="pre_api_request")
        return self._turn_for_message(chat_id, message_id)

    async def _hand_over(self, older: Turn, now: float) -> None:
        """A queued follow-up of the same session takes over: the older turn runs nothing any more. It
        closes now — with its answer, or with an empty finish frame when it said nothing —
        unless it is mid-answer (streamed text, no complete final yet): then it stays, frozen, until its
        own on_processing_complete. An internal turn has none: it always closes now."""
        older.freeze(now)
        if older.internal or older.answer_delivered or not older.body(STREAM_CURSORS).strip():
            await self._close_turn(older)

    async def _complete_turns(self, event: Any) -> set[str]:
        """Close the turn(s) *event* opened; returns the message ids of turns still live in its chat."""
        chat_id = str(getattr(getattr(event, "source", None), "chat_id", "") or "")
        opener = str(getattr(event, "message_id", "") or "")
        for turn in self._live_turns(chat_id):
            if turn.event is event or (opener and turn.opener_id == opener):
                await self._close_turn(turn)
        return {t.msg.message_id for t in self._live_turns(chat_id)}

    async def _close_turn(self, turn: Turn) -> None:
        """Seal the bubble: body + lasting notices (a final-only overlay, so a later edit continues cleanly);
        tool lines never outlive the turn. An empty turn closes with an empty finish frame, like the built-in adapter
        does for a suppressed reply. On a dead req_id the unshown rest is re-sent as markdown."""
        if turn.closed:
            return
        turn.closed, turn.closed_at = True, self._now()
        self._closed_openers[turn.opener_id] = None
        while len(self._closed_openers) > MAX_INBOUND_SESSIONS:
            self._closed_openers.pop(next(iter(self._closed_openers)))
        self._pending_inbound.pop(turn.opener_id, None)
        turn.freeze(turn.closed_at)  # nothing runs any more: no spinner on the finish frame
        if turn.msg.live is None and not turn.msg.bubbles:  # never drawn: nothing on screen to close
            self._book.discard(turn.msg)
            if turn.internal:  # its notices were never shown: they go out the old way
                for notice in [n for n in turn.notices.values() if n.text]:
                    await self._send_outside_turn(turn.chat_id, notice.text, None, {})
            await self._deliver_unshown(turn)
            return
        ok = await self._book.redraw(turn.msg, lambda: (turn.body(STREAM_CURSORS), ""), finalize=True,
                                     final_overlay=turn.final_tail())
        if not ok:
            # The finish frame failed: a dead req_id (846604), a lost subscription (846609), a server reject,
            # or the socket was down so it never left. Whatever was not proven on screen goes out the old way.
            # Only "sent but no ack in time" (indeterminate) counts as delivered — that returns ok.
            logger.warning("[%s] closing turn bubble failed for chat %s (req_dead=%s); re-sending its unshown body",
                           self.name, turn.chat_id, turn.msg.req_dead)
            self._book.discard(turn.msg)
            await self._deliver_unshown(turn)

    async def _deliver_unshown(self, turn: Turn) -> SendResult:
        """Send the part of the turn's reply no acked finish frame proved on screen (``body[confirmed_upto:]``).
        Intermediate frames are fire-and-forget, so nothing proves what they showed before a req_id died: that
        part may repeat (accepted over losing it). If the socket is down the send is parked and goes out on the
        next connection (``_flush_parked_rescues``)."""
        body = turn.body(STREAM_CURSORS)
        shown = turn.msg.confirmed_upto
        text = body[min(shown, len(body)):].strip()
        if not text:
            return SendResult(success=True)
        logger.warning("[%s] turn bubble for chat %s did not land; delivering %d chars on their own",
                       self.name, turn.chat_id, len(text))
        if not self._socket_up():
            self._park_rescue(turn.chat_id, text)
            return SendResult(success=True)
        result = await self._send_outside_turn(turn.chat_id, text, None, {})
        if not result.success:
            logger.error("[%s] turn reply re-send failed for chat %s: %s; parked for the next connection",
                         self.name, turn.chat_id, result.error)
            self._park_rescue(turn.chat_id, text)
        return result

    # ── rescue across a dropped connection (G1/G2) ──────────────────────
    def _socket_up(self) -> bool:
        ws = getattr(self, "_ws", None)
        return ws is not None and not getattr(ws, "closed", True)

    def _park_rescue(self, chat_id: str, text: str) -> None:
        """Keep an answer that could not be delivered now; it goes out on the next connection — of this
        adapter, or of the new adapter Hermes builds when it reconnects the platform (``rescue``)."""
        rescue.park(self._rescue_key(), chat_id, text)
        logger.warning("[%s] parked an undelivered reply for chat %s (%d chars) until the next connection",
                       self.name, chat_id, len(text))

    def _rescue_key(self) -> str:
        return str(getattr(self, "_bot_id", "") or "xwecom")

    async def _flush_parked_rescues(self) -> None:
        """Called once the socket is up (connect, reconnect): proactive send of every parked reply, in order."""
        for chat_id, text in rescue.take(self._rescue_key()):
            result = await self._send_outside_turn(chat_id, text, None, {"force_proactive_send": True})
            if not result.success:
                logger.error("[%s] parked reply for chat %s still undeliverable: %s", self.name, chat_id, result.error)
                rescue.park(self._rescue_key(), chat_id, text)

    async def _rescue_live_turns(self) -> None:
        """The connection is going away (disconnect / reconnect): every live turn's bubble can never be
        finished on its req_id again. Park whatever no acked finish proved on screen, so a reported-delivered
        answer is not lost across a restart or reconnect."""
        for turns in list(self._turns.values()):
            for turn in [t for t in turns if not t.closed]:
                turn.closed, turn.closed_at = True, self._now()
                self._book.discard(turn.msg)
                body = turn.body(STREAM_CURSORS)
                shown = turn.msg.confirmed_upto
                text = body[min(shown, len(body)):].strip()
                if text:
                    self._park_rescue(turn.chat_id, text)

    def _retire_turn(self, turn: Turn) -> None:
        """Its req_id is dead: nothing more can render there. Drop its message from the book so no later
        close re-renders it; the caller delivers the content another way."""
        turn.closed, turn.closed_at = True, self._now()
        self._book.discard(turn.msg)
        logger.info("[%s] turn bubble retired for chat %s (req_id dead)", self.name, turn.chat_id)

    def _spawn(self, coro: Any) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        task.add_done_callback(lambda t: t.cancelled() or t.exception())

    # ── drawing ─────────────────────────────────────────────────────────
    def _compose(self, turn: Turn) -> tuple[str, str]:
        """(body, status) of a live turn bubble. Cursors are stripped from every segment: a live WeCom
        stream already reads as "typing", and the body prefix stays stable for the seal offsets."""
        return turn.body(STREAM_CURSORS), turn.status(self._now())

    async def _redraw_turn(self, turn: Turn) -> bool:
        if turn.closed:
            return True
        if not any(self._compose(turn)):
            return True  # nothing to show: never draw WeCom's empty-stream "<think>" placeholder for a turn
        if turn.internal and not turn.msg.bubbles and not (turn.body(STREAM_CURSORS).strip() or turn.tools or turn.done):
            return True  # an internal turn shows up with its first tool or text: a silent wake stays invisible
        if not turn.msg.bubbles:
            logger.info("[%s] turn bubble first frame chat=%s opener=%s after %.1fs", self.name, turn.chat_id, turn.opener_id, self._now() - turn.started)
        return await self._book.redraw(turn.msg, lambda: self._compose(turn))

    def _kick(self, turn: Turn) -> None:
        """Redraw now (a hook just changed the status) instead of waiting for the next tick."""
        if not turn.closed:
            self._spawn(self._redraw_turn(turn))

    async def _tick_turns(self, now: float) -> None:
        """Per-second redraw. A turn whose message is busy (its finish frame awaiting the ack) is skipped
        so one slow chat never freezes the others; a turn with no event and no running tool for
        ``TURN_IDLE_TIMEOUT`` is closed."""
        for turns in list(self._turns.values()):
            for turn in [t for t in turns if not t.closed]:
                idle_limit = (SETTLE_AFTER_FINAL_SECONDS if turn.looks_finished() and turn.final_sent
                              else SETTLED_IDLE_TIMEOUT if turn.looks_finished()
                              else UNDRAWN_INTERNAL_IDLE if turn.internal and not turn.msg.bubbles
                              else TURN_IDLE_TIMEOUT)
                if turn.tools:
                    turn.touch(now)  # a running tool is activity, however long it takes
                elif now - turn.last_activity > idle_limit:
                    logger.info("[%s] closing turn bubble idle for %.0fs (chat %s)", self.name, now - turn.last_activity, turn.chat_id)
                    self._spawn(self._close_turn(turn))
                    continue
                if turn.msg.lock.locked():
                    continue
                if turn.msg.live is None and not turn.msg.bubbles and now - turn.started < OPEN_AFTER_SECONDS:
                    continue  # opened lazily: the first model call, tool or reply draws it
                turn.ticks += 1
                self._spawn(self._redraw_turn(turn))

    def _forget_closed_turns(self, now: float, max_age: float = 3600.0) -> None:
        for chat_id, turns in list(self._turns.items()):
            keep = [t for t in turns if not t.closed or now - t.closed_at < max_age]
            if keep:
                self._turns[chat_id] = keep
            else:
                del self._turns[chat_id]
        live = {id(t) for turns in self._turns.values() for t in turns}
        for fake_id, (turn, _) in list(self._turn_ids.items()):
            if id(turn) not in live:
                del self._turn_ids[fake_id]

    async def _settle_turns_before_media(self, chat_id: str, reply_to: Optional[str] = None) -> None:
        """A passive media reply needs its req_id free of a pending stream ack. When the media is anchored
        on a turn whose complete answer was delivered, that turn has nothing left to stream: close it so
        the attachment can go out as a reply (groups cannot send proactively). Any other turn — another
        session's, or one still answering — is left alone and the media goes proactive as before."""
        turn = self._turn_for_message(chat_id, str(reply_to or ""))
        if turn is not None and turn.answer_delivered:
            await self._close_turn(turn)

    # ── Hermes sends during a turn ──────────────────────────────────────
    def _send_lane(self) -> str:
        """"progress" / "agent_status" when this adapter call comes from core's tool-progress lane / agent
        status lane, else ""."""
        lane = core_send_lane()
        return {"progress": "progress", "status": "agent_status"}.get(lane, "")

    async def send_or_update_status(self, chat_id: str, status_key: str, content: str, *,
                                    metadata: Optional[dict[str, Any]] = None) -> SendResult:
        """Core's agent status lane (``_send_or_update_status_coro``): a live turn shows it as one status line,
        updated in place per *status_key*; otherwise it is a message of its own, as before."""
        turn = self._sole_session_turn(chat_id)
        if turn is not None:
            fake_id = f"xwt_status_{status_key}"
            turn.set_notice(fake_id, content)
            self._turn_ids[fake_id] = (turn, None)
            await self._redraw_turn(turn)
            return SendResult(success=True, message_id=fake_id)
        return await self.send(chat_id, content, metadata=metadata)

    def _is_status(self, content: str) -> bool:
        """An ``_interim_send`` core status line: heartbeat, stuck warning, approval timeout, drain notices.
        Busy acks are never sent with ``_interim_send``, so their looser pattern is not tried here."""
        return matches_status(content, self._status_re)

    def _is_busy_ack(self, content: str) -> bool:
        """``run_busy._send_busy_reply`` text: a busy ack (``{head}{detail}{tail}``) or a drain notice."""
        return matches_status(content, self._busy_ack_re) or matches_status(content, self._status_re)

    def _send_kind(self, chat_id: str, content: str, reply_to: Optional[str],
                   metadata: dict[str, Any]) -> tuple[Optional[Turn], Optional[str]]:
        """(turn, "reply" | "status" | "ack" | "progress" | "agent_status") for a send that belongs to a live
        turn, else (None, None)."""
        if metadata.get("is_approval_prompt") or metadata.get("force_proactive_send"):
            return None, None
        anchor = _anchor(reply_to, metadata)
        lane = self._send_lane()
        if lane:
            # Core's tool-progress lane / agent status lane (identified by the coroutine making the call):
            # they belong to the turn of this chat when one session's turn is live.
            turn = self._turn_for_message(chat_id, anchor) if anchor else self._sole_session_turn(chat_id)
            return (turn, lane) if turn is not None else (None, None)
        if metadata.get("_interim_send"):
            if not self._is_status(content):
                return None, None  # content (prose, a command, a plugin post, an answer tail): its own message
            turn = self._turn_for_message(chat_id, anchor) if anchor else self._sole_session_turn(chat_id)
            return (turn, "status") if turn is not None else (None, None)
        if not anchor:
            # An internal turn's reply carries no anchor. It joins that turn when it is the chat's only live
            # turn; it never opens one (cron / send_message deliver unanchored with the same markers).
            turn = self._sole_session_turn(chat_id)
            if turn is not None and turn.internal and (metadata.get("expect_edits") or metadata.get("notify")):
                return turn, "reply"
            return None, None  # cron / send_message / anything not addressed to a turn
        turn = self._turn_for_message(chat_id, anchor)
        if turn is not None:
            return turn, "reply"
        marked = metadata.get("expect_edits") or metadata.get("notify")
        if marked or anchor not in self._reply_req_ids or not self._is_busy_ack(content):
            return None, None
        session = self._inbound_sessions.get(anchor)
        turn = self._newest_of_session(chat_id, session) if session else self._sole_session_turn(chat_id)
        return (turn, "ack") if turn is not None else (None, None)

    async def _send_into_turn(self, chat_id: str, content: str, reply_to: Optional[str],
                              metadata: dict[str, Any]) -> Optional[SendResult]:
        anchor = _anchor(reply_to, metadata)
        if (anchor in self._pending_inbound and self._turn_for_message(chat_id, anchor) is None
                and (metadata.get("notify") or metadata.get("expect_edits"))
                and not metadata.get("_interim_send") and not metadata.get("is_approval_prompt")
                and not metadata.get("force_proactive_send")):
            # The turn's own reply (core marks every reply: the stream consumer ``expect_edits``, the final
            # send ``notify``) before any lifecycle hook or model call opened its turn: open it now so the
            # reply lands in it. Unmarked sends — a busy ack anchored on the new message — never open one.
            await self._open_from_hook(chat_id, anchor)
        turn, kind = self._send_kind(chat_id, content, reply_to, metadata)
        if turn is None:
            return None
        turn.touch(self._now())
        fake_id = f"xwt_{uuid.uuid4().hex[:16]}"
        if kind == "progress":
            # Tool-progress lines ("🔍 Searching the web for herdr.dev") become the labels of the status
            # lines the timer already draws for those tools, instead of a second bubble. Accepted under a
            # fake id so the lane's later edits (it rewrites the whole list) land here too.
            turn.open_progress_message(fake_id)
            turn.apply_progress_lines(content, fake_id)
            self._turn_ids[fake_id] = (turn, -1)
            self._kick(turn)
            return SendResult(success=True, message_id=fake_id)
        if kind == "agent_status":
            # Agent status lines (the memory recall indicator, provider fallback notices): one status line.
            turn.set_notice(fake_id, content)
            self._turn_ids[fake_id] = (turn, None)
            await self._redraw_turn(turn)
            return SendResult(success=True, message_id=fake_id)
        if kind in ("status", "ack"):
            if kind == "ack":
                turn.aliases.add(_anchor(reply_to, metadata))
            turn.set_notice(fake_id, content, lasting=matches_status(content, self._lasting_re))
            self._turn_ids[fake_id] = (turn, None)
            await self._redraw_turn(turn)  # a lost status line is not worth a fallback
            return SendResult(success=True, message_id=fake_id)
        complete = not metadata.get("expect_edits")
        index = turn.add_segment(content)
        if complete:
            turn.thinking = False
        if await self._redraw_turn(turn):
            self._turn_ids[fake_id] = (turn, index)
            turn.answer_delivered = turn.answer_delivered or complete
            if complete:
                turn.text_settled = True  # a preview never un-settles; a model call / tool does
                turn.final_sent = bool(metadata.get("notify"))  # core's turn-final send
            return SendResult(success=True, message_id=fake_id)
        if complete and turn.msg.req_dead:
            return await self._rescue_turn(turn, fake_id)
        # Not a dead req_id (a preview, or a transient failure): undo and let this send take the old path,
        # exactly as before the turn bubble existed.
        turn.segments.pop(index)
        if turn.msg.req_dead:
            self._retire_turn(turn)
        return None

    async def _rescue_turn(self, turn: Turn, fake_id: str) -> SendResult:
        """The turn's req_id is dead (846604): retire it and deliver what no acked frame proved on screen
        the old way — on a dead req_id that is a proactive markdown send."""
        self._retire_turn(turn)
        result = await self._deliver_unshown(turn)
        return SendResult(success=result.success, message_id=fake_id if result.success else None, error=result.error)

    async def _edit_turn_part(self, fake_id: str, content: str, *, finalize: bool) -> SendResult:
        turn, segment = self._turn_ids[fake_id]
        turn.touch(self._now())
        if segment == -1:  # the folded tool-progress list, rewritten by its lane
            if not turn.closed:
                turn.apply_progress_lines(content, fake_id)
                self._kick(turn)
            return SendResult(success=True, message_id=fake_id)
        if segment is None:  # a status line its producer refreshes (heartbeat)
            if turn.closed:
                return SendResult(success=False, error="turn is over")
            turn.edit_notice(fake_id, content)
            await self._redraw_turn(turn)
            return SendResult(success=True, message_id=fake_id)
        previous = turn.segments[segment]
        turn.set_segment(segment, content)
        if turn.closed:
            if self._book.get(turn.msg.message_id) is None:
                return SendResult(success=False, error="WeCom stream unavailable")
            # A late edit after the turn ended: the unseen rest continues in a fresh bubble (messages.py);
            # a lasting notice already sits under the sealed bubble and is not repeated.
            ok = await self._book.redraw(turn.msg, lambda: (turn.body(STREAM_CURSORS), ""), finalize=True)
        elif finalize:
            # A finalized segment is complete text, not necessarily the turn's answer (a tool-boundary
            # preamble finalizes too): it never marks the answer delivered, and it never stops a thinking
            # clock that a later model call already started.
            ok = await self._redraw_turn(turn)
            # A segment finalize is NOT the end of the turn: the consumer finalizes at every tool boundary,
            # and on its own task that can land after the next model call already started. Only a complete
            # reply (``_send_into_turn``) settles the turn for the idle close.
            if not ok and turn.msg.req_dead:
                return await self._rescue_turn(turn, fake_id)
            if ok and turn.internal:
                turn.text_settled = True  # no lifecycle closes an internal turn: finished text, then quiet, does
            if not ok:
                # Core treats this text as undelivered and its fallback sends the unseen tail itself: the
                # turn must not hold it too, or the tail shows twice. Back to what was last shown, cursor
                # stripped (the consumer will not edit this segment again), and the tail that follows
                # continues this segment's sentence instead of starting a new paragraph.
                turn.segments[segment] = turn.strip_cursor(previous, STREAM_CURSORS)
                turn.glued.add(segment)
        else:
            ok = await self._redraw_turn(turn)
        if not ok:
            return SendResult(success=False, error="WeCom stream unavailable")
        return SendResult(success=True, message_id=fake_id)

    # ── hook events (already on the loop, stamped with the time they fired) ──
    def llm_request_started(self, chat_id: str, message_id: str = "", at: Optional[float] = None,
                            session: str = "", turn_id: str = "", wake: bool = False) -> None:
        turn = self._turn_for_hook(chat_id, message_id, session)
        if turn is not None and self._foreign_to(turn, turn_id):
            return
        if turn is None and not message_id and wake:
            turn = self._open_internal_turn(chat_id, session, turn_id)
        if turn is None and message_id in self._pending_inbound:
            at = at if at is not None else self._now()
            self._spawn(self._open_then(chat_id, message_id, lambda t: t.llm_request_started(at)))
            return
        logger.info("[%s] pre_api_request chat=%s message_id=%s -> turn=%s", self.name, chat_id, message_id, getattr(turn, "opener_id", None))
        if turn is not None:
            turn.llm_request_started(at if at is not None else self._now())
            self._kick(turn)

    async def _open_then(self, chat_id: str, message_id: str, apply: Any) -> None:
        turn = await self._open_from_hook(chat_id, message_id)
        if turn is not None:
            apply(turn)
            self._kick(turn)

    def _turn_tool_started(self, chat_id: str, key: str, label: str, message_id: str, now: float,
                           session: str = "", turn_id: str = "") -> bool:
        turn = self._turn_for_hook(chat_id, message_id, session)
        if turn is not None and self._foreign_to(turn, turn_id):
            return True  # handled: ignored, and no legacy timer either

        prefixes = tool_line_prefixes(label or "tool")

        def start(t: Turn) -> None:
            t.tool_started(key, label, now, prefixes)
        if turn is None and message_id in self._pending_inbound:
            self._spawn(self._open_then(chat_id, message_id, start))
            return True
        if turn is None:
            return False
        start(turn)
        self._kick(turn)
        return True

    def _turn_tool_finished(self, chat_id: str, key: str, message_id: str, now: float) -> bool:
        turn = next((t for t in self._live_turns(chat_id) if key in t.tools), None)
        if turn is None:
            return False
        turn.tool_finished(key, now)
        self._kick(turn)
        return True
