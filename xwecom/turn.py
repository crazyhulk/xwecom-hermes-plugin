"""State of one *turn bubble*: the single WeCom bubble a turn lives in (parity with the core WeCom
tool timer, ``gateway/tool_timer.py``).

The bubble is opened when an inbound message starts processing and shows a status area that the ticker
re-renders every second:

    ✓ terminal (3s)              ← completed tools, oldest first, at most 5
    ⠹ 🔧 web_search (4s)          ← each running tool
    ⠹ 💭 Thinking (12s)           ← only when no tool runs and the model is working
    ⏳ Working — 3 min            ← notice lines folded from short status sends

The turn's reply text (``segments``, one per Hermes message the turn sent) sits above it. Segments are
only ever appended, so the body's prefix is stable (``messages.py`` seals bubbles at absolute body offsets).
Only known core status sends become notices (``STATUS_KEYS`` / busy acks, matched against their exact
templates). Reply text clears the tool lines (running and completed) drawn before it, as core's
``_append_accumulated`` cleared its progress lines; tools of a later round show again below the text.
When the turn ends the status area collapses to its *tail*: the lasting notices (an approval timeout) only —
no tool footer. Pure state: no I/O, no gateway imports.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
COMPLETED_MAX = 5
NOTICES_MAX = 3  # status lines shown at once (newest kept); older persistent ones still reach the tail
SEGMENT_JOINER = "\n\n"
INTERNAL_HEADER = "🔔 后台任务"  # tops an internal turn's bubble: it is not an answer to the user's last message
INTERNAL_HEADER_CONT = "🔔 后台任务（续）"  # the same background run continuing after its first bubble sealed
STATUS_RULE = "\n\n---\n"  # same rule messages.OVERLAY_RULE draws above the live status


# Core status sends the turn bubble folds, by i18n key (``locales/en.yaml``). Busy acks are composed as
# ``ack: "{head}{detail}{tail}"`` from one head and one tail.
STATUS_KEYS = ("gateway.progress.working_heartbeat", "gateway.progress.stuck_warning",
               "gateway.exec_approval.timed_out_notice", "gateway.busy.drain_queued",
               "gateway.busy.drain_rejected", "gateway.busy.drain_rejected_new_work")
BUSY_HEADS = ("steered_subagents_head", "steered_head", "redirected_head", "subagent_working_head",
              "compressing_head", "queued_head", "interrupting_head")
BUSY_TAILS = ("steered_subagents_tail", "steered_tail", "redirected_tail", "demoted_tail", "queued_tail",
              "interrupting_tail")
_FIELD = re.compile(r"\{([a-z_]*)\}")
# What each template field can expand to, as core fills it — never "anything":
#   minutes / remaining: an integer (heartbeat, stuck warning);
#   window: ``format_approval_window`` — "5 minutes" / "90 seconds" / "2 hours" or the localized
#     equivalent: a number plus a short unit;
#   action: ``_status_action_gerund`` — "restarting" / "shutting down" (localized), a few words;
#   detail: the heartbeat's " — iteration 2/90, terminal" (``_status_detail``), empty by default.
_FIELD_PATTERNS = {
    "minutes": r"\d+",
    "remaining": r"\d+",
    "window": r"\d+ ?[^\W\d_]{1,12}",
    "action": r"[^\W\d_][^\W\d_ ]{0,15}(?: [^\W\d_]{1,15})?",
    "detail": r"(?: — [^\n]{0,120})?",
}


def template_pattern(template: str) -> "re.Pattern[str]":
    """A full-match regex for a core ``str.format`` template: literal text exact, each field only what core
    puts there (``_FIELD_PATTERNS``; an unknown field matches nothing, so the template never folds)."""
    out, pos = [], 0
    for m in _FIELD.finditer(template):
        out.append(re.escape(template[pos:m.start()]))
        out.append(_FIELD_PATTERNS.get(m.group(1), r"(?!)"))
        pos = m.end()
    out.append(re.escape(template[pos:]))
    return re.compile("".join(out))


def busy_ack_pattern(heads: list[str], tails: list[str]) -> "re.Pattern[str]":
    """``{head}{detail}{tail}``: a known head, an optional parenthesised detail, a known tail (or none)."""
    head = "|".join(re.escape(h) for h in heads)
    tail = "|".join(re.escape(t) for t in tails)
    # detail = ``ack_detail_wrap`` " ({elapsed}, {iteration}, running: {tool})", ASCII or full-width parens
    return re.compile(f"(?:{head})(?: ?[(（][^\\n()（）]{{1,120}}[)）])?(?:{tail})?")


def matches_status(text: str, patterns: list["re.Pattern[str]"]) -> bool:
    stripped = (text or "").strip()
    return bool(stripped) and any(p.fullmatch(stripped) for p in patterns)


@dataclass
class TurnTool:
    label: str
    started: float
    detail: str = ""  # core tool-progress line for this call, e.g. "🔍 Searching the web for herdr.dev"
    took: int = 0  # seconds, once finished
    prefixes: tuple = ()  # how core's progress line for this tool starts (``compat.tool_line_prefixes``)
    round: int = 0  # the turn's model-call count when this call started
    cleared: bool = False  # reply text of a later model call arrived: no longer drawn

    @property
    def shown(self) -> str:
        """The label once done: the progress line without its emoji, else the bare tool name."""
        return re.sub(r"^\S+\s+", "", self.detail, count=1) if self.detail else self.label

    @property
    def running(self) -> str:
        """The label while running: core's progress line (it carries its own emoji), else ``🔧 name``."""
        return self.detail or f"🔧 {self.label}"


@dataclass
class Notice:
    text: str
    lasting: bool = False  # informative once the turn is over (approval timed out): kept in the tail


@dataclass
class Turn:
    chat_id: str
    opener_id: str  # inbound message id that opened the turn
    session: str  # adapter session key of the opener (chat, or chat + sender in per-user group sessions)
    msg: Any  # messages.VirtualMessage
    started: float
    thinking_since: float
    event: Any = None
    is_group: bool = False
    internal: bool = False  # a synthetic turn (no inbound message): drawn only once it has a tool or text
    turn_id: str = ""  # an internal turn: core's turn_id of the run that woke it
    header: str = ""  # first line of the bubble (internal turns: INTERNAL_HEADER / INTERNAL_HEADER_CONT)
    aliases: set[str] = field(default_factory=set)  # inbound ids folded into this turn (busy acks)
    thinking: bool = True  # the model is working (cleared when reply text arrives)
    round: int = 0  # model calls seen (``pre_api_request``): text clears the tools of earlier ones
    tools: dict[str, TurnTool] = field(default_factory=dict)  # tool_call_id -> running tool
    done: list[TurnTool] = field(default_factory=list)  # finished calls, oldest first, at most COMPLETED_MAX
    calls: list[TurnTool] = field(default_factory=list)  # every tool call of the turn, in start order
    _progress_msgs: dict = field(default_factory=dict)  # progress message fake id -> first call it covers
    _progress_texts: dict = field(default_factory=dict)  # progress message fake id -> its parsed lines
    _labelled_by: dict = field(default_factory=dict)  # progress message fake id -> ids of calls it labelled
    segments: list[str] = field(default_factory=list)
    notices: dict[str, Notice] = field(default_factory=dict)  # fake message id -> notice (arrival order)
    answer_delivered: bool = False  # a COMPLETE reply landed (a mid-turn preamble finalize does not count)
    text_settled: bool = False  # a complete reply (not a segment finalize) landed; a later call / tool clears it
    final_sent: bool = False  # that complete reply was core's turn-final send (``notify``): seal it soon
    glued: set[int] = field(default_factory=set)  # segment i's successor is its fallback tail: no break between
    ticks: int = 0
    last_activity: float = 0.0
    closed: bool = False
    closed_at: float = 0.0

    def answers(self, message_id: str) -> bool:
        return bool(message_id) and (message_id == self.opener_id or message_id in self.aliases)

    @staticmethod
    def strip_cursor(segment: str, strip_suffixes: tuple[str, ...]) -> str:
        for suffix in strip_suffixes:
            if suffix and segment.endswith(suffix):
                return segment[: -len(suffix)]
        return segment

    def body(self, strip_suffixes: tuple[str, ...] = ()) -> str:
        out, glue = "", ""
        for index, segment in enumerate(self.segments):
            segment = self.strip_cursor(segment, strip_suffixes)
            if not segment.strip():
                continue
            out += (glue if out else "") + segment
            glue = "" if index in self.glued else SEGMENT_JOINER
        # The header is part of the body (not the overlay) so it is sealed with the first bubble and every
        # seal / confirmed offset counts it; before any text it is drawn atop the status instead.
        return f"{self.header}{SEGMENT_JOINER}{out}" if out and self.header else out

    # ── events ──────────────────────────────────────────────────────────
    def touch(self, now: float) -> None:
        self.last_activity = max(self.last_activity, now)

    def set_segment(self, index: int, text: str) -> None:
        before = len(self.body())
        self.segments[index] = text
        if len(self.body()) > before:
            self.thinking = False  # reply text wins over the thinking animation (core: _append_accumulated)
            # ... and over the tool lines drawn so far. A model's text precedes its own tool calls, but the
            # stream consumer delivers it on its own task and may land after this round's first tool started
            # (seen on the real path): such a call belongs BELOW the text, so only earlier rounds clear.
            # Without any model-call signal there are no rounds: everything clears.
            for run in self.calls:
                if not self.round or run.round < self.round:
                    run.cleared = True

    def add_segment(self, text: str) -> int:
        self.segments.append("")
        self.set_segment(len(self.segments) - 1, text)
        return len(self.segments) - 1

    def llm_request_started(self, now: float) -> None:
        # A new model call means every tool of the previous round is done (core: on_llm_thinking).
        for key in list(self.tools):
            self._complete(key, now)
        self.thinking, self.thinking_since = True, now
        self.round += 1
        self.text_settled = self.final_sent = False
        self.touch(now)

    def tool_started(self, key: str, label: str, now: float, prefixes: tuple = ()) -> None:
        if key not in self.tools:
            run = self.tools[key] = TurnTool(label or "tool", now, prefixes=tuple(prefixes), round=self.round)
            self.calls.append(run)
            self._apply_pending_progress()
        self.text_settled = self.final_sent = False
        self.touch(now)

    def open_progress_message(self, fake_id: str) -> None:
        """Core sent a NEW progress message (its first, or a fresh one after a content bubble reset the
        old). It lists the tools started since the previous message's last line — core sends a message only
        once its first line exists, so that tool already started. Its lines map to calls from the first call
        no earlier message covers."""
        covered = max((self._covered_upto(fid) for fid in self._progress_msgs), default=0)
        self._progress_msgs[fake_id] = covered

    def _covered_upto(self, fake_id: str) -> int:
        """One past the last call an earlier progress message's lines labelled."""
        start = self._progress_msgs.get(fake_id, 0)
        labelled = [i for i, c in enumerate(self.calls[start:], start) if id(c) in self._labelled_by.get(fake_id, set())]
        return (max(labelled) + 1) if labelled else start

    def apply_progress_lines(self, text: str, fake_id: str = "") -> None:
        """Core's tool-progress message (rewritten whole on every new line) gives tool calls their human
        label. A line is matched to a call only by what identifies it: it must start with that call's
        ``{emoji} {tool|verb}`` prefix. Lines are taken in order against the calls the message covers (from
        its offset), each consuming the first unlabelled matching call; ``(×N)`` covers N matching calls.
        Anything else — thinking text, the /verbose hint, a line that matches no pending call — is skipped,
        and a call that no line identifies keeps its bare hook label rather than a wrong one."""
        lines, in_fence = [], False
        for raw in (text or "").splitlines():
            line = raw.strip()
            if line.startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence or not line:
                continue
            if re.fullmatch(r"\(×\d+\)", line) and lines:  # a repeat count on its own line (after a fence)
                lines[-1] = (lines[-1][0], int(line[2:-1]))
                continue
            m = re.search(r"\s*\(×(\d+)\)$", line)
            lines.append((line[: m.start()] if m else line, int(m.group(1)) if m else 1))
        self._progress_texts[fake_id] = lines
        self._apply_pending_progress()

    def _apply_pending_progress(self) -> None:
        for fake_id, lines in self._progress_texts.items():
            calls = self.calls[self._progress_msgs.get(fake_id, 0):]
            used: set[int] = set()
            self._labelled_by[fake_id] = labelled = set()
            for line, count in lines:
                for _ in range(count):
                    idx = next((i for i, c in enumerate(calls) if i not in used and c.prefixes
                                and any(line == p or line.startswith((p + " ", p + ":", p + "：", p + ".", p + "…"))
                                        for p in c.prefixes)), None)
                    if idx is None:
                        break
                    used.add(idx)
                    labelled.add(id(calls[idx]))
                    calls[idx].detail = line

    def tool_finished(self, key: str, now: float) -> None:
        if key in self.tools:
            self._complete(key, now)
        if not self.tools:
            # Back to the model; the next pre_api_request (normally immediate) restarts this clock too.
            self.thinking, self.thinking_since = True, now
        self.touch(now)

    def freeze(self, now: float) -> None:
        """A newer turn of the same session took over: this one runs nothing any more."""
        for key in list(self.tools):
            self._complete(key, now)
        self.thinking = False

    def _complete(self, key: str, now: float) -> None:
        run = self.tools.pop(key)
        run.took = int(now - run.started)
        self.done.append(run)
        del self.done[:-COMPLETED_MAX]

    @property
    def completed(self) -> list[str]:
        """``✓ {label} ({N}s)`` per finished call, oldest first. Rendered (not stored) so a progress line
        that arrives after its call finished — core's progress lane batches its edits — still labels it."""
        return [f"✓ {run.shown} ({run.took}s)" for run in self.done]

    def set_notice(self, fake_id: str, text: str, *, lasting: bool = False) -> None:
        self.notices[fake_id] = Notice((text or "").strip(), lasting)

    def edit_notice(self, fake_id: str, text: str) -> None:
        self.notices[fake_id].text = (text or "").strip()

    def looks_finished(self) -> bool:
        """A complete reply landed and nothing (model call, tool) happened after it."""
        return self.text_settled and not self.tools

    # ── rendering ───────────────────────────────────────────────────────
    def status(self, now: float) -> str:
        """The live status area (spinner frame advances with ``ticks``)."""
        spin = SPINNER[self.ticks % len(SPINNER)]
        lines = [f"✓ {run.shown} ({run.took}s)" for run in self.done if not run.cleared]
        running = [run for run in self.tools.values() if not run.cleared]
        if running:
            lines += [f"{spin} {run.running} ({int(now - run.started)}s)" for run in running]
        elif self.thinking and not self.tools and not self.internal:  # an internal turn never shows Thinking
            lines.append(f"{spin} 💭 Thinking ({int(now - self.thinking_since)}s)")
        lines += [n.text for n in list(self.notices.values())[-NOTICES_MAX:] if n.text]
        if lines and self.header and not self.body():
            lines.insert(0, self.header)
        return "\n".join(lines)

    def final_tail(self) -> str:
        """What stays under the reply once the turn is over: lasting notices only. Tool lines and live status
        (heartbeat, busy acks, stuck warning) are gone with the turn."""
        return "\n".join(n.text for n in self.notices.values() if n.text and n.lasting)

    def final_text(self, strip_suffixes: tuple[str, ...]) -> str:
        body, tail = self.body(strip_suffixes), self.final_tail()
        return f"{body}{STATUS_RULE}{tail}" if body and tail else body or tail
