"""
Template card detection, validation, and sending.

Aligned with OpenClaw:
  - src/template-card-parser.ts (extractTemplateCards, maskTemplateCardBlocks,
    field normalization & validation, simplified-format transforms)
  - src/template-card-manager.ts (cache, sendTemplateCards, updateTemplateCard
    on event)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────────────────

# Aligned with OpenClaw: src/const.ts:VALID_CARD_TYPES
VALID_CARD_TYPES: Tuple[str, ...] = (
    "text_notice",
    "news_notice",
    "button_interaction",
    "vote_interaction",
    "multiple_interaction",
)

# Aligned with OpenClaw: src/const.ts cache settings
TEMPLATE_CARD_CACHE_TTL_MS = 30 * 60 * 1000  # 30 min
TEMPLATE_CARD_CACHE_MAX_SIZE = 500

# Regex for fenced JSON blocks (``` or ```json) — aligned with TS CODE_BLOCK_RE
_CODE_BLOCK_RE = re.compile(r"```(?:json)?\s*\n([\s\S]*?)\n```", re.MULTILINE)
# Unclosed trailing code block (still being streamed)
_UNCLOSED_BLOCK_RE = re.compile(r"```(?:json)?\s*\n[\s\S]*$", re.MULTILINE)


# ── Data types ─────────────────────────────────────────────────────────────


@dataclass
class ExtractedTemplateCard:
    """Aligned with OpenClaw: src/interface.ts:ExtractedTemplateCard"""

    card_json: Dict[str, Any]
    card_type: str


@dataclass
class TemplateCardExtractionResult:
    """Aligned with OpenClaw: src/interface.ts:TemplateCardExtractionResult"""

    cards: List[ExtractedTemplateCard] = field(default_factory=list)
    remaining_text: str = ""


# ── Field type coercion helpers ────────────────────────────────────────────


def _coerce_to_int(value: Any) -> Optional[int]:
    """Aligned with OpenClaw: template-card-parser.ts:coerceToInt"""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        try:
            return int(round(value))
        except (OverflowError, ValueError):
            return None
    if isinstance(value, str):
        try:
            return int(round(float(value.strip())))
        except (ValueError, AttributeError):
            return None
    return None


def _coerce_to_bool(value: Any) -> Optional[bool]:
    """Aligned with OpenClaw: template-card-parser.ts:coerceToBool"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
        return None
    if isinstance(value, (int, float)):
        return value != 0
    return None


_MODE_ALIASES: Dict[str, int] = {
    "single": 0,
    "radio": 0,
    "单选": 0,
    "multi": 1,
    "multiple": 1,
    "多选": 1,
}


def _coerce_checkbox_mode(value: Any) -> Optional[int]:
    """Aligned with OpenClaw: template-card-parser.ts:coerceCheckboxMode"""
    if isinstance(value, str):
        t = value.strip().lower()
        if t in _MODE_ALIASES:
            return _MODE_ALIASES[t]
    n = _coerce_to_int(value)
    if n is None:
        return None
    return 1 if n > 0 else 0


# ── Field normalisation ────────────────────────────────────────────────────


def _normalize_template_card_fields(card: Dict[str, Any]) -> Dict[str, Any]:
    """Aligned with OpenClaw: template-card-parser.ts:normalizeTemplateCardFields"""
    checkbox = card.get("checkbox")
    if isinstance(checkbox, dict):
        if "mode" in checkbox:
            fixed = _coerce_checkbox_mode(checkbox["mode"])
            if fixed is None:
                checkbox.pop("mode", None)
            else:
                checkbox["mode"] = fixed
        if "disable" in checkbox:
            fixed_b = _coerce_to_bool(checkbox["disable"])
            if fixed_b is not None:
                checkbox["disable"] = fixed_b
        opts = checkbox.get("option_list")
        if isinstance(opts, list):
            for opt in opts:
                if isinstance(opt, dict) and "is_checked" in opt:
                    b = _coerce_to_bool(opt["is_checked"])
                    if b is not None:
                        opt["is_checked"] = b

    for key in ("source", "card_action", "quote_area", "image_text_area"):
        block = card.get(key)
        if isinstance(block, dict):
            inner_key = "desc_color" if key == "source" else "type"
            if inner_key in block:
                fixed_n = _coerce_to_int(block[inner_key])
                if fixed_n is not None:
                    block[inner_key] = fixed_n

    for key in ("horizontal_content_list", "jump_list"):
        items = card.get(key)
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and "type" in item:
                    fixed_n = _coerce_to_int(item["type"])
                    if fixed_n is not None:
                        item["type"] = fixed_n

    button_list = card.get("button_list")
    if isinstance(button_list, list):
        for btn in button_list:
            if isinstance(btn, dict) and "style" in btn:
                fixed_n = _coerce_to_int(btn["style"])
                if fixed_n is not None:
                    btn["style"] = fixed_n

    button_selection = card.get("button_selection")
    if isinstance(button_selection, dict) and "disable" in button_selection:
        fixed_b = _coerce_to_bool(button_selection["disable"])
        if fixed_b is not None:
            button_selection["disable"] = fixed_b

    select_list = card.get("select_list")
    if isinstance(select_list, list):
        for sel in select_list:
            if isinstance(sel, dict) and "disable" in sel:
                fixed_b = _coerce_to_bool(sel["disable"])
                if fixed_b is not None:
                    sel["disable"] = fixed_b

    return card


# ── Required-field validation ──────────────────────────────────────────────


_TASK_ID_INVALID_CHARS = re.compile(r"[^a-zA-Z0-9_\-@]")
_TASK_ID_TIMESTAMP_TAIL = re.compile(r"_\d{8,}$")


def _short_rand(n: int = 4) -> str:
    return uuid.uuid4().hex[:n]


def _validate_and_fix_required_fields(card: Dict[str, Any]) -> Dict[str, Any]:
    """Aligned with OpenClaw: template-card-parser.ts:validateAndFixRequiredFields"""
    card_type = card.get("card_type", "")
    ts = int(time.time() * 1000)
    rand = _short_rand()

    raw_tid = card.get("task_id")
    raw_tid_s = raw_tid.strip() if isinstance(raw_tid, str) else ""
    if raw_tid_s:
        prefix = _TASK_ID_TIMESTAMP_TAIL.sub("", raw_tid_s)
        prefix = _TASK_ID_INVALID_CHARS.sub("_", prefix)[:80]
        final_tid = f"{prefix}_{ts}_{rand}" if prefix else f"task_{card_type}_{ts}_{rand}"
    else:
        final_tid = f"task_{card_type}_{ts}_{rand}"
    card["task_id"] = final_tid

    main_title = card.get("main_title")
    has_main_title = (
        isinstance(main_title, dict)
        and isinstance(main_title.get("title"), str)
        and main_title["title"].strip()
    )
    sub_title_text = card.get("sub_title_text")
    has_sub = isinstance(sub_title_text, str) and sub_title_text.strip()

    if card_type == "text_notice":
        if not has_main_title and not has_sub:
            card["sub_title_text"] = sub_title_text or "通知"
    elif card_type in ("news_notice", "button_interaction", "vote_interaction", "multiple_interaction"):
        if not isinstance(main_title, dict):
            card["main_title"] = {"title": "通知"}
        elif not has_main_title:
            main_title["title"] = "通知"

    if card_type in ("text_notice", "news_notice"):
        if not isinstance(card.get("card_action"), dict):
            card["card_action"] = {"type": 1, "url": "https://work.weixin.qq.com"}

    if card_type in ("vote_interaction", "multiple_interaction"):
        if not isinstance(card.get("submit_button"), dict):
            card["submit_button"] = {
                "text": "提交",
                "key": f"submit_{card_type}_{ts}",
            }

    return card


# ── Simplified format transforms ───────────────────────────────────────────


def _gen_key(prefix: str) -> str:
    return f"{prefix}_{int(time.time() * 1000)}_{_short_rand()}"


def _transform_vote_interaction(card: Dict[str, Any]) -> Dict[str, Any]:
    """Aligned with OpenClaw: template-card-parser.ts:transformVoteInteraction"""
    existing_checkbox = card.get("checkbox")
    if isinstance(existing_checkbox, dict) and isinstance(
        existing_checkbox.get("option_list"), list
    ):
        return card

    options = card.get("options")
    if not isinstance(options, list) or not options:
        return card

    title = card.pop("title", None)
    description = card.pop("description", None)
    if title or description:
        mt: Dict[str, Any] = {}
        if title:
            mt["title"] = title
        if description:
            mt["desc"] = description
        card["main_title"] = mt

    mode = _coerce_checkbox_mode(card.pop("mode", None)) or 0
    clamped = options[:20]
    card["checkbox"] = {
        "question_key": _gen_key("vote"),
        "mode": mode,
        "option_list": [
            {
                "id": str(opt.get("id") or opt.get("value") or f"opt_{_short_rand()}"),
                "text": str(opt.get("text") or opt.get("label") or opt.get("name") or ""),
            }
            for opt in clamped
            if isinstance(opt, dict)
        ],
    }
    card.pop("options", None)

    submit_text = card.pop("submit_text", None) or "提交"
    card["submit_button"] = {"text": submit_text, "key": _gen_key("submit_vote")}

    for cleanup in ("vote_question", "vote_option", "vote_options"):
        card.pop(cleanup, None)
    return card


def _transform_multiple_interaction(card: Dict[str, Any]) -> Dict[str, Any]:
    """Aligned with OpenClaw: template-card-parser.ts:transformMultipleInteraction"""
    existing = card.get("select_list")
    if (
        isinstance(existing, list)
        and existing
        and isinstance(existing[0], dict)
        and isinstance(existing[0].get("option_list"), list)
    ):
        return card

    selectors = card.get("selectors")
    if not isinstance(selectors, list) or not selectors:
        return card

    title = card.pop("title", None)
    description = card.pop("description", None)
    if title or description:
        mt: Dict[str, Any] = {}
        if title:
            mt["title"] = title
        if description:
            mt["desc"] = description
        card["main_title"] = mt

    clamped = selectors[:3]
    card["select_list"] = []
    for idx, sel in enumerate(clamped):
        if not isinstance(sel, dict):
            continue
        sel_opts = (sel.get("options") or [])[:10]
        card["select_list"].append(
            {
                "question_key": _gen_key(f"sel_{idx}"),
                "title": str(sel.get("title") or sel.get("label") or f"选择{idx + 1}"),
                "option_list": [
                    {
                        "id": str(o.get("id") or o.get("value") or f"opt_{_short_rand()}"),
                        "text": str(o.get("text") or o.get("label") or o.get("name") or ""),
                    }
                    for o in sel_opts
                    if isinstance(o, dict)
                ],
            }
        )
    card.pop("selectors", None)

    submit_text = card.pop("submit_text", None) or "提交"
    card["submit_button"] = {"text": submit_text, "key": _gen_key("submit_multi")}
    return card


def _transform_simplified_card(card: Dict[str, Any]) -> Dict[str, Any]:
    card_type = card.get("card_type", "")
    if card_type == "vote_interaction":
        return _transform_vote_interaction(card)
    if card_type == "multiple_interaction":
        return _transform_multiple_interaction(card)
    return card


# ── Extraction & masking ───────────────────────────────────────────────────


def extract_template_cards(text: str) -> TemplateCardExtractionResult:
    """Find fenced JSON blocks whose card_type is valid.

    Aligned with OpenClaw: template-card-parser.ts:extractTemplateCards
    """
    cards: List[ExtractedTemplateCard] = []
    blocks_to_remove: List[str] = []
    if not text:
        return TemplateCardExtractionResult(cards=cards, remaining_text="")

    for match in _CODE_BLOCK_RE.finditer(text):
        full = match.group(0)
        json_content = match.group(1).strip()
        try:
            parsed = json.loads(json_content)
        except (ValueError, TypeError) as err:
            logger.debug(f"[template-card] JSON parse failed: {err}")
            continue
        if not isinstance(parsed, dict):
            continue
        card_type = parsed.get("card_type")
        if not isinstance(card_type, str) or card_type not in VALID_CARD_TYPES:
            continue

        _transform_simplified_card(parsed)
        _normalize_template_card_fields(parsed)
        _validate_and_fix_required_fields(parsed)

        cards.append(ExtractedTemplateCard(card_json=parsed, card_type=card_type))
        blocks_to_remove.append(full)

    remaining = text
    for block in blocks_to_remove:
        remaining = remaining.replace(block, "", 1)
    remaining = re.sub(r"\n{3,}", "\n\n", remaining).strip()

    return TemplateCardExtractionResult(cards=cards, remaining_text=remaining)


_CARD_TYPE_HINT_RE = re.compile(r"['\"]card_type['\"]")


def mask_template_card_blocks(text: str) -> str:
    """Replace template-card JSON code blocks with a friendly placeholder.

    Aligned with OpenClaw: template-card-parser.ts:maskTemplateCardBlocks
    """
    if not text or "```" not in text:
        return text

    def _replace_closed(match: "re.Match[str]") -> str:
        full = match.group(0)
        content = match.group(1)
        if _CARD_TYPE_HINT_RE.search(content):
            return "\n\n📋 *正在生成卡片消息...*\n\n"
        return full

    masked = _CODE_BLOCK_RE.sub(_replace_closed, text)

    unclosed = _UNCLOSED_BLOCK_RE.search(masked)
    if unclosed and _CARD_TYPE_HINT_RE.search(unclosed.group(0)):
        masked = masked[: unclosed.start()] + "\n\n📋 *正在生成卡片消息...*"
    return masked


# ── Cache ──────────────────────────────────────────────────────────────────


@dataclass
class _CacheEntry:
    template_card: Dict[str, Any]
    created_at: float


class TemplateCardCache:
    """In-memory cache of sent cards, keyed by ``(accountId, taskId)``.

    Aligned with OpenClaw: template-card-manager.ts cache section.
    """

    def __init__(
        self,
        ttl_ms: int = TEMPLATE_CARD_CACHE_TTL_MS,
        max_size: int = TEMPLATE_CARD_CACHE_MAX_SIZE,
    ) -> None:
        self._ttl_ms = ttl_ms
        self._max_size = max_size
        self._store: Dict[str, _CacheEntry] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(account_id: str, task_id: str) -> str:
        return f"{account_id}:{task_id}"

    def _prune_locked(self) -> None:
        now = time.time()
        for key in list(self._store.keys()):
            entry = self._store[key]
            if (now - entry.created_at) * 1000 >= self._ttl_ms:
                self._store.pop(key, None)
        if len(self._store) <= self._max_size:
            return
        ordered = sorted(self._store.items(), key=lambda kv: kv[1].created_at)
        overflow = len(self._store) - self._max_size
        for k, _ in ordered[:overflow]:
            self._store.pop(k, None)

    def save(self, account_id: str, template_card: Dict[str, Any]) -> None:
        task_id = template_card.get("task_id")
        if not task_id:
            return
        with self._lock:
            self._store[self._key(account_id, task_id)] = _CacheEntry(
                template_card=json.loads(json.dumps(template_card)),
                created_at=time.time(),
            )
            self._prune_locked()

    def get(self, account_id: str, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            self._prune_locked()
            entry = self._store.get(self._key(account_id, task_id))
            if not entry:
                return None
            return json.loads(json.dumps(entry.template_card))

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# ── Event update ───────────────────────────────────────────────────────────


def _build_selected_option_map(
    template_card_event: Optional[Dict[str, Any]],
) -> Dict[str, List[str]]:
    """Aligned with OpenClaw: template-card-manager.ts:buildSelectedOptionMap"""
    result: Dict[str, List[str]] = {}
    if not template_card_event:
        return result
    items = (template_card_event.get("selected_items") or {}).get("selected_item") or []
    for item in items:
        qk = (item.get("question_key") or "").strip()
        if not qk:
            continue
        option_ids = [
            oid for oid in ((item.get("option_ids") or {}).get("option_id") or []) if oid
        ]
        result[qk] = option_ids
    return result


def _apply_selected_state(
    template_card: Dict[str, Any],
    selected_map: Dict[str, List[str]],
    template_card_event: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Aligned with OpenClaw: template-card-manager.ts:applySelectedStateToTemplateCard"""
    card = json.loads(json.dumps(template_card))

    if template_card_event:
        if template_card_event.get("task_id"):
            card["task_id"] = template_card_event["task_id"]
        if template_card_event.get("card_type"):
            card["card_type"] = template_card_event["card_type"]

    sb = card.get("submit_button")
    if isinstance(sb, dict) and sb.get("text"):
        sb["text"] = "已提交"

    checkbox = card.get("checkbox")
    if isinstance(checkbox, dict) and checkbox.get("question_key"):
        selected_ids = selected_map.get(checkbox["question_key"], [])
        checkbox["disable"] = True
        opts = checkbox.get("option_list")
        if isinstance(opts, list):
            checkbox["option_list"] = [
                {**o, "is_checked": (o.get("id") in selected_ids)}
                for o in opts
                if isinstance(o, dict)
            ]

    select_list = card.get("select_list")
    if isinstance(select_list, list):
        next_list = []
        for sel in select_list:
            if not isinstance(sel, dict):
                next_list.append(sel)
                continue
            qk = sel.get("question_key", "")
            selected_ids = selected_map.get(qk, [])
            new_sel = dict(sel)
            new_sel["disable"] = True
            if selected_ids:
                new_sel["selected_id"] = selected_ids[0]
            next_list.append(new_sel)
        card["select_list"] = next_list

    bs = card.get("button_selection")
    if isinstance(bs, dict) and bs.get("question_key"):
        selected_ids = selected_map.get(bs["question_key"], [])
        bs["disable"] = True
        if selected_ids:
            bs["selected_id"] = selected_ids[0]

    return card


async def update_template_card_on_event(
    ws_client: Any,
    frame: Dict[str, Any],
    *,
    account_id: str,
    cache: TemplateCardCache,
) -> bool:
    """Refresh a sent card based on a ``template_card_event`` callback.

    Aligned with OpenClaw: template-card-manager.ts:updateTemplateCardOnEvent

    Returns True if the card was successfully pushed back, False if skipped.
    """
    body = frame.get("body") or {}
    event = body.get("event") or {}
    tce = event.get("template_card_event") or {}
    task_id = tce.get("task_id")
    if not task_id:
        return False
    cached = cache.get(account_id, task_id)
    if not cached:
        return False

    selected_map = _build_selected_option_map(tce)
    updated = _apply_selected_state(cached, selected_map, tce)

    from_obj = body.get("from") or {}
    user_id = from_obj.get("userid")
    userids = [user_id] if user_id else []

    # The SDK exposes either update_template_card or reply_template_card_with_userids;
    # we adapt to what's present.
    if hasattr(ws_client, "update_template_card"):
        await ws_client.update_template_card(frame, updated, userids)
    elif hasattr(ws_client, "reply_template_card"):
        await ws_client.reply_template_card(frame, updated)
    else:
        # Fall back to send_message with msgtype=template_card.
        chat_id = body.get("chatid") or user_id
        if chat_id:
            await ws_client.send_message(
                chat_id, {"msgtype": "template_card", "template_card": updated}
            )

    cache.save(account_id, updated)
    return True


# ── Sending cards ──────────────────────────────────────────────────────────


async def send_template_cards(
    ws_client: Any,
    frame: Dict[str, Any],
    *,
    cards: List[ExtractedTemplateCard],
    account_id: str,
    cache: TemplateCardCache,
) -> Tuple[int, int]:
    """Push template cards via ``ws_client.send_message``.

    Aligned with OpenClaw: template-card-manager.ts:sendTemplateCards

    Returns ``(success_count, failure_count)``.
    """
    body = frame.get("body") or {}
    chat_id = body.get("chatid") or (body.get("from") or {}).get("userid")
    if not chat_id:
        return (0, len(cards))

    success = 0
    failure = 0
    for card in cards:
        try:
            await ws_client.send_message(
                chat_id,
                {"msgtype": "template_card", "template_card": card.card_json},
            )
            cache.save(account_id, card.card_json)
            success += 1
        except Exception as err:  # noqa: BLE001
            logger.error(
                f"[template-card] failed to send {card.card_type}: {err}"
            )
            failure += 1
    return success, failure


async def process_template_cards_if_needed(
    ws_client: Any,
    frame: Dict[str, Any],
    *,
    accumulated_text: str,
    account_id: str,
    cache: TemplateCardCache,
) -> Optional[TemplateCardExtractionResult]:
    """Detect & send cards in one go.

    Aligned with OpenClaw: template-card-manager.ts:processTemplateCardsIfNeeded

    Returns the extraction result if at least one card was sent, else None.
    """
    visible = (accumulated_text or "").strip()
    if not visible:
        return None
    result = extract_template_cards(accumulated_text)
    if not result.cards:
        return None
    await send_template_cards(
        ws_client,
        frame,
        cards=result.cards,
        account_id=account_id,
        cache=cache,
    )
    return result
