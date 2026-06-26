"""
Message parser — full WeCom message body parsing.

Aligned with OpenClaw: src/message-parser.ts:parseMessageContent

Extracts text, images, files, voice content, quote content, mentions,
and event callback contents from WeCom WsFrame.body dicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ── Auth type map (Aligned with OpenClaw: src/message-parser.ts:AUTH_TYPE_MAP) ──
AUTH_TYPE_MAP: Dict[int, str] = {
    1: "新建和编辑文档",
    2: "获取成员文档内容",
}

# Auth type enum values from OpenClaw
AUTH_CREATE_AND_EDIT_DOC = 1
AUTH_GET_DOC_CONTENT = 2


@dataclass
class ParsedMessageContent:
    """Result of parsing a WeCom message body.

    Aligned with OpenClaw: src/message-parser.ts:ParsedMessageContent
    """

    text_parts: List[str] = field(default_factory=list)
    """Collected text segments (text body, voice transcription, mixed text items, event text)."""

    image_urls: List[str] = field(default_factory=list)
    """Image URLs to download."""

    image_aes_keys: Dict[str, str] = field(default_factory=dict)
    """url → aes_key map for encrypted images."""

    file_urls: List[str] = field(default_factory=list)
    """File / video URLs to download."""

    file_aes_keys: Dict[str, str] = field(default_factory=dict)
    """url → aes_key map for encrypted files."""

    quote_content: Optional[str] = None
    """Original quoted text content (if reply quotes a text/voice message)."""

    mentions: List[str] = field(default_factory=list)
    """User IDs mentioned (@user_id) in the message text."""

    location: Optional[Dict[str, Any]] = None
    """Location payload, if msgtype == 'location'."""

    link: Optional[Dict[str, Any]] = None
    """Link payload (title/description/url), if msgtype == 'link'."""

    @property
    def text(self) -> str:
        """Convenience: joined text from text_parts."""
        return "\n".join(self.text_parts).strip()


# ── Event text builders ─────────────────────────────────────────────────────


def _build_template_card_event_text(body: Dict[str, Any]) -> Optional[str]:
    """Format a template_card_event for downstream LLM consumption.

    Aligned with OpenClaw: src/message-parser.ts:buildTemplateCardEventText
    """
    if body.get("msgtype") != "event":
        return None
    event = body.get("event") or {}
    if event.get("eventtype") != "template_card_event":
        return None
    template_card_event = event.get("template_card_event")
    if not template_card_event:
        return None

    selected_items = (
        (template_card_event.get("selected_items") or {}).get("selected_item") or []
    )
    selected_lines: List[str] = []
    for item in selected_items:
        question_key = (item.get("question_key") or "").strip() or "unknown_question"
        option_ids = [oid for oid in ((item.get("option_ids") or {}).get("option_id") or []) if oid]
        ids_str = ", ".join(option_ids) if option_ids else "(未选择)"
        selected_lines.append(f"- {question_key}: {ids_str}")

    sender = body.get("from") or {}
    sender_user_id = sender.get("userid") or ""
    sender_corp_id = sender.get("corpid") or ""
    chat_id = body.get("chatid") or sender_user_id

    lines: List[Optional[str]] = [
        "[企业微信模板卡片回调]",
        "event_type(事件类型): template_card_event",
        f"msgid(消息 id): {body['msgid']}" if body.get("msgid") else None,
        f"aibotid(机器人 id): {body['aibotid']}" if body.get("aibotid") else None,
        f"chat_type(会话类型): {body['chattype']}" if body.get("chattype") else None,
        f"chat_id(会话 id): {chat_id}" if chat_id else None,
        f"from.corpid(企业 id): {sender_corp_id}" if sender_corp_id else None,
        f"from.userid(发送人 id): {sender_user_id}" if sender_user_id else None,
        f"sender_userid(发送人 id): {sender_user_id}" if sender_user_id else None,
        f"card_type(卡片类型): {template_card_event['card_type']}"
        if template_card_event.get("card_type")
        else None,
        f"event_key(事件 key): {template_card_event['event_key']}"
        if template_card_event.get("event_key")
        else None,
        f"task_id(任务 id): {template_card_event['task_id']}"
        if template_card_event.get("task_id")
        else None,
        "selected_items(选择项):" if selected_lines else "selected_items(选择项): []",
    ]
    lines.extend(selected_lines)
    return "\n".join(line for line in lines if line)


def _build_auth_change_event_text(body: Dict[str, Any]) -> Optional[str]:
    """Format an auth_change_event for downstream LLM consumption.

    Aligned with OpenClaw: src/message-parser.ts:buildAuthChangeEventText
    """
    if body.get("msgtype") != "event":
        return None
    event = body.get("event") or {}
    if event.get("eventtype") != "auth_change_event":
        return None
    auth_change = event.get("auth_change_event")
    if not auth_change:
        return None

    auth_list: List[int] = auth_change.get("auth_list") or []
    auth_descriptions = "、".join(
        AUTH_TYPE_MAP.get(code, f"未知权限({code})") for code in auth_list
    )

    has_doc_content_auth = AUTH_GET_DOC_CONTENT in auth_list
    if has_doc_content_auth:
        action_hint = "用户已授予文档内容读取权限，请继续之前的文档操作。"
    elif auth_list:
        action_hint = (
            "当前授权不包含文档内容读取权限，无法继续文档操作。"
            "请引导用户授予「获取成员文档内容」权限，该权限需要向管理员申请，"
            "管理员审批通过后可使用。"
        )
    else:
        action_hint = "当前无任何文档权限，无法继续文档操作。请引导用户完成文档授权。"

    sender = body.get("from") or {}
    sender_user_id = sender.get("userid") or ""
    sender_corp_id = sender.get("corpid") or ""
    chat_id = sender.get("chat_id") or body.get("chatid") or sender_user_id

    auth_list_str = ", ".join(str(code) for code in auth_list)
    lines: List[Optional[str]] = [
        "[企业微信文档权限变更回调]",
        "event_type(事件类型): auth_change_event",
        f"auth_list(当前权限列表): [{auth_list_str}] ({auth_descriptions or '无'})",
        f"msgid(消息 id): {body['msgid']}" if body.get("msgid") else None,
        f"aibotid(机器人 id): {body['aibotid']}" if body.get("aibotid") else None,
        f"chat_type(会话类型): {body['chattype']}" if body.get("chattype") else None,
        f"chat_id(会话 id): {chat_id}" if chat_id else None,
        f"from.corpid(企业 id): {sender_corp_id}" if sender_corp_id else None,
        f"from.userid(发送人 id): {sender_user_id}" if sender_user_id else None,
        "",
        f"[操作指引] {action_hint}",
    ]
    return "\n".join(line for line in lines if line is not None)


# ── Mentions extraction ────────────────────────────────────────────────────


def _extract_mentions(text: str) -> List[str]:
    """Extract @mentioned user IDs from text.

    A mention is the token following '@' up to the next whitespace, '@', or
    sentence-punctuation character. Returns a list with duplicates preserved
    in encounter order.
    """
    if not text or "@" not in text:
        return []
    # Punctuation that terminates a mention token.
    _STOP = set(",.;:!?，。；：！？\"'()[]{}<>")
    mentions: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "@":
            j = i + 1
            while j < n and not text[j].isspace() and text[j] != "@" and text[j] not in _STOP:
                j += 1
            token = text[i + 1 : j]
            if token:
                mentions.append(token)
            i = j
        else:
            i += 1
    return mentions


# ── Main entry ──────────────────────────────────────────────────────────────


def parse_message_content(body: Dict[str, Any]) -> ParsedMessageContent:
    """Parse a WeCom message body into a structured result.

    Aligned with OpenClaw: src/message-parser.ts:parseMessageContent

    Supports:
      - text / voice / image / file / video / mixed / location / link
      - quote messages (text/voice/image/file/video)
      - @mention extraction
      - event callbacks (template_card_event, auth_change_event)
    """
    result = ParsedMessageContent()

    msgtype = body.get("msgtype", "")

    # ── Event callbacks ────────────────────────────────────────────────
    if msgtype == "event":
        auth_text = _build_auth_change_event_text(body)
        if auth_text:
            result.text_parts.append(auth_text)
            return result
        card_text = _build_template_card_event_text(body)
        if card_text:
            result.text_parts.append(card_text)
        return result

    # ── Mixed messages (text + image items) ────────────────────────────
    if msgtype == "mixed":
        mixed = body.get("mixed") or {}
        # OpenClaw TS uses `msg_item`; legacy/some adapter code uses `items`.
        msg_items = mixed.get("msg_item") or mixed.get("items") or []
        for item in msg_items:
            item_type = item.get("msgtype") or item.get("type") or ""
            if item_type == "text":
                content = ""
                if isinstance(item.get("text"), dict):
                    content = item["text"].get("content", "")
                else:
                    content = item.get("content", "")
                if content:
                    result.text_parts.append(content)
            elif item_type == "image":
                image = item.get("image") if isinstance(item.get("image"), dict) else item
                url = image.get("url", "")
                aeskey = image.get("aeskey") or image.get("aes_key", "")
                if url:
                    result.image_urls.append(url)
                    if aeskey:
                        result.image_aes_keys[url] = aeskey
    else:
        # ── Single-type messages ───────────────────────────────────────
        text_block = body.get("text") or {}
        if text_block.get("content"):
            result.text_parts.append(text_block["content"])

        # voice — content holds the speech-to-text transcription
        if msgtype == "voice":
            voice = body.get("voice") or {}
            if voice.get("content"):
                result.text_parts.append(voice["content"])

        # image
        image = body.get("image") or {}
        if image.get("url"):
            url = image["url"]
            result.image_urls.append(url)
            if image.get("aeskey"):
                result.image_aes_keys[url] = image["aeskey"]

        # file
        if msgtype == "file":
            file_block = body.get("file") or {}
            if file_block.get("url"):
                url = file_block["url"]
                result.file_urls.append(url)
                if file_block.get("aeskey"):
                    result.file_aes_keys[url] = file_block["aeskey"]

        # video — routed through file download pipeline
        if msgtype == "video":
            video = body.get("video") or {}
            if video.get("url"):
                url = video["url"]
                result.file_urls.append(url)
                if video.get("aeskey"):
                    result.file_aes_keys[url] = video["aeskey"]

        # location — not directly supported in OpenClaw TS; preserve payload
        if msgtype == "location":
            loc = body.get("location")
            if isinstance(loc, dict):
                result.location = dict(loc)
                # Build a textual summary so downstream can read it.
                parts = []
                for key in ("name", "address", "latitude", "longitude"):
                    if loc.get(key) is not None:
                        parts.append(f"{key}={loc[key]}")
                if parts:
                    result.text_parts.append("[location] " + ", ".join(parts))

        # link — title/description/url
        if msgtype == "link":
            link = body.get("link")
            if isinstance(link, dict):
                result.link = dict(link)
                title = link.get("title", "")
                desc = link.get("description", "") or link.get("desc", "")
                url = link.get("url", "")
                summary = " | ".join(p for p in (title, desc, url) if p)
                if summary:
                    result.text_parts.append(f"[link] {summary}")

    # ── Quote message ──────────────────────────────────────────────────
    quote = body.get("quote")
    if isinstance(quote, dict):
        qtype = quote.get("msgtype", "")
        if qtype == "text" and (quote.get("text") or {}).get("content"):
            result.quote_content = quote["text"]["content"]
        elif qtype == "voice" and (quote.get("voice") or {}).get("content"):
            result.quote_content = quote["voice"]["content"]
        elif qtype == "image" and (quote.get("image") or {}).get("url"):
            qimg = quote["image"]
            url = qimg["url"]
            result.image_urls.append(url)
            if qimg.get("aeskey"):
                result.image_aes_keys[url] = qimg["aeskey"]
        elif qtype == "file" and (quote.get("file") or {}).get("url"):
            qfile = quote["file"]
            url = qfile["url"]
            result.file_urls.append(url)
            if qfile.get("aeskey"):
                result.file_aes_keys[url] = qfile["aeskey"]
        elif qtype == "video" and (quote.get("video") or {}).get("url"):
            qvideo = quote["video"]
            url = qvideo["url"]
            result.file_urls.append(url)
            if qvideo.get("aeskey"):
                result.file_aes_keys[url] = qvideo["aeskey"]

    # ── Mentions ───────────────────────────────────────────────────────
    joined = " ".join(result.text_parts)
    result.mentions = _extract_mentions(joined)

    return result


# ── Backward-compatible tuple-style helper ──────────────────────────────────


def parse_message_simple(
    body: Dict[str, Any],
) -> Tuple[str, List[Dict[str, str]]]:
    """Simple wrapper returning ``(text, [{url, aes_key, filename}, ...])``.

    Kept for adapter.py back-compat with the old _parse_message_content shape.
    """
    parsed = parse_message_content(body)
    images: List[Dict[str, str]] = []
    for url in parsed.image_urls:
        images.append(
            {
                "url": url,
                "aes_key": parsed.image_aes_keys.get(url, ""),
                "filename": "image.png",
            }
        )
    return parsed.text, images
