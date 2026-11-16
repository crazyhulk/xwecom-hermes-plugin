"""Small stand-ins for core internals the bundled WeCom adapter imports but the plugin API does not
publish (``utils.env_float``, ``gateway.platform_env``, ``gateway.platforms.send_targets``,
``agent.i18n.t``, ``agent.ssl_verify``, ``tools.send_message_senders``).

An out-of-tree plugin may only rely on ``hermes_cli/data/plugin_api.json``; internal paths move without
notice. Everything here is either a tiny reimplementation or a best-effort import with a local fallback,
so a core refactor degrades a nicety instead of breaking plugin load.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from gateway.platforms._shared import get_scoped_secret

# Env namespace. The plugin reads XWECOM_* first and falls back to the built-in adapter's WECOM_* so an
# existing WeCom profile switches over without re-entering credentials.
ENV_PREFIXES = ("XWECOM", "WECOM")


def env_setting(suffix: str, default: str = "") -> str:
    """First non-empty ``<PREFIX>_<suffix>`` among ``ENV_PREFIXES`` (profile-scoped)."""
    for prefix in ENV_PREFIXES:
        value = str(get_scoped_secret(f"{prefix}_{suffix}", "") or "").strip()
        if value:
            return value
    return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def truthy(value: Any) -> bool:
    """Fail-closed config truthiness: only canonical tokens enable (a typo never turns a feature on)."""
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


# Catalog-free English text for the few user-facing notices the bundled adapter routes through
# ``agent.i18n.t`` with ``platform.wecom.*`` keys (those keys belong to the bundled plugin's catalog).
_TEXT = {
    "platform.xwecom.media.file_too_large": "File too large ({size_mb} MB > 20 MB), cannot send.",
    "platform.xwecom.media.voice_format_downgraded": "Voice only supports AMR ({content_type} sent as a file).",
    "platform.xwecom.media.size_downgraded": "{label} over {cap} ({size_mb} MB), sent as a file.",
    "platform.xwecom.media.kind_image": "Image",
    "platform.xwecom.media.kind_video": "Video",
    "platform.xwecom.media.kind_voice": "Voice",
}


def t(key: str, **kwargs: Any) -> str:
    """Translate through the bundled ``platform.wecom.*`` catalog when present, else local English."""
    try:
        from agent.i18n import t as core_t
        text = core_t(key.replace("platform.xwecom.", "platform.wecom."), **kwargs)
        if text and not text.startswith("platform."):
            return text
    except Exception:
        pass
    return _TEXT.get(key, key).format(**kwargs)


def certifi_cafile() -> Optional[str]:
    try:
        from agent.ssl_verify import certifi_bundle_path
        return certifi_bundle_path()
    except Exception:
        try:
            import certifi
            return certifi.where()
        except Exception:
            return None


def live_adapter(platform: Any) -> Any:
    """The running gateway's adapter for *platform* in the active profile, or None."""
    try:
        from tools.send_message_senders import _live_adapter
        return _live_adapter(platform)[1]
    except Exception:
        return None


def parse_nonempty(ref: str):
    stripped = (ref or "").strip()
    return (stripped, None) if stripped else None


def standalone_send_policy() -> Any:
    """``StandaloneSendPolicy(text_direct=True)`` when core still has it; None just keeps core defaults."""
    try:
        from gateway.platforms.send_targets import StandaloneSendPolicy
        return StandaloneSendPolicy(text_direct=True)
    except Exception:
        return None


def source_session_key(adapter: Any, source: Any) -> str:
    """The adapter-level session key of an inbound source (``BasePlatformAdapter._source_session_key``:
    chat, + sender in per-user group sessions). That method is core-private (not in plugin_api.json);
    fall back to the same shape from the source's own fields."""
    try:
        return str(adapter._source_session_key(source))
    except Exception:
        chat, chat_type = getattr(source, "chat_id", ""), str(getattr(source, "chat_type", "") or "")
        return f"{chat}:{getattr(source, 'user_id', '')}" if chat_type == "group" else str(chat)


# English copies of the core status templates the turn bubble folds (``locales/en.yaml``), used when
# ``agent.i18n`` is unavailable. Placeholders are matched as wildcards.
_STATUS_TEMPLATES_EN = {
    "gateway.progress.working_heartbeat": "⏳ Working — {minutes} min{detail}",
    "gateway.progress.stuck_warning": "⚠️ I seem to be stuck (no activity for {minutes} min). If nothing happens in the next {remaining} min I'll give up on this task. You can keep waiting, send /stop to cancel it, or /new to start a fresh conversation.",
    "gateway.exec_approval.timed_out_notice": "⌛ Approval timed out after {window} — the command was NOT run. Ask me to try again if you still want it, or raise approvals.timeout in config.yaml.",
    "gateway.busy.drain_queued": "⏳ Gateway {action} — queued for the next turn after it comes back.",
    "gateway.busy.drain_rejected": "⏳ Gateway is {action} and is not accepting another turn right now.",
    "gateway.busy.drain_rejected_new_work": "⏳ Gateway is {action} and is not accepting new work right now.",
    "gateway.busy.steered_subagents_head": "⏩ Steered into current run and its active subagent(s)",
    "gateway.busy.steered_subagents_tail": ". Your message arrives after their next tool call.",
    "gateway.busy.steered_head": "⏩ Steered into current run",
    "gateway.busy.steered_tail": ". Your message arrives after the next tool call.",
    "gateway.busy.redirected_head": "↪ Redirected current run",
    "gateway.busy.redirected_tail": ". I'll adjust using your correction.",
    "gateway.busy.subagent_working_head": "⏳ Subagent working",
    "gateway.busy.compressing_head": "⏳ Compressing context",
    "gateway.busy.demoted_tail": " — your message is queued for when it finishes (use /stop to cancel everything).",
    "gateway.busy.queued_head": "⏳ Queued for the next turn",
    "gateway.busy.queued_tail": ". I'll respond once the current task finishes.",
    "gateway.busy.interrupting_head": "⚡ Interrupting current task",
    "gateway.busy.interrupting_tail": ". I'll respond to your message shortly.",
}


def core_templates(key: str) -> list[str]:
    """The raw (unformatted) core template for *key*: the active language's and English. Read through
    ``agent.i18n.t`` (core-private) with the local English copy as fallback."""
    out: list[str] = []
    try:
        from agent.i18n import t as core_t
        for lang in (None, "en"):
            text = core_t(key) if lang is None else core_t(key, lang=lang)
            if text and text != key and text not in out:
                out.append(text)
    except Exception:
        pass
    fallback = _STATUS_TEMPLATES_EN.get(key)
    if fallback and fallback not in out:
        out.append(fallback)
    return out




# Core sends its status surfaces from these coroutines (``asyncio.current_task()`` is the coroutine that
# issued the adapter call): the tool-progress lane (``TurnRunner.send_progress_messages``, gateway/
# run_turn_progress.py) and agent status lines such as the memory-recall indicator
# (``_send_or_update_status_coro``, gateway/run.py). Identified by the call path, not by text; a core rename
# simply stops the folding (the message gets its own bubble, as without the plugin's turn bubble).
_PROGRESS_LANE = "send_progress_messages"
_STATUS_LANE = "_send_or_update_status_coro"


def core_send_lane() -> str:
    """"progress" | "status" | "" — which core status lane the current adapter call comes from."""
    try:
        import asyncio
        task = asyncio.current_task()
        name = task.get_coro().cr_code.co_name if task is not None else ""
    except Exception:
        return ""
    return {_PROGRESS_LANE: "progress", _STATUS_LANE: "status"}.get(name, "")


def tool_line_prefixes(tool_name: str) -> tuple[str, ...]:
    """How core's tool-progress line for *tool_name* starts (gateway/run_turn_progress.py
    ``_progress_build_message``): ``{emoji} {tool}`` (pending / preview / verbose / terminal-block header) or
    ``{emoji} {friendly verb}``. The emoji is the published ``tools.registry`` one (core's "⚙️" default);
    the verb comes from core-private ``agent.display`` (no fallback: without it a verb line just stays
    unmatched and the call keeps its bare label)."""
    emoji = "⚙️"
    try:
        from tools.registry import registry
        emoji = registry.get_emoji(tool_name, default="") or "⚙️"
    except Exception:
        pass
    out = [f"{emoji} {tool_name}"]
    try:
        from agent.display import get_tool_verb
        verb = get_tool_verb(tool_name)
        if verb:
            out.append(f"{emoji} {verb}")
    except Exception:
        pass
    return tuple(out)
