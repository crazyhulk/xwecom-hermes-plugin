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
