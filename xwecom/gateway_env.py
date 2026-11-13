"""Env -> ``PlatformConfig.extra`` seeding for the ``xwecom`` platform (``env_enablement_fn``).

Credentials and settings are read as ``XWECOM_<NAME>`` first, then the built-in adapter's
``WECOM_<NAME>``, so a profile that already runs the bundled WeCom adapter switches over without
re-entering its bot id / secret. Uses only the published ``seed_extra_from_env`` helper.
"""

from __future__ import annotations

from typing import Optional

from .compat import env_setting

# (env suffix, extra key)
_SEEDED = (
    ("BOT_ID", "bot_id"),
    ("SECRET", "secret"),
    ("WEBSOCKET_URL", "websocket_url"),
    ("DM_POLICY", "dm_policy"),
    ("GROUP_POLICY", "group_policy"),
    ("ALLOWED_USERS", "allow_from"),
)

# Authorization env names (``PlatformEntry`` fields). Prefixed with XWECOM_ so this platform's
# allowlist never silently follows the built-in adapter's; ``compat.env_setting`` still falls back for
# the adapter's own reads.
AUTHZ = {
    "allowed_users_env": "XWECOM_ALLOWED_USERS",
    "allow_all_env": "XWECOM_ALLOW_ALL_USERS",
    "own_policy_env": ("XWECOM_DM_POLICY", "XWECOM_GROUP_POLICY"),
    "allowlist_suppresses_pairing": True,
}

HOME_ENV = "XWECOM_HOME_CHANNEL"


def env_enablement() -> Optional[dict]:
    """Seed ``extra`` (and the home channel) from env; None when the bot is not configured."""
    seed = {key: value for suffix, key in _SEEDED if (value := env_setting(suffix))}
    if not (seed.get("bot_id") and seed.get("secret")):
        return None
    home = env_setting("HOME_CHANNEL")
    if home:
        seed["home_channel"] = {"chat_id": home, "name": env_setting("HOME_CHANNEL_NAME", "Home")}
    return seed
