"""DM and Group access control policies — aligned with OpenClaw dm-policy.ts / group-policy.ts."""

from __future__ import annotations

import re
from typing import Dict, List, Optional


def _normalize_entry(raw: str) -> str:
    """Normalize allowlist entries (strip wecom:/user:/group: prefixes)."""
    value = str(raw).strip()
    value = re.sub(r"^wecom:", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^(user|group):", "", value, flags=re.IGNORECASE)
    return value.strip()


def _entry_matches(entries: List[str], target: str) -> bool:
    """Case-insensitive allowlist match with wildcard '*' support."""
    normalized_target = str(target).strip().lower()
    for entry in entries:
        normalized = _normalize_entry(entry).lower()
        if normalized == "*" or normalized == normalized_target:
            return True
    return False


def check_dm_policy(
    policy: str,
    allow_from: List[str],
    user_id: str,
) -> bool:
    """Check if a DM message from user_id is allowed.

    Policies:
        - "open": accept all DMs
        - "allowlist": only accept from listed users
        - "disabled": reject all DMs
        - "pairing": pass intake to Hermes, which owns pairing requests/codes
    """
    policy = policy.lower().strip()

    if policy == "disabled":
        return False
    if policy == "open":
        return True
    if policy == "pairing":
        return True
    if policy == "allowlist":
        return _entry_matches(allow_from, user_id)

    # Default: open
    return True


def check_group_policy(
    policy: str,
    group_allow_from: List[str],
    chat_id: str,
    user_id: str,
    groups_config: Optional[Dict[str, Dict]] = None,
) -> bool:
    """Check if a group message is allowed.

    Policies:
        - "open": accept all group messages
        - "allowlist": only accept from listed groups (+ per-group sender allowlist)
        - "disabled": reject all group messages
    """
    policy = policy.lower().strip()

    if policy == "disabled":
        return False
    if policy == "open":
        return True
    if policy == "allowlist":
        # Check if group is in the allow list
        if not _entry_matches(group_allow_from, chat_id):
            return False
        # Check per-group sender allowlist
        if groups_config and chat_id in groups_config:
            group_cfg = groups_config[chat_id]
            per_group_allow = group_cfg.get("allow_from", [])
            if per_group_allow:
                return _entry_matches(per_group_allow, user_id)
        return True

    # Default: open
    return True
