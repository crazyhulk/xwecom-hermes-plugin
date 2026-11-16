"""Replies the plugin reported delivered but could not put on screen, kept across adapter instances.

Hermes builds a NEW adapter object when it reconnects a platform (gateway/run_adapters.py reconnect
watcher), so a reply that was still unfinished when the old connection died must not live on the old
adapter. It is parked here, keyed by bot id (one WeCom bot = one connection, whatever the profile), and
the next adapter of that bot sends it once it is connected.
"""

from __future__ import annotations

from typing import Optional

MAX_PARKED = 50  # undelivered replies kept per bot (oldest dropped beyond this)

_parked: dict[str, list[tuple[str, str]]] = {}  # bot id -> [(chat_id, text)]


def park(bot_id: str, chat_id: str, text: str) -> None:
    queue = _parked.setdefault(bot_id, [])
    queue.append((chat_id, text))
    del queue[:-MAX_PARKED]


def take(bot_id: str) -> list[tuple[str, str]]:
    return _parked.pop(bot_id, [])


def pending(bot_id: Optional[str] = None) -> int:
    return len(_parked.get(bot_id, [])) if bot_id is not None else sum(len(v) for v in _parked.values())
