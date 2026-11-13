"""Tool lifecycle → the xwecom tool timer.

Hermes runs tools on worker threads that copy the turn's context, so ``get_session_env`` (published plugin
API) names the chat the tool belongs to. Each hook hands the event to the live xwecom adapter on its own
event loop; it never blocks or alters the tool call.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from gateway.session_context import get_session_env

logger = logging.getLogger(__name__)

_adapters: list[Any] = []  # live adapters (one per connected xwecom profile)


def attach(adapter: Any) -> None:
    if adapter not in _adapters:
        _adapters.append(adapter)


def detach(adapter: Any) -> None:
    if adapter in _adapters:
        _adapters.remove(adapter)


def _dispatch(fn: Callable[[Any, str], None]) -> None:
    if get_session_env("HERMES_SESSION_PLATFORM") != "xwecom":
        return
    chat_id = get_session_env("HERMES_SESSION_CHAT_ID")
    if not chat_id:
        return
    for adapter in list(_adapters):
        loop: Optional[asyncio.AbstractEventLoop] = getattr(adapter, "_loop", None)
        if loop is None or loop.is_closed():
            continue
        try:
            loop.call_soon_threadsafe(fn, adapter, chat_id)
        except RuntimeError:
            pass


def on_pre_tool_call(tool_name: str = "", tool_call_id: str = "", **_: Any) -> None:
    _dispatch(lambda adapter, chat: adapter.tool_started(chat, tool_call_id, tool_name))


def on_post_tool_call(tool_name: str = "", tool_call_id: str = "", **_: Any) -> None:
    _dispatch(lambda adapter, chat: adapter.tool_finished(chat, tool_call_id))


def register_hooks(ctx: Any) -> None:
    ctx.register_hook("pre_tool_call", on_pre_tool_call)
    ctx.register_hook("post_tool_call", on_post_tool_call)
