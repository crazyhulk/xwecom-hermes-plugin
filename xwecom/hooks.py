"""Tool / model lifecycle → the xwecom status area (turn bubble) and tool timer.

Hermes runs the turn (``run_sync``) on an executor thread that copies the gateway's context
(``GatewayRunner._run_in_executor_with_context``), and bounded hooks (``pre_api_request``,
``post_tool_call``) run on a helper thread that copies the caller's context again
(``plugins_dispatch``: ``contextvars.copy_context().run``). So ``get_session_env`` (published plugin API)
names the chat and inbound message on every hook here. Each hook hands the event, stamped with the time it
fired, to the live xwecom adapter on its own event loop; it never blocks or alters the call.
"""

from __future__ import annotations

import asyncio
import logging
import time
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


def _dispatch(fn: Callable[[Any, str, str, float], None]) -> None:
    if get_session_env("HERMES_SESSION_PLATFORM") != "xwecom":
        return
    chat_id = get_session_env("HERMES_SESSION_CHAT_ID")
    if not chat_id:
        return
    message_id, at = get_session_env("HERMES_SESSION_MESSAGE_ID"), time.monotonic()
    for adapter in list(_adapters):
        loop: Optional[asyncio.AbstractEventLoop] = getattr(adapter, "_loop", None)
        if loop is None or loop.is_closed():
            continue
        try:
            loop.call_soon_threadsafe(fn, adapter, chat_id, message_id, at)
        except RuntimeError:
            pass


def on_pre_tool_call(tool_name: str = "", tool_call_id: str = "", **_: Any) -> None:
    _dispatch(lambda adapter, chat, mid, at: adapter.tool_started(chat, tool_call_id, tool_name, mid, at))


def on_post_tool_call(tool_name: str = "", tool_call_id: str = "", **_: Any) -> None:
    _dispatch(lambda adapter, chat, mid, at: adapter.tool_finished(chat, tool_call_id, mid, at))


def on_pre_api_request(**_: Any) -> None:
    """Every model call of a turn restarts "💭 Thinking (Ns)". Model name / call count are never shown."""
    _dispatch(lambda adapter, chat, mid, at: adapter.llm_request_started(chat, mid, at))


def register_hooks(ctx: Any) -> None:
    ctx.register_hook("pre_tool_call", on_pre_tool_call)
    ctx.register_hook("post_tool_call", on_post_tool_call)
    ctx.register_hook("pre_api_request", on_pre_api_request)
