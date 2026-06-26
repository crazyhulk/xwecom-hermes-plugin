"""
xwecom SDK - Forked from wecom-aibot-python-sdk-async.

Patched for Hermes Agent integration:
- Fixed asyncio.get_event_loop() deprecation
- Fixed duplicate websockets imports
- Added configurable ACK timeout
- Added connection state pre-checks
- Fixed dotenv dependency naming
"""

from .client import WSClient
from .types import WSClientOptions, WsCmd, MessageType, EventType, TemplateCardType
from .logger import DefaultLogger

__all__ = [
    "WSClient",
    "WSClientOptions",
    "WsCmd",
    "MessageType",
    "EventType",
    "TemplateCardType",
    "DefaultLogger",
]
