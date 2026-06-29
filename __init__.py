"""xwecom — WeCom platform adapter plugin for Hermes Agent.

This __init__.py is loaded by Hermes plugin system at runtime where
gateway.* modules are available. For tests, import modules directly.
"""

import logging as _logging

try:
    from .adapter import register

    __all__ = ["register"]
except ImportError as _exc:
    # Running outside Hermes (e.g., pytest). Don't swallow silently —
    # log so a *real* missing dep is still visible.
    _logging.getLogger(__name__).debug(
        "xwecom __init__.py: skipping register import (%s)", _exc,
    )
    __all__ = []
