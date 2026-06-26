"""xwecom — WeCom platform adapter plugin for Hermes Agent.

This __init__.py is loaded by Hermes plugin system at runtime where
gateway.* modules are available. For tests, import modules directly.
"""

from .adapter import register

__all__ = ["register"]
