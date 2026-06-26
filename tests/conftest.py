"""Pytest configuration for xwecom tests."""

import os
import sys
from types import ModuleType
from unittest.mock import MagicMock

# Add the plugin root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock gateway modules that are only available inside Hermes runtime
gateway_mock = ModuleType("gateway")
gateway_config = ModuleType("gateway.config")
gateway_platforms = ModuleType("gateway.platforms")
gateway_platforms_base = ModuleType("gateway.platforms.base")
gateway_platforms_helpers = ModuleType("gateway.platforms.helpers")
gateway_status = ModuleType("gateway.status")
utils_mock = ModuleType("utils")


# Create mock classes
class MockPlatform:
    def __init__(self, name="xwecom"):
        self.name = name
        self.value = name


class MockPlatformConfig:
    def __init__(self, extra=None):
        self.extra = extra or {}


class MockBasePlatformAdapter:
    def __init__(self, config=None, platform=None):
        self._config = config
        self._platform = platform

    def _mark_connected(self):
        pass

    def _mark_disconnected(self):
        pass

    def build_source(self, **kwargs):
        return kwargs

    async def handle_message(self, event):
        pass


class MockSendResult:
    def __init__(self, success=False, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error


class MockMessageEvent:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.metadata = {}


class MockMessageType:
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"


# Wire up mocks
gateway_config.Platform = MockPlatform
gateway_config.PlatformConfig = MockPlatformConfig
gateway_platforms_base.BasePlatformAdapter = MockBasePlatformAdapter
gateway_platforms_base.MessageEvent = MockMessageEvent
gateway_platforms_base.MessageType = MockMessageType
gateway_platforms_base.SendResult = MockSendResult
gateway_platforms_base.cache_image_from_bytes = MagicMock(return_value="/tmp/cached.png")
gateway_platforms_base.cache_document_from_bytes = MagicMock(return_value="/tmp/cached.pdf")
gateway_platforms_helpers.MessageDeduplicator = MagicMock
gateway_status.acquire_scoped_lock = MagicMock(return_value=True)
gateway_status.release_scoped_lock = MagicMock()
utils_mock.env_float = MagicMock(return_value=0.0)

gateway_mock.config = gateway_config
gateway_mock.platforms = gateway_platforms
gateway_platforms.base = gateway_platforms_base
gateway_platforms.helpers = gateway_platforms_helpers

sys.modules["gateway"] = gateway_mock
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_platforms_base
sys.modules["gateway.platforms.helpers"] = gateway_platforms_helpers
sys.modules["gateway.status"] = gateway_status
sys.modules["utils"] = utils_mock
