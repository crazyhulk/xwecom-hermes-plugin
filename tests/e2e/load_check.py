"""Load the xwecom plugin through Hermes' real discovery against a throwaway HERMES_HOME."""
import os, shutil, tempfile, pathlib

home = pathlib.Path(tempfile.mkdtemp(prefix="xw-home-"))
os.environ["HERMES_HOME"] = str(home)
(home / "plugins").mkdir()
shutil.copytree(pathlib.Path(__file__).resolve().parents[2] / "xwecom", home / "plugins" / "xwecom")
(home / "config.yaml").write_text("plugins:\n  enabled:\n    - xwecom\n")
(home / ".env").write_text("WECOM_BOT_ID=test-bot\nWECOM_SECRET=test-secret\n")
os.environ.update(WECOM_BOT_ID="test-bot", WECOM_SECRET="test-secret")  # the gateway loads .env at startup

from hermes_cli.plugins import discover_plugins, get_plugin_manager
discover_plugins(force=True)
mgr = get_plugin_manager()
loaded = {k: (p.enabled, p.error) for k, p in mgr._plugins.items() if "xwecom" in k}
print("plugin:", loaded)

from gateway.platform_registry import platform_registry
entry = platform_registry.get("xwecom")
print("registered:", entry is not None, "| tier:", entry.display_tier, "| source:", entry.source)
from gateway.config import PlatformConfig
seed = entry.env_enablement_fn()
print("env seed (WECOM_* fallback):", {k: ("***" if k == "secret" else v) for k, v in (seed or {}).items()})
adapter = platform_registry.create_adapter("xwecom", PlatformConfig(enabled=True, extra=seed))
print("adapter:", type(adapter).__name__, "| platform:", adapter.platform.value)
for attr in ("SUPPORTS_MESSAGE_EDITING", "REQUIRES_EDIT_FINALIZE", "SUPPORTS_NATIVE_STREAMING"):
    print(f"  {attr} =", getattr(adapter, attr, None))
print("  exec approval buttons:", type(adapter).supports_exec_approval_buttons())
print("  hooks:", [h for h in ("pre_tool_call", "post_tool_call", "pre_api_request") if any("xwecom" in str(getattr(cb, "__module__", "")) for cb in mgr._hooks.get(h, []))])
assert all(any("xwecom" in str(getattr(cb, "__module__", "")) for cb in mgr._hooks.get(h, []))
           for h in ("pre_tool_call", "post_tool_call", "pre_api_request")), "a plugin hook is not registered"
from gateway.display_config import _platform_defaults
d = _platform_defaults("xwecom")
print("  display defaults:", {k: d.get(k) for k in ("tool_progress", "streaming", "interim_assistant_messages")})
shutil.rmtree(home)
