End-to-end checks that load the plugin through Hermes' real discovery against a throwaway
`HERMES_HOME` and drive it with Hermes' real components. They need a Hermes checkout on `PYTHONPATH`
and `HERMES_DISABLE_LAZY_INSTALLS=1` (otherwise core relaunches into a fresh runtime for the temp home),
and are run directly, not by pytest:

    HERMES_DISABLE_LAZY_INSTALLS=1 PYTHONPATH=/path/to/hermes-agent python tests/e2e/<name>.py

| script | covers |
|---|---|
| load_check.py | discovery, registration, capabilities, hooks, display defaults |
| consumer_e2e.py | Hermes' real `GatewayStreamConsumer` against the emulated editing surface |
| hooks_approval_e2e.py | tool hooks on worker threads, approval card click → resolved approval |
| turn_bubble_e2e.py | turn bubble scenarios (thinking, tools, redirect, notices, rescue, rotation) |
| turn_bubble_zh_e2e.py | the same with `display.language: zh` (status template matching) |
| authority_e2e.py | real `GatewayRunner` + session authority path (what a live gateway runs) |
