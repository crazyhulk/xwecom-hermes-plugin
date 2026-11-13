End-to-end checks that load the plugin through Hermes' real discovery against a throwaway
`HERMES_HOME` and drive it with Hermes' real `GatewayStreamConsumer` / hook dispatch / approval queue.
They need a Hermes checkout on `PYTHONPATH` and are run directly, not by pytest:

    PYTHONPATH=/path/to/hermes-agent python tests/e2e/consumer_e2e.py
