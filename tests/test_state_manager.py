"""Tests for state_manager — TTL, eviction, reqid store, sessionChatInfo."""

import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from state_manager import (
    MessageState,
    SessionChatInfo,
    StateManager,
    get_state_manager,
    reset_state_manager,
)


class TestStateManagerLifecycle:
    def test_singleton_returns_same_instance(self):
        a = get_state_manager()
        b = get_state_manager()
        assert a is b

    def test_reset_replaces_singleton(self):
        a = get_state_manager()
        reset_state_manager()
        b = get_state_manager()
        assert a is not b


class TestWSClientRegistry:
    def setup_method(self):
        reset_state_manager()
        self.mgr = get_state_manager()

    def test_set_and_get_client(self):
        client = object()
        self.mgr.set_ws_client("acc1", client)
        assert self.mgr.get_ws_client("acc1") is client

    def test_delete_client(self):
        self.mgr.set_ws_client("acc1", object())
        self.mgr.delete_ws_client("acc1")
        assert self.mgr.get_ws_client("acc1") is None

    def test_missing_client_returns_none(self):
        assert self.mgr.get_ws_client("nope") is None


class TestMessageStateStore:
    def setup_method(self):
        reset_state_manager()
        self.mgr = get_state_manager()

    def test_set_and_get(self):
        st = MessageState(stream_id="s1")
        self.mgr.set_message_state("m1", st)
        assert self.mgr.get_message_state("m1") is st

    def test_delete(self):
        self.mgr.set_message_state("m1", MessageState())
        self.mgr.delete_message_state("m1")
        assert self.mgr.get_message_state("m1") is None

    def test_clear_all(self):
        self.mgr.set_message_state("m1", MessageState())
        self.mgr.set_message_state("m2", MessageState())
        self.mgr.clear_all_message_states()
        assert self.mgr.get_message_state("m1") is None
        assert self.mgr.get_message_state("m2") is None

    def test_ttl_expiry(self):
        mgr = StateManager(ttl_ms=50)
        mgr.set_message_state("m1", MessageState())
        assert mgr.get_message_state("m1") is not None
        time.sleep(0.1)
        assert mgr.get_message_state("m1") is None

    def test_capacity_eviction(self):
        mgr = StateManager(ttl_ms=60_000, max_size=3)
        for i in range(5):
            mgr.set_message_state(f"m{i}", MessageState())
            time.sleep(0.001)
        # Only the 3 newest remain
        survivors = [f"m{i}" for i in range(5) if mgr.get_message_state(f"m{i}")]
        assert len(survivors) == 3
        # Specifically, the oldest two should be gone
        assert mgr.get_message_state("m0") is None
        assert mgr.get_message_state("m1") is None


class TestReqIdStore:
    def setup_method(self):
        reset_state_manager()
        self.mgr = get_state_manager()

    def test_set_and_get(self):
        self.mgr.set_reqid_for_chat("chat1", "req-A", account_id="acc1")
        assert self.mgr.get_reqid_for_chat("chat1", account_id="acc1") == "req-A"

    def test_isolation_between_accounts(self):
        self.mgr.set_reqid_for_chat("chat1", "req-A", account_id="acc1")
        self.mgr.set_reqid_for_chat("chat1", "req-B", account_id="acc2")
        assert self.mgr.get_reqid_for_chat("chat1", account_id="acc1") == "req-A"
        assert self.mgr.get_reqid_for_chat("chat1", account_id="acc2") == "req-B"

    def test_delete(self):
        self.mgr.set_reqid_for_chat("c", "r", "acc")
        self.mgr.delete_reqid_for_chat("c", "acc")
        assert self.mgr.get_reqid_for_chat("c", "acc") is None

    def test_generate_req_id_format(self):
        rid = StateManager.generate_req_id("foo")
        assert rid.startswith("foo_")
        assert len(rid) > 4


class TestSessionChatInfo:
    def setup_method(self):
        reset_state_manager()
        self.mgr = get_state_manager()

    def test_set_and_get(self):
        info = SessionChatInfo(chat_id="ChatX", chat_type="group")
        self.mgr.set_session_chat_info("sk1", info)
        got = self.mgr.get_session_chat_info("sk1")
        assert got is info
        assert got.chat_id == "ChatX"
        assert got.chat_type == "group"

    def test_get_with_none_key(self):
        assert self.mgr.get_session_chat_info(None) is None

    def test_delete(self):
        self.mgr.set_session_chat_info(
            "sk1", SessionChatInfo(chat_id="x", chat_type="single")
        )
        self.mgr.delete_session_chat_info("sk1")
        assert self.mgr.get_session_chat_info("sk1") is None


class TestConnectionState:
    def setup_method(self):
        reset_state_manager()
        self.mgr = get_state_manager()

    def test_set_and_get(self):
        self.mgr.set_connection_state("acc1", "connected")
        assert self.mgr.get_connection_state("acc1") == "connected"
        self.mgr.set_connection_state("acc1", "disconnected")
        assert self.mgr.get_connection_state("acc1") == "disconnected"


class TestCleanupAccount:
    def setup_method(self):
        reset_state_manager()
        self.mgr = get_state_manager()

    def test_cleanup_calls_disconnect_and_clears(self):
        class _FakeClient:
            def __init__(self):
                self.disconnected = False

            def disconnect(self):
                self.disconnected = True

        client = _FakeClient()
        self.mgr.set_ws_client("acc", client)
        asyncio.run(self.mgr.cleanup_account("acc"))
        assert client.disconnected is True
        assert self.mgr.get_ws_client("acc") is None

    def test_cleanup_all_clears_everything(self):
        self.mgr.set_message_state("m1", MessageState())
        self.mgr.set_reqid_for_chat("c", "r", "acc")
        self.mgr.set_session_chat_info(
            "sk", SessionChatInfo(chat_id="x", chat_type="single")
        )
        asyncio.run(self.mgr.cleanup_all())
        assert self.mgr.get_message_state("m1") is None
        assert self.mgr.get_reqid_for_chat("c", "acc") is None
        assert self.mgr.get_session_chat_info("sk") is None
