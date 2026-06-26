"""Tests for template_card — extraction, masking, cache, event update, send."""

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from template_card import (
    ExtractedTemplateCard,
    TemplateCardCache,
    VALID_CARD_TYPES,
    extract_template_cards,
    mask_template_card_blocks,
    process_template_cards_if_needed,
    send_template_cards,
    update_template_card_on_event,
)


def run(coro):
    return asyncio.run(coro)


# ── extract_template_cards ─────────────────────────────────────────────────


class TestExtractTemplateCards:
    def test_no_cards_in_plain_text(self):
        result = extract_template_cards("hello world, no JSON here")
        assert result.cards == []
        assert result.remaining_text == "hello world, no JSON here"

    def test_extracts_valid_card_type(self):
        card_src = (
            "Some prefix.\n\n"
            "```json\n"
            '{"card_type": "text_notice", "main_title": {"title": "T"}}\n'
            "```\n\n"
            "suffix"
        )
        result = extract_template_cards(card_src)
        assert len(result.cards) == 1
        assert result.cards[0].card_type == "text_notice"
        assert "Some prefix" in result.remaining_text
        assert "suffix" in result.remaining_text
        assert "```" not in result.remaining_text

    def test_invalid_card_type_kept_in_text(self):
        card_src = (
            '```json\n{"card_type": "made_up_type"}\n```'
        )
        result = extract_template_cards(card_src)
        assert result.cards == []
        # Invalid block stays in remaining text
        assert "made_up_type" in result.remaining_text

    def test_invalid_json_kept(self):
        text = "```json\n{not json}\n```"
        result = extract_template_cards(text)
        assert result.cards == []

    def test_multiple_cards(self):
        text = (
            '```json\n{"card_type": "text_notice"}\n```\n'
            '```json\n{"card_type": "news_notice"}\n```'
        )
        result = extract_template_cards(text)
        assert len(result.cards) == 2
        assert result.cards[0].card_type == "text_notice"
        assert result.cards[1].card_type == "news_notice"

    def test_task_id_is_normalized(self):
        text = (
            '```json\n{"card_type": "text_notice", "task_id": "my_task_99999999"}\n```'
        )
        result = extract_template_cards(text)
        assert result.cards
        tid = result.cards[0].card_json["task_id"]
        # The 8+ digit timestamp tail should have been stripped and a new ts appended
        assert tid.startswith("my_task_")
        assert tid != "my_task_99999999"

    def test_text_notice_fallback_title(self):
        text = '```json\n{"card_type": "text_notice"}\n```'
        result = extract_template_cards(text)
        assert result.cards
        c = result.cards[0].card_json
        # Either main_title or sub_title_text gets filled
        has_title = (
            isinstance(c.get("main_title"), dict)
            and c["main_title"].get("title")
        ) or c.get("sub_title_text")
        assert has_title

    def test_news_notice_main_title_added(self):
        text = '```json\n{"card_type": "news_notice"}\n```'
        result = extract_template_cards(text)
        assert result.cards
        c = result.cards[0].card_json
        assert isinstance(c["main_title"], dict)
        assert c["main_title"]["title"]
        assert isinstance(c["card_action"], dict)

    def test_vote_simplified_format(self):
        text = (
            '```json\n'
            '{"card_type": "vote_interaction", '
            '"title": "Vote", '
            '"options": [{"id": "a", "text": "A"}, {"id": "b", "text": "B"}]}\n'
            '```'
        )
        result = extract_template_cards(text)
        assert result.cards
        c = result.cards[0].card_json
        # Transformed into API format
        assert isinstance(c.get("checkbox"), dict)
        assert isinstance(c["checkbox"]["option_list"], list)
        assert c["checkbox"]["option_list"][0]["id"] == "a"
        assert "options" not in c
        # main_title constructed from "title"
        assert c["main_title"]["title"] == "Vote"
        # Submit button auto-added
        assert isinstance(c.get("submit_button"), dict)

    def test_multiple_interaction_simplified(self):
        text = (
            '```json\n'
            '{"card_type": "multiple_interaction", '
            '"title": "Pick", '
            '"selectors": [{"title": "Color", "options": [{"id": "r", "text": "Red"}]}]}\n'
            '```'
        )
        result = extract_template_cards(text)
        assert result.cards
        c = result.cards[0].card_json
        assert isinstance(c["select_list"], list)
        assert c["select_list"][0]["title"] == "Color"

    def test_checkbox_mode_alias_normalised(self):
        text = (
            '```json\n'
            '{"card_type": "vote_interaction", '
            '"options": [{"id": "a", "text": "A"}], '
            '"mode": "multi"}\n'
            '```'
        )
        result = extract_template_cards(text)
        assert result.cards
        assert result.cards[0].card_json["checkbox"]["mode"] == 1


# ── mask_template_card_blocks ──────────────────────────────────────────────


class TestMaskTemplateCardBlocks:
    def test_no_card_block_unchanged(self):
        text = "Plain text with ```json\n{\"x\":1}\n``` no card_type"
        out = mask_template_card_blocks(text)
        assert out == text

    def test_closed_card_block_masked(self):
        text = 'Hi ```json\n{"card_type": "text_notice"}\n``` bye'
        out = mask_template_card_blocks(text)
        assert "正在生成卡片消息" in out
        assert "text_notice" not in out

    def test_unclosed_card_block_trimmed(self):
        text = 'Hi ```json\n{"card_type": "text_notice", "main_title":'
        out = mask_template_card_blocks(text)
        assert "正在生成卡片消息" in out
        assert "main_title" not in out


# ── Cache ─────────────────────────────────────────────────────────────────


class TestTemplateCardCache:
    def test_save_and_get(self):
        cache = TemplateCardCache()
        card = {"card_type": "text_notice", "task_id": "t1"}
        cache.save("acc", card)
        got = cache.get("acc", "t1")
        assert got is not None
        assert got["task_id"] == "t1"
        # Returned is a deep copy
        assert got is not card

    def test_save_without_task_id_noop(self):
        cache = TemplateCardCache()
        cache.save("acc", {"card_type": "text_notice"})
        # Nothing to retrieve
        assert cache.get("acc", "") is None

    def test_per_account_isolation(self):
        cache = TemplateCardCache()
        cache.save("acc1", {"task_id": "t1", "card_type": "text_notice"})
        cache.save("acc2", {"task_id": "t1", "card_type": "news_notice"})
        a = cache.get("acc1", "t1")
        b = cache.get("acc2", "t1")
        assert a["card_type"] == "text_notice"
        assert b["card_type"] == "news_notice"

    def test_clear(self):
        cache = TemplateCardCache()
        cache.save("acc", {"task_id": "t1", "card_type": "text_notice"})
        cache.clear()
        assert cache.get("acc", "t1") is None


# ── send_template_cards ────────────────────────────────────────────────────


class _FakeClient:
    def __init__(self, *, fail_on=None):
        self.sent = []
        self.fail_on = fail_on
        self.update_calls = []

    async def send_message(self, chat_id, body):
        if self.fail_on == "send":
            raise RuntimeError("nope")
        self.sent.append((chat_id, body))
        return {"errcode": 0}

    async def update_template_card(self, frame, card, userids):
        self.update_calls.append({"card": card, "userids": userids})
        return {"errcode": 0}


class TestSendTemplateCards:
    def test_sends_each_card_and_caches(self):
        client = _FakeClient()
        cache = TemplateCardCache()
        cards = [
            ExtractedTemplateCard(
                card_json={"card_type": "text_notice", "task_id": "T1"},
                card_type="text_notice",
            ),
            ExtractedTemplateCard(
                card_json={"card_type": "news_notice", "task_id": "T2"},
                card_type="news_notice",
            ),
        ]
        frame = {"body": {"chatid": "C", "from": {"userid": "u"}}}
        success, failure = run(
            send_template_cards(
                client, frame, cards=cards, account_id="acc", cache=cache
            )
        )
        assert (success, failure) == (2, 0)
        assert len(client.sent) == 2
        # Both cached
        assert cache.get("acc", "T1") is not None
        assert cache.get("acc", "T2") is not None

    def test_failure_returns_count(self):
        client = _FakeClient(fail_on="send")
        cache = TemplateCardCache()
        cards = [
            ExtractedTemplateCard(
                card_json={"card_type": "text_notice", "task_id": "T1"},
                card_type="text_notice",
            )
        ]
        frame = {"body": {"chatid": "C", "from": {"userid": "u"}}}
        success, failure = run(
            send_template_cards(
                client, frame, cards=cards, account_id="acc", cache=cache
            )
        )
        assert (success, failure) == (0, 1)


# ── process_template_cards_if_needed ──────────────────────────────────────


class TestProcessTemplateCardsIfNeeded:
    def test_returns_none_when_no_cards(self):
        client = _FakeClient()
        cache = TemplateCardCache()
        frame = {"body": {"chatid": "C", "from": {"userid": "u"}}}
        result = run(
            process_template_cards_if_needed(
                client,
                frame,
                accumulated_text="just text",
                account_id="acc",
                cache=cache,
            )
        )
        assert result is None
        assert client.sent == []

    def test_returns_extraction_when_cards_present(self):
        client = _FakeClient()
        cache = TemplateCardCache()
        frame = {"body": {"chatid": "C", "from": {"userid": "u"}}}
        text = (
            'Here is a card:\n'
            '```json\n{"card_type": "text_notice"}\n```\n'
            'Anything else.'
        )
        result = run(
            process_template_cards_if_needed(
                client,
                frame,
                accumulated_text=text,
                account_id="acc",
                cache=cache,
            )
        )
        assert result is not None
        assert len(result.cards) == 1
        assert "```" not in result.remaining_text
        assert client.sent


# ── update_template_card_on_event ─────────────────────────────────────────


class TestUpdateTemplateCardOnEvent:
    def test_updates_disabled_state(self):
        client = _FakeClient()
        cache = TemplateCardCache()
        # Pre-seed cache with the card we sent earlier
        card = {
            "card_type": "vote_interaction",
            "task_id": "T-X",
            "checkbox": {
                "question_key": "q1",
                "option_list": [{"id": "a"}, {"id": "b"}],
            },
            "submit_button": {"text": "提交", "key": "sb"},
        }
        cache.save("acc", card)

        frame = {
            "body": {
                "chatid": "C",
                "from": {"userid": "u"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "task_id": "T-X",
                        "card_type": "vote_interaction",
                        "selected_items": {
                            "selected_item": [
                                {
                                    "question_key": "q1",
                                    "option_ids": {"option_id": ["a"]},
                                }
                            ]
                        },
                    },
                },
            }
        }

        ok = run(
            update_template_card_on_event(
                client, frame, account_id="acc", cache=cache
            )
        )
        assert ok is True
        assert len(client.update_calls) == 1
        updated = client.update_calls[0]["card"]
        assert updated["checkbox"]["disable"] is True
        assert updated["submit_button"]["text"] == "已提交"
        ck_opts = updated["checkbox"]["option_list"]
        # Option 'a' is selected, 'b' is not
        opts_by_id = {o["id"]: o for o in ck_opts}
        assert opts_by_id["a"]["is_checked"] is True
        assert opts_by_id["b"]["is_checked"] is False

    def test_no_task_id_returns_false(self):
        client = _FakeClient()
        cache = TemplateCardCache()
        frame = {
            "body": {
                "from": {"userid": "u"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {},
                },
            }
        }
        ok = run(
            update_template_card_on_event(
                client, frame, account_id="acc", cache=cache
            )
        )
        assert ok is False

    def test_cache_miss_returns_false(self):
        client = _FakeClient()
        cache = TemplateCardCache()
        frame = {
            "body": {
                "from": {"userid": "u"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {"task_id": "unknown"},
                },
            }
        }
        ok = run(
            update_template_card_on_event(
                client, frame, account_id="acc", cache=cache
            )
        )
        assert ok is False


# ── Valid card types const ─────────────────────────────────────────────────


class TestValidCardTypes:
    def test_const_has_expected_types(self):
        for t in (
            "text_notice",
            "news_notice",
            "button_interaction",
            "vote_interaction",
            "multiple_interaction",
        ):
            assert t in VALID_CARD_TYPES
