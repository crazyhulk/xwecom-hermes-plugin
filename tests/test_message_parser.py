"""Tests for the message_parser module — aligned with OpenClaw parser."""

import os
import sys

import pytest

# Path setup so we can import the plugin in either flat-import mode
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from message_parser import (
    ParsedMessageContent,
    parse_message_content,
    parse_message_simple,
)


class TestTextMessage:
    def test_plain_text(self):
        result = parse_message_content(
            {"msgtype": "text", "text": {"content": "hello"}}
        )
        assert result.text == "hello"
        assert result.image_urls == []
        assert result.file_urls == []
        assert result.quote_content is None

    def test_empty_text(self):
        result = parse_message_content({"msgtype": "text", "text": {"content": ""}})
        assert result.text == ""
        assert result.text_parts == []


class TestImageMessage:
    def test_image_with_aeskey(self):
        result = parse_message_content(
            {
                "msgtype": "image",
                "image": {"url": "https://x/y.png", "aeskey": "K1"},
            }
        )
        assert result.image_urls == ["https://x/y.png"]
        assert result.image_aes_keys == {"https://x/y.png": "K1"}

    def test_image_without_aeskey(self):
        result = parse_message_content(
            {"msgtype": "image", "image": {"url": "https://x/y.png"}}
        )
        assert result.image_urls == ["https://x/y.png"]
        assert "https://x/y.png" not in result.image_aes_keys


class TestVoiceMessage:
    def test_voice_with_transcription(self):
        result = parse_message_content(
            {"msgtype": "voice", "voice": {"content": "spoken words"}}
        )
        assert result.text == "spoken words"

    def test_voice_without_transcription(self):
        result = parse_message_content({"msgtype": "voice", "voice": {}})
        assert result.text == ""


class TestFileMessage:
    def test_file_url_and_aeskey(self):
        result = parse_message_content(
            {
                "msgtype": "file",
                "file": {"url": "https://x/doc.pdf", "aeskey": "fk"},
            }
        )
        assert result.file_urls == ["https://x/doc.pdf"]
        assert result.file_aes_keys == {"https://x/doc.pdf": "fk"}


class TestVideoMessage:
    def test_video_routes_through_file_pipeline(self):
        result = parse_message_content(
            {
                "msgtype": "video",
                "video": {"url": "https://x/v.mp4", "aeskey": "vk"},
            }
        )
        # Video URLs go into file_urls (Aligned with OpenClaw: parser.ts)
        assert result.file_urls == ["https://x/v.mp4"]
        assert result.file_aes_keys == {"https://x/v.mp4": "vk"}


class TestMixedMessage:
    def test_mixed_with_msg_item(self):
        result = parse_message_content(
            {
                "msgtype": "mixed",
                "mixed": {
                    "msg_item": [
                        {"msgtype": "text", "text": {"content": "Look:"}},
                        {
                            "msgtype": "image",
                            "image": {"url": "https://x/1.png", "aeskey": "k1"},
                        },
                    ]
                },
            }
        )
        assert result.text == "Look:"
        assert result.image_urls == ["https://x/1.png"]
        assert result.image_aes_keys["https://x/1.png"] == "k1"

    def test_mixed_multiple_text_and_image_segments(self):
        result = parse_message_content(
            {
                "msgtype": "mixed",
                "mixed": {
                    "msg_item": [
                        {"msgtype": "text", "text": {"content": "first"}},
                        {"msgtype": "text", "text": {"content": "second"}},
                        {
                            "msgtype": "image",
                            "image": {"url": "https://a/1.png"},
                        },
                        {
                            "msgtype": "image",
                            "image": {"url": "https://a/2.png"},
                        },
                    ]
                },
            }
        )
        assert result.text_parts == ["first", "second"]
        assert result.image_urls == ["https://a/1.png", "https://a/2.png"]


class TestQuoteMessage:
    def test_quote_text(self):
        result = parse_message_content(
            {
                "msgtype": "text",
                "text": {"content": "reply"},
                "quote": {"msgtype": "text", "text": {"content": "original"}},
            }
        )
        assert result.text == "reply"
        assert result.quote_content == "original"

    def test_quote_voice(self):
        result = parse_message_content(
            {
                "msgtype": "text",
                "text": {"content": "reply"},
                "quote": {"msgtype": "voice", "voice": {"content": "spoke"}},
            }
        )
        assert result.quote_content == "spoke"

    def test_quote_image_appends_to_image_urls(self):
        result = parse_message_content(
            {
                "msgtype": "text",
                "text": {"content": "see"},
                "quote": {
                    "msgtype": "image",
                    "image": {"url": "https://q/img.png", "aeskey": "qk"},
                },
            }
        )
        assert "https://q/img.png" in result.image_urls
        assert result.image_aes_keys["https://q/img.png"] == "qk"

    def test_quote_file(self):
        result = parse_message_content(
            {
                "msgtype": "text",
                "text": {"content": "see file"},
                "quote": {
                    "msgtype": "file",
                    "file": {"url": "https://q/doc.pdf", "aeskey": "qfk"},
                },
            }
        )
        assert "https://q/doc.pdf" in result.file_urls
        assert result.file_aes_keys["https://q/doc.pdf"] == "qfk"


class TestMentions:
    def test_single_mention(self):
        result = parse_message_content(
            {"msgtype": "text", "text": {"content": "hi @alice please"}}
        )
        assert "alice" in result.mentions

    def test_multiple_mentions(self):
        result = parse_message_content(
            {
                "msgtype": "text",
                "text": {"content": "@bob and @carol, look at @bob again"},
            }
        )
        assert result.mentions == ["bob", "carol", "bob"]

    def test_no_mentions(self):
        result = parse_message_content(
            {"msgtype": "text", "text": {"content": "no at signs here"}}
        )
        assert result.mentions == []


class TestLocationMessage:
    def test_location_summary(self):
        result = parse_message_content(
            {
                "msgtype": "location",
                "location": {
                    "name": "Tower",
                    "address": "1 Main St",
                    "latitude": 39.9,
                    "longitude": 116.4,
                },
            }
        )
        assert result.location is not None
        assert result.location["name"] == "Tower"
        assert any("location" in p for p in result.text_parts)


class TestLinkMessage:
    def test_link_extraction(self):
        result = parse_message_content(
            {
                "msgtype": "link",
                "link": {
                    "title": "Title",
                    "description": "Desc",
                    "url": "https://link.example/foo",
                },
            }
        )
        assert result.link == {
            "title": "Title",
            "description": "Desc",
            "url": "https://link.example/foo",
        }
        assert any("link" in p for p in result.text_parts)


class TestTemplateCardEventParsing:
    def test_template_card_event_summary(self):
        result = parse_message_content(
            {
                "msgtype": "event",
                "msgid": "m1",
                "aibotid": "a1",
                "chattype": "single",
                "chatid": "c1",
                "from": {"userid": "u1", "corpid": "wx"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "btn",
                        "task_id": "t1",
                        "selected_items": {
                            "selected_item": [
                                {
                                    "question_key": "q1",
                                    "option_ids": {"option_id": ["o1", "o2"]},
                                }
                            ]
                        },
                    },
                },
            }
        )
        body = result.text
        assert "[企业微信模板卡片回调]" in body
        assert "card_type" in body
        assert "q1: o1, o2" in body


class TestAuthChangeEvent:
    def test_auth_change_with_get_doc_content(self):
        result = parse_message_content(
            {
                "msgtype": "event",
                "from": {"userid": "u1"},
                "event": {
                    "eventtype": "auth_change_event",
                    "auth_change_event": {"auth_list": [1, 2]},
                },
            }
        )
        body = result.text
        assert "auth_change_event" in body
        assert "获取成员文档内容" in body
        assert "用户已授予文档内容读取权限" in body

    def test_auth_change_empty_list(self):
        result = parse_message_content(
            {
                "msgtype": "event",
                "from": {"userid": "u1"},
                "event": {
                    "eventtype": "auth_change_event",
                    "auth_change_event": {"auth_list": []},
                },
            }
        )
        body = result.text
        assert "当前无任何文档权限" in body


class TestParseMessageSimple:
    def test_returns_tuple(self):
        text, images = parse_message_simple(
            {"msgtype": "text", "text": {"content": "hi"}}
        )
        assert text == "hi"
        assert images == []

    def test_image_in_tuple(self):
        text, images = parse_message_simple(
            {"msgtype": "image", "image": {"url": "https://u/i", "aeskey": "k"}}
        )
        assert text == ""
        assert images == [
            {"url": "https://u/i", "aes_key": "k", "filename": "image.png"}
        ]
