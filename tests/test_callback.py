"""Tests for WeCom self-built app callback protocol helpers."""

import base64
import hashlib
import os
import sys

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from callback import (
    decrypt_verified_callback_message,
    decrypt_callback_message,
    extract_encrypt_from_xml,
    is_callback_timestamp_fresh,
    parse_callback_message_xml,
    verify_callback_signature,
)


def _make_test_key():
    key = os.urandom(32)
    encoding_aes_key = base64.b64encode(key).decode("ascii").rstrip("=")
    return key, encoding_aes_key


def _encrypt_callback(*, key: bytes, xml: str, corp_id: str) -> str:
    xml_bytes = xml.encode("utf-8")
    content = (
        os.urandom(16)
        + len(xml_bytes).to_bytes(4, "big")
        + xml_bytes
        + corp_id.encode("utf-8")
    )
    pad_len = 32 - (len(content) % 32)
    padded = content + bytes([pad_len]) * pad_len
    cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16]))
    encryptor = cipher.encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode("ascii")


def _signature(*, token: str, timestamp: str, nonce: str, msg_encrypt: str) -> str:
    parts = sorted([token, timestamp, nonce, msg_encrypt])
    return hashlib.sha1("".join(parts).encode("utf-8")).hexdigest()


class TestCallbackSignature:
    def test_valid_signature(self):
        token = "token"
        timestamp = "1700000000"
        nonce = "nonce"
        msg_encrypt = "AAABBB=="
        signature = _signature(
            token=token, timestamp=timestamp, nonce=nonce, msg_encrypt=msg_encrypt
        )

        assert verify_callback_signature(
            token=token,
            timestamp=timestamp,
            nonce=nonce,
            msg_encrypt=msg_encrypt,
            signature=signature,
        )

    def test_invalid_signature(self):
        assert not verify_callback_signature(
            token="wrong",
            timestamp="1700000000",
            nonce="nonce",
            msg_encrypt="AAABBB==",
            signature=_signature(
                token="token",
                timestamp="1700000000",
                nonce="nonce",
                msg_encrypt="AAABBB==",
            ),
        )

    def test_timestamp_tolerance(self):
        assert is_callback_timestamp_fresh("100", now=120, tolerance_s=30)
        assert not is_callback_timestamp_fresh("100", now=200, tolerance_s=30)
        assert not is_callback_timestamp_fresh("not-a-number", now=120)


class TestCallbackDecrypt:
    def test_decrypts_utf8_xml_and_corp_id(self):
        key, encoding_aes_key = _make_test_key()
        xml = "<xml><Content><![CDATA[你好 世界]]></Content></xml>"
        encrypted = _encrypt_callback(key=key, xml=xml, corp_id="wxCORP")

        result = decrypt_callback_message(
            encoding_aes_key=encoding_aes_key,
            encrypted=encrypted,
        )

        assert result.xml == xml
        assert result.corp_id == "wxCORP"

    def test_invalid_padding_raises(self):
        key, encoding_aes_key = _make_test_key()
        bad_plain = bytes(32)
        cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16]))
        encryptor = cipher.encryptor()
        encrypted = base64.b64encode(
            encryptor.update(bad_plain) + encryptor.finalize()
        ).decode("ascii")

        with pytest.raises(ValueError, match="Invalid PKCS7"):
            decrypt_callback_message(
                encoding_aes_key=encoding_aes_key,
                encrypted=encrypted,
            )

    def test_decrypt_verified_callback_message(self):
        key, encoding_aes_key = _make_test_key()
        token = "token"
        timestamp = "1700000000"
        nonce = "nonce"
        xml = """
        <xml>
          <FromUserName><![CDATA[lisi]]></FromUserName>
          <MsgType><![CDATA[text]]></MsgType>
          <Content><![CDATA[hello]]></Content>
          <MsgId>42</MsgId>
        </xml>
        """
        encrypted = _encrypt_callback(key=key, xml=xml, corp_id="wxCORP")
        outer_xml = f"<xml><Encrypt><![CDATA[{encrypted}]]></Encrypt></xml>"
        signature = _signature(
            token=token,
            timestamp=timestamp,
            nonce=nonce,
            msg_encrypt=encrypted,
        )

        result = decrypt_verified_callback_message(
            token=token,
            encoding_aes_key=encoding_aes_key,
            receive_id="wxCORP",
            timestamp=timestamp,
            nonce=nonce,
            msg_signature=signature,
            outer_xml=outer_xml,
            now=1700000000,
        )

        assert result.decrypted.corp_id == "wxCORP"
        assert result.parsed is not None
        assert result.parsed.text == "hello"

    def test_decrypt_verified_callback_rejects_bad_signature(self):
        key, encoding_aes_key = _make_test_key()
        encrypted = _encrypt_callback(
            key=key,
            xml="<xml><MsgType><![CDATA[text]]></MsgType></xml>",
            corp_id="wxCORP",
        )

        with pytest.raises(ValueError, match="signature mismatch"):
            decrypt_verified_callback_message(
                token="token",
                encoding_aes_key=encoding_aes_key,
                receive_id="wxCORP",
                timestamp="1700000000",
                nonce="nonce",
                msg_signature="bad",
                outer_xml=f"<xml><Encrypt>{encrypted}</Encrypt></xml>",
                now=1700000000,
            )


class TestCallbackXml:
    def test_extract_encrypt_from_outer_xml(self):
        assert (
            extract_encrypt_from_xml("<xml><Encrypt><![CDATA[cipher]]></Encrypt></xml>")
            == "cipher"
        )
        assert extract_encrypt_from_xml("<xml></xml>") is None

    def test_parse_text_message(self):
        msg = parse_callback_message_xml(
            """
            <xml>
              <FromUserName><![CDATA[lisi]]></FromUserName>
              <MsgType><![CDATA[text]]></MsgType>
              <Content><![CDATA[你好世界]]></Content>
              <MsgId>88888888</MsgId>
            </xml>
            """
        )
        assert msg is not None
        assert msg.sender_id == "lisi"
        assert msg.chat_id == "lisi"
        assert msg.text == "你好世界"
        assert msg.msg_id == "88888888"
        assert msg.is_group_chat is False

    def test_parse_image_message(self):
        msg = parse_callback_message_xml(
            """
            <xml>
              <FromUserName><![CDATA[userABC]]></FromUserName>
              <MsgType><![CDATA[image]]></MsgType>
              <MediaId><![CDATA[MEDIA_ID_001]]></MediaId>
              <MsgId>99999</MsgId>
            </xml>
            """
        )
        assert msg is not None
        assert msg.media_id == "MEDIA_ID_001"
        assert msg.media_type == "image"
        assert msg.text is None

    def test_parse_voice_message_prefers_recognition(self):
        msg = parse_callback_message_xml(
            """
            <xml>
              <FromUserName><![CDATA[voiceUser]]></FromUserName>
              <MsgType><![CDATA[voice]]></MsgType>
              <MediaId><![CDATA[VOICE_MEDIA]]></MediaId>
              <Recognition><![CDATA[今天天气怎么样]]></Recognition>
              <MsgId>55555</MsgId>
            </xml>
            """
        )
        assert msg is not None
        assert msg.media_type == "voice"
        assert msg.voice_recognition == "今天天气怎么样"
        assert msg.text == "今天天气怎么样"

    def test_parse_file_and_video_as_file_media(self):
        file_msg = parse_callback_message_xml(
            """
            <xml>
              <FromUserName><![CDATA[fileUser]]></FromUserName>
              <MsgType><![CDATA[file]]></MsgType>
              <MediaId><![CDATA[FILE_MEDIA]]></MediaId>
            </xml>
            """
        )
        video_msg = parse_callback_message_xml(
            """
            <xml>
              <FromUserName><![CDATA[videoUser]]></FromUserName>
              <MsgType><![CDATA[video]]></MsgType>
              <MediaId><![CDATA[VIDEO_MEDIA]]></MediaId>
            </xml>
            """
        )
        assert file_msg is not None
        assert file_msg.media_type == "file"
        assert file_msg.media_id == "FILE_MEDIA"
        assert video_msg is not None
        assert video_msg.media_type == "file"
        assert video_msg.media_id == "VIDEO_MEDIA"

    def test_event_empty_sender_and_unknown_type_return_none(self):
        assert (
            parse_callback_message_xml(
                "<xml><MsgType><![CDATA[event]]></MsgType></xml>"
            )
            is None
        )
        assert (
            parse_callback_message_xml(
                "<xml><MsgType><![CDATA[text]]></MsgType><FromUserName><![CDATA[]]></FromUserName></xml>"
            )
            is None
        )
        assert (
            parse_callback_message_xml(
                "<xml><MsgType><![CDATA[location]]></MsgType><FromUserName><![CDATA[u]]></FromUserName></xml>"
            )
            is None
        )
