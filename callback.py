"""WeCom self-built app callback helpers.

Aligned with openclaw-plugin-wecom:
  - wecom/callback-crypto.js
  - wecom/callback-inbound.js:parseCallbackMessageXml

The adapter wires these primitives into an optional aiohttp callback listener;
keeping the crypto/XML code here makes it independently testable.
"""

from __future__ import annotations

import base64
import hashlib
import re
import time
from dataclasses import dataclass
from typing import Optional

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


CALLBACK_TIMESTAMP_TOLERANCE_S = 300


@dataclass
class DecryptedCallbackMessage:
    xml: str
    corp_id: str


@dataclass
class ParsedCallbackMessage:
    msg_id: str
    sender_id: str
    chat_id: str
    is_group_chat: bool
    text: Optional[str] = None
    media_id: Optional[str] = None
    media_type: Optional[str] = None
    voice_recognition: Optional[str] = None


@dataclass
class VerifiedCallbackMessage:
    """Decrypted and parsed callback payload for one configured WeCom app."""

    decrypted: DecryptedCallbackMessage
    parsed: Optional[ParsedCallbackMessage]


def verify_callback_signature(
    *,
    token: str,
    timestamp: str,
    nonce: str,
    msg_encrypt: str,
    signature: str,
) -> bool:
    """Verify WeCom callback msg_signature.

    Signature algorithm:
        SHA1(sort([token, timestamp, nonce, msgEncrypt]).join(""))
    """
    items = [str(token), str(timestamp), str(nonce), str(msg_encrypt)]
    digest = hashlib.sha1("".join(sorted(items)).encode("utf-8")).hexdigest()
    return digest == str(signature)


def is_callback_timestamp_fresh(
    timestamp: str,
    *,
    now: Optional[float] = None,
    tolerance_s: int = CALLBACK_TIMESTAMP_TOLERANCE_S,
) -> bool:
    """Return False when timestamp is missing, invalid, or outside tolerance."""
    try:
        ts = float(timestamp)
    except (TypeError, ValueError):
        return False
    current = time.time() if now is None else now
    return abs(current - ts) <= tolerance_s


def _decode_encoding_aes_key(encoding_aes_key: str) -> bytes:
    if not encoding_aes_key:
        raise ValueError("encodingAESKey is required")
    padded = encoding_aes_key + "=" * ((4 - len(encoding_aes_key) % 4) % 4)
    key = base64.b64decode(padded)
    if len(key) != 32:
        raise ValueError(f"encodingAESKey decoded to {len(key)} bytes, expected 32")
    return key


def _strip_pkcs7_padding(data: bytes) -> bytes:
    if not data:
        raise ValueError("Decrypted content is empty")
    pad_len = data[-1]
    if pad_len < 1 or pad_len > 32 or pad_len > len(data):
        raise ValueError(f"Invalid PKCS7 padding byte: {pad_len}")
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        raise ValueError("Invalid PKCS7 padding bytes")
    return data[:-pad_len]


def decrypt_callback_message(
    *,
    encoding_aes_key: str,
    encrypted: str,
) -> DecryptedCallbackMessage:
    """Decrypt a WeCom AES-256-CBC callback message.

    Plaintext layout after unpadding:
        16 random bytes | 4-byte big-endian msg length | msg xml | corp id
    """
    key = _decode_encoding_aes_key(encoding_aes_key)
    iv = key[:16]
    ciphertext = base64.b64decode(encrypted)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(ciphertext) + decryptor.finalize()
    content = _strip_pkcs7_padding(decrypted)

    if len(content) < 20:
        raise ValueError("Decrypted content too short")
    msg_len = int.from_bytes(content[16:20], "big")
    msg_start = 20
    msg_end = msg_start + msg_len
    if len(content) < msg_end:
        raise ValueError(f"Decrypted content shorter than declared msgLen ({msg_len})")

    xml = content[msg_start:msg_end].decode("utf-8")
    corp_id = content[msg_end:].decode("utf-8")
    return DecryptedCallbackMessage(xml=xml, corp_id=corp_id)


def decrypt_verified_callback_message(
    *,
    token: str,
    encoding_aes_key: str,
    receive_id: str,
    timestamp: str,
    nonce: str,
    msg_signature: str,
    outer_xml: str,
    now: Optional[float] = None,
    enforce_fresh_timestamp: bool = True,
) -> VerifiedCallbackMessage:
    """Verify, decrypt, and parse a WeCom callback request body."""
    encrypted = extract_encrypt_from_xml(outer_xml)
    if not encrypted:
        raise ValueError("Callback body missing Encrypt")
    if enforce_fresh_timestamp and not is_callback_timestamp_fresh(timestamp, now=now):
        raise ValueError("Callback timestamp outside tolerance")
    if not verify_callback_signature(
        token=token,
        timestamp=timestamp,
        nonce=nonce,
        msg_encrypt=encrypted,
        signature=msg_signature,
    ):
        raise ValueError("Callback signature mismatch")

    decrypted = decrypt_callback_message(
        encoding_aes_key=encoding_aes_key,
        encrypted=encrypted,
    )
    if receive_id and decrypted.corp_id != receive_id:
        raise ValueError("Callback receive_id mismatch")
    return VerifiedCallbackMessage(
        decrypted=decrypted,
        parsed=parse_callback_message_xml(decrypted.xml),
    )


def extract_xml_value(xml: str, tag: str) -> Optional[str]:
    """Extract a CDATA or plain XML element value from WeCom callback XML."""
    cdata = re.search(
        rf"<{re.escape(tag)}><!\[CDATA\[([\s\S]*?)\]\]></{re.escape(tag)}>",
        xml,
    )
    if cdata:
        return cdata.group(1)
    plain = re.search(rf"<{re.escape(tag)}>([\s\S]*?)</{re.escape(tag)}>", xml)
    return plain.group(1) if plain else None


def extract_encrypt_from_xml(xml: str) -> Optional[str]:
    """Extract <Encrypt> from the outer WeCom callback XML wrapper."""
    return extract_xml_value(xml, "Encrypt")


def parse_callback_message_xml(xml: str) -> Optional[ParsedCallbackMessage]:
    """Parse decrypted WeCom callback XML into a normalized message.

    Events and unsupported message types return None, matching the OpenClaw
    callback inbound path.
    """
    msg_type = extract_xml_value(xml, "MsgType")
    if not msg_type or msg_type == "event":
        return None

    sender_id = extract_xml_value(xml, "FromUserName") or ""
    if not sender_id:
        return None

    msg_id = extract_xml_value(xml, "MsgId") or str(int(time.time() * 1000))
    chat_id = sender_id
    text: Optional[str] = None
    media_id: Optional[str] = None
    media_type: Optional[str] = None
    voice_recognition: Optional[str] = None

    if msg_type == "text":
        text = extract_xml_value(xml, "Content") or ""
    elif msg_type == "image":
        media_id = extract_xml_value(xml, "MediaId")
        media_type = "image"
    elif msg_type == "voice":
        media_id = extract_xml_value(xml, "MediaId")
        media_type = "voice"
        voice_recognition = extract_xml_value(xml, "Recognition")
        text = voice_recognition or None
    elif msg_type == "file":
        media_id = extract_xml_value(xml, "MediaId")
        media_type = "file"
    elif msg_type == "video":
        media_id = extract_xml_value(xml, "MediaId")
        media_type = "file"
    else:
        return None

    return ParsedCallbackMessage(
        msg_id=msg_id,
        sender_id=sender_id,
        chat_id=chat_id,
        is_group_chat=False,
        text=text,
        media_id=media_id,
        media_type=media_type,
        voice_recognition=voice_recognition,
    )
