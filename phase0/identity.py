# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Phase 0 — Identity: Ed25519 ключи + подпись + DID.



Использует cryptography.hazmat.primitives.asymmetric.ed25519.
"""

import json
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519


class Identity:
    """Ed25519 идентичность для mesh-узла."""

    def __init__(self, private_key: ed25519.Ed25519PrivateKey | None = None):
        if private_key:
            self._private_key = private_key
        else:
            self._private_key = ed25519.Ed25519PrivateKey.generate()
        self._public_key = self._private_key.public_key()
        self.did = self._make_did()

    def _make_did(self) -> str:
        pub_hex = self._public_key.public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        ).hex()
        return f"did:snin:{pub_hex[:16]}"

    @classmethod
    def from_seed(cls, seed: bytes) -> "Identity":
        """Создать Identity из seed (32 байта)."""
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
        return cls(private_key)

    @classmethod
    def from_private_key_hex(cls, hex_key: str) -> "Identity":
        """Восстановить Identity из hex приватного ключа."""
        raw = bytes.fromhex(hex_key)
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(raw)
        return cls(private_key)

    def sign(self, data: bytes) -> str:
        """Подписать данные. Возвращает hex подписи."""
        return self._private_key.sign(data).hex()

    def sign_message(self, msg: dict) -> dict:
        """Подписать сообщение (dict), добавить signature, pubkey, from."""
        msg["from"] = self.did
        msg["pubkey"] = self.public_key_hex
        msg["ts"] = msg.get("ts", time.time())
        # Подписываем каноничный JSON
        to_sign = json.dumps(
            {k: msg[k] for k in ["topic", "payload", "from", "ts"]},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        msg["signature"] = self.sign(to_sign)
        return msg

    @staticmethod
    def verify(msg: dict) -> bool:
        """Верифицировать подпись сообщения."""
        try:
            pub_raw = bytes.fromhex(msg.get("pubkey", ""))
            sig_raw = bytes.fromhex(msg.get("signature", ""))
            public_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_raw)
            to_verify = json.dumps(
                {k: msg[k] for k in ["topic", "payload", "from", "ts"] if k in msg},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            public_key.verify(sig_raw, to_verify)
            return True
        except (InvalidSignature, KeyError, ValueError, Exception):
            return False

    @property
    def public_key_hex(self) -> str:
        return self._public_key.public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        ).hex()

    @property
    def private_key_hex(self) -> str:
        return self._private_key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        ).hex()

    def to_dict(self) -> dict:
        return {
            "did": self.did,
            "public_key": self.public_key_hex,
        }
