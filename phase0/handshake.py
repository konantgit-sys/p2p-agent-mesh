# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Phase 0 — Handshake: Ed25519 mutual auth + X25519 ECDH + ChaCha20-Poly1305.



Схема:
1. Client → Server: hello (pubkey, nonce, eph_pub)
2. Server → Client: challenge (server_pubkey, nonce, nonce_sig, eph_pub)
3. Client verifies server signed client's nonce → derives session key
4. Client → Server: auth (nonce_sig)
5. Server verifies client signed server's nonce → derives session key
6. Все последующие сообщения: ChaCha20-Poly1305 encrypted (JSON envelope)
"""

import asyncio
import json
import os

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from phase0.identity import Identity

# ─────────────────────────────────────────────
# Session: хранит ключ + nonce sequence
# ─────────────────────────────────────────────


class SecureSession:
    """Сессионный ключ + ChaCha20-Poly1305 для шифрования сообщений."""

    def __init__(self, session_key: bytes):
        assert len(session_key) == 32, "session_key must be 32 bytes"
        self._cipher = ChaCha20Poly1305(session_key)
        self._send_seq = 0
        self._recv_seq = 0
        self.peer_pubkey_hex: str | None = None

    def encrypt(self, plaintext: bytes) -> bytes:
        """Зашифровать. Возвращает nonce(12) + ciphertext+tag."""
        nonce = self._send_seq.to_bytes(4, "big") + b"\x00" * 8
        self._send_seq += 1
        ct = self._cipher.encrypt(nonce, plaintext, None)
        return nonce + ct  # nonce префиксом для дешифровки

    def decrypt(self, data: bytes) -> bytes:
        """Расшифровать. data = nonce(12) + ciphertext+tag."""
        nonce = data[:12]
        ct = data[12:]
        self._recv_seq += 1
        return self._cipher.decrypt(nonce, ct, None)

    def encrypt_json(self, msg: dict) -> bytes:
        """Зашифровать dict → bytes."""
        return self.encrypt(json.dumps(msg, separators=(",", ":")).encode() + b"\n")

    def pack_encrypted(self, plain_json: dict) -> dict:
        """Запаковать JSON-сообщение в encrypted envelope.
        Возвращает {"type":"enc","d":"<hex>"}."""
        raw = json.dumps(plain_json, separators=(",", ":")).encode() + b"\n"
        ct = self.encrypt(raw)
        return {"type": "enc", "d": ct.hex()}

    def unpack_encrypted(self, envelope: dict) -> dict:
        """Распаковать encrypted envelope → оригинальное JSON-сообщение."""
        ct = bytes.fromhex(envelope["d"])
        decrypted = self.decrypt(ct)
        return json.loads(decrypted.decode().strip())


# ─────────────────────────────────────────────
# Handshake
# ─────────────────────────────────────────────


def _generate_nonce() -> bytes:
    return os.urandom(32)


def _x25519_pub_to_hex(pub: X25519PublicKey) -> str:
    return pub.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()


def _x25519_priv_generate() -> tuple[X25519PrivateKey, str]:
    priv = X25519PrivateKey.generate()
    return priv, _x25519_pub_to_hex(priv.public_key())


def _derive_session_key(
    my_eph_priv: X25519PrivateKey,
    peer_eph_pub_hex: str,
    client_nonce: bytes,
    server_nonce: bytes,
) -> bytes:
    """ECDH + HKDF → 32 байта session key."""
    peer_raw = bytes.fromhex(peer_eph_pub_hex)
    peer_pub = X25519PublicKey.from_public_bytes(peer_raw)
    shared = my_eph_priv.exchange(peer_pub)
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=client_nonce + server_nonce,
        info=b"p2p-agent-mesh/v0.4",
    ).derive(shared)


async def server_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    identity: Identity,
) -> SecureSession | None:
    """Server-side handshake."""
    # 1. Ждём hello
    line = await reader.readline()
    if not line:
        return None
    try:
        hello = json.loads(line.decode().strip())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if hello.get("type") != "hello":
        return None

    client_pubkey_hex = hello.get("pubkey", "")
    client_nonce = bytes.fromhex(hello.get("nonce", ""))
    client_eph_pub_hex = hello.get("eph_pub", "")

    if len(client_nonce) != 32 or not client_pubkey_hex or not client_eph_pub_hex:
        return None

    # Валидация pubkey
    try:
        client_raw = bytes.fromhex(client_pubkey_hex)
        Ed25519PublicKey.from_public_bytes(client_raw)
    except (ValueError, Exception):
        return None

    # 2. Генерируем свой эфемерный ключ + nonce
    server_eph_priv, server_eph_pub_hex = _x25519_priv_generate()
    server_nonce = _generate_nonce()

    # Подписываем nonce клиента
    nonce_sig = identity.sign(client_nonce)

    challenge = {
        "type": "challenge",
        "server_pubkey": identity.public_key_hex,
        "nonce": server_nonce.hex(),
        "nonce_sig": nonce_sig,
        "eph_pub": server_eph_pub_hex,
    }
    writer.write(json.dumps(challenge, separators=(",", ":")).encode() + b"\n")
    await writer.drain()

    # 3. Ждём auth
    line = await reader.readline()
    if not line:
        return None
    try:
        auth = json.loads(line.decode().strip())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if auth.get("type") != "auth":
        return None

    client_sig = bytes.fromhex(auth.get("nonce_sig", ""))
    if not client_sig:
        return None

    # Верифицируем подпись клиента
    try:
        client_pub = Ed25519PublicKey.from_public_bytes(bytes.fromhex(client_pubkey_hex))
        client_pub.verify(client_sig, server_nonce)
    except (InvalidSignature, ValueError):
        return None

    # 4. Вычисляем сессионный ключ
    session_key = _derive_session_key(
        server_eph_priv,
        client_eph_pub_hex,
        client_nonce,
        server_nonce,
    )
    session = SecureSession(session_key)
    session.peer_pubkey_hex = client_pubkey_hex
    return session


async def client_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    identity: Identity,
    expected_server_pubkey: str | None = None,
) -> SecureSession | None:
    """Client-side handshake."""
    # 1. Отправляем hello
    client_eph_priv, client_eph_pub_hex = _x25519_priv_generate()
    client_nonce = _generate_nonce()

    hello = {
        "type": "hello",
        "pubkey": identity.public_key_hex,
        "nonce": client_nonce.hex(),
        "eph_pub": client_eph_pub_hex,
    }
    writer.write(json.dumps(hello, separators=(",", ":")).encode() + b"\n")
    await writer.drain()

    # 2. Получаем challenge
    line = await reader.readline()
    if not line:
        return None
    try:
        challenge = json.loads(line.decode().strip())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if challenge.get("type") != "challenge":
        return None

    server_pubkey_hex = challenge.get("server_pubkey", "")
    server_nonce = bytes.fromhex(challenge.get("nonce", ""))
    server_nonce_sig = bytes.fromhex(challenge.get("nonce_sig", ""))
    server_eph_pub_hex = challenge.get("eph_pub", "")

    if (
        len(server_nonce) != 32
        or not server_pubkey_hex
        or not server_nonce_sig
        or not server_eph_pub_hex
    ):
        return None

    # Проверяем pubkey сервера
    if expected_server_pubkey and server_pubkey_hex != expected_server_pubkey:
        return None

    # Верифицируем подпись сервера
    try:
        server_pub = Ed25519PublicKey.from_public_bytes(bytes.fromhex(server_pubkey_hex))
        server_pub.verify(server_nonce_sig, client_nonce)
    except (InvalidSignature, ValueError):
        return None

    # 3. Отправляем auth
    client_sig = identity.sign(server_nonce)
    auth = {"type": "auth", "nonce_sig": client_sig}
    writer.write(json.dumps(auth, separators=(",", ":")).encode() + b"\n")
    await writer.drain()

    # 4. Вычисляем сессионный ключ
    session_key = _derive_session_key(
        client_eph_priv,
        server_eph_pub_hex,
        client_nonce,
        server_nonce,
    )
    session = SecureSession(session_key)
    session.peer_pubkey_hex = server_pubkey_hex
    return session


# ─────────────────────────────────────────────
# Message detection helpers
# ─────────────────────────────────────────────


def is_encrypted_envelope(msg: dict) -> bool:
    """Проверить, является ли сообщение encrypted envelope."""
    return msg.get("type") == "enc" and "d" in msg
