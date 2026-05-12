# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Relay Client — подключение к relay ноде для NAT traversal.



Agent → Relay (TLS) → Agent
Сообщения E2E зашифрованы, relay не видит контент.
"""

import asyncio
import json
import os
import random
import sys
from collections.abc import Callable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from phase0.handshake import (
    SecureSession,
    client_handshake,
    is_encrypted_envelope,
)
from phase0.identity import Identity


class RelayClient:
    """Клиент для подключения к relay серверу.

    API:
        register() — подключиться и зарегистрироваться
        send(target_pubkey, data) — отправить E2E сообщение
        e2e_establish(target_pubkey) — установить E2E ключ
        on_recv(callback) — получать входящие E2E сообщения
        peers() — список подключённых агентов
        stop() — отключиться
    """

    SOFTMAX_BASE: float = 1.0
    SOFTMAX_MAX: float = 30.0

    @staticmethod
    def _compute_backoff(attempt: int) -> float:
        """Softmax-style backoff: avoids thundering herd without full randomness."""
        exp_delay = RelayClient.SOFTMAX_BASE * (2 ** min(attempt, 5))  # cap at 2^5 = 32x
        jitter = random.uniform(0.8, 1.2)  # ±20%
        return min(RelayClient.SOFTMAX_MAX, exp_delay * jitter)

    def __init__(
        self,
        identity: Identity,
        relay_host: str,
        relay_port: int,
        capabilities: list[str] | None = None,
    ):
        self._identity = identity
        self._relay_host = relay_host
        self._relay_port = relay_port
        self._capabilities = capabilities or []
        self._relay_session: SecureSession | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._running = False
        self._recv_callbacks: list[Callable] = []
        self._peers: list[dict] = []
        self._e2e_sessions: dict[str, SecureSession] = {}  # peer_pubkey → session
        self._e2e_pending: dict[str, asyncio.Future] = {}  # peer_pubkey → future
        self._reconnect_attempt: int = 0

    async def connect(self) -> bool:
        """Подключиться к relay + handshake + register."""
        # Сброс счётчика при успешном connect (вызвано вручную)
        self._reconnect_attempt = 0
        return await self._do_connect()

    async def _do_connect(self) -> bool:
        """Внутренняя логика подключения (с возможностью retry)."""
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._relay_host, self._relay_port
            )
        except (OSError, ConnectionRefusedError) as e:
            print(f"[relay] Connection failed: {e}")
            return False

        # Handshake с relay (mutual auth)
        self._relay_session = await client_handshake(self._reader, self._writer, self._identity)
        if self._relay_session is None:
            print("[relay] Handshake with relay failed")
            return False

        # Register
        await self._send(
            {
                "type": "register",
                "pubkey": self._identity.public_key_hex,
                "capabilities": self._capabilities,
            }
        )

        # Ждём registered + peers
        self._running = True
        asyncio.create_task(self._read_loop())
        return True

    async def register(self) -> bool:
        """Алиас для connect."""
        return await self.connect()

    async def stop(self):
        """Отключиться от relay."""
        self._running = False
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass

    def on_recv(self, callback: Callable):
        """Подписаться на входящие E2E сообщения.
        callback(from_pubkey: str, data: str, e2e_session: SecureSession)
        """
        self._recv_callbacks.append(callback)

    async def e2e_establish(self, target_pubkey: str) -> SecureSession | None:
        """Установить E2E сессию с целевым агентом через relay.

        1. A → Relay: e2e_init (с эфемерным X25519 ключом)
        2. Relay → B: e2e_req
        3. B → Relay: e2e_accept (со своим эфемерным ключом)
        4. Relay → A: e2e_ready
        5. Оба вычисляют ECDH → session key
        """
        if target_pubkey in self._e2e_sessions:
            return self._e2e_sessions[target_pubkey]

        # Генерируем эфемерный X25519 ключ
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric.x25519 import (
            X25519PrivateKey,
        )
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF

        eph_priv = X25519PrivateKey.generate()
        eph_pub = (
            eph_priv.public_key()
            .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
            .hex()
        )

        # Создаём future для ожидания ответа
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._e2e_pending[target_pubkey] = future

        # Отправляем e2e_init
        await self._send(
            {
                "type": "e2e_init",
                "target": target_pubkey,
                "eph_pub": eph_pub,
            }
        )

        # Ждём e2e_ready (timeout 15 сек)
        try:
            peer_eph_pub_hex = await asyncio.wait_for(future, 15.0)
        except TimeoutError:
            self._e2e_pending.pop(target_pubkey, None)
            return None
        finally:
            self._e2e_pending.pop(target_pubkey, None)

        # Вычисляем E2E session key
        from cryptography.hazmat.primitives.asymmetric.x25519 import (
            X25519PublicKey,
        )

        peer_raw = bytes.fromhex(peer_eph_pub_hex)
        peer_pub = X25519PublicKey.from_public_bytes(peer_raw)
        shared = eph_priv.exchange(peer_pub)

        # Используем pubkeys как salt (как в handshake.py)
        client_nonce = bytes.fromhex(self._identity.public_key_hex[:32].ljust(64, "0"))
        server_nonce = bytes.fromhex(target_pubkey[:32].ljust(64, "0"))

        session_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=client_nonce + server_nonce,
            info=b"p2p-agent-mesh/e2e/v0.4.1",
        ).derive(shared)

        session = SecureSession(session_key)
        session.peer_pubkey_hex = target_pubkey
        self._e2e_sessions[target_pubkey] = session
        return session

    async def send(self, target_pubkey: str, data: bytes) -> bool:
        """Отправить E2E зашифрованное сообщение агенту через relay.

        Шифрует data E2E ключом, relay видит только hex блоб.
        """
        # Получаем или устанавливаем E2E сессию
        e2e = self._e2e_sessions.get(target_pubkey)
        if e2e is None:
            e2e = await self.e2e_establish(target_pubkey)
        if e2e is None:
            return False

        # Шифруем E2E ключом
        encrypted = e2e.encrypt(data)
        encrypted_hex = encrypted.hex()

        await self._send(
            {
                "type": "send",
                "target": target_pubkey,
                "data": encrypted_hex,
            }
        )
        return True

    def peers(self) -> list[dict]:
        """Список подключённых агентов (кроме себя)."""
        return list(self._peers)

    # ───────────────────────── Internal ─────────────────────────

    def _handle_e2e_req(self, msg: dict):
        """Обработка e2e_req от relay (кто-то хочет установить E2E с нами)."""
        from_pubkey = msg["from"]
        peer_eph_pub = msg["eph_pub"]

        # Генерируем свой эфемерный ключ
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric.x25519 import (
            X25519PrivateKey,
            X25519PublicKey,
        )
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF

        eph_priv = X25519PrivateKey.generate()
        eph_pub_hex = (
            eph_priv.public_key()
            .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
            .hex()
        )

        # Вычисляем E2E session key
        peer_raw = bytes.fromhex(peer_eph_pub)
        peer_pub = X25519PublicKey.from_public_bytes(peer_raw)
        shared = eph_priv.exchange(peer_pub)

        client_nonce = bytes.fromhex(from_pubkey[:32].ljust(64, "0"))
        server_nonce = bytes.fromhex(self._identity.public_key_hex[:32].ljust(64, "0"))

        session_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=client_nonce + server_nonce,
            info=b"p2p-agent-mesh/e2e/v0.4.1",
        ).derive(shared)

        e2e = SecureSession(session_key)
        e2e.peer_pubkey_hex = from_pubkey
        self._e2e_sessions[from_pubkey] = e2e

        # Отправляем e2e_accept обратно через relay
        asyncio.create_task(
            self._send(
                {
                    "type": "e2e_accept",
                    "target": from_pubkey,
                    "eph_pub": eph_pub_hex,
                }
            )
        )

    async def _read_loop(self):
        """Фоновый цикл чтения сообщений от relay.
        При разрыве — переподключается с softmax backoff.
        """
        while self._running:
            try:
                line = await self._reader.readline()
                if not line:
                    # Сброс счётчика при успешном цикле
                    self._reconnect_attempt = 0
                    break

                msg = json.loads(line.decode().strip())
                if is_encrypted_envelope(msg):
                    msg = self._relay_session.unpack_encrypted(msg)

                msg_type = msg.get("type", "")

                if msg_type == "registered":
                    pass  # OK

                elif msg_type == "peers":
                    self._peers = msg.get("peers", [])

                elif msg_type == "e2e_req":
                    self._handle_e2e_req(msg)

                elif msg_type == "e2e_ready":
                    # Наш e2e_init подтверждён
                    from_pubkey = msg["from"]
                    peer_eph_pub = msg["eph_pub"]
                    future = self._e2e_pending.get(from_pubkey)
                    if future and not future.done():
                        future.set_result(peer_eph_pub)

                elif msg_type == "recv":
                    # Входящее E2E сообщение
                    from_pubkey = msg["from"]
                    data_hex = msg["data"]

                    # Пробуем расшифровать E2E ключом
                    e2e = self._e2e_sessions.get(from_pubkey)
                    if e2e:
                        try:
                            ct = bytes.fromhex(data_hex)
                            decrypted = e2e.decrypt(ct)
                            for cb in self._recv_callbacks:
                                cb(from_pubkey, decrypted, e2e)
                        except Exception:
                            pass  # Не смогли расшифровать

                elif msg_type == "pong":
                    pass  # keepalive

                elif msg_type == "error":
                    print(f"[relay] Error from relay: {msg.get('message', '')}")

            except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                pass
            except asyncio.CancelledError:
                break
            except Exception:
                break

        # Reconnect with softmax backoff при разрыве
        if self._running:
            self._reconnect_attempt += 1
            delay = self._compute_backoff(self._reconnect_attempt)
            print(f"[relay] Disconnected. Reconnect #{self._reconnect_attempt} in {delay:.1f}s")
            await asyncio.sleep(delay)
            asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        """Цикл переподключения с нарастающим backoff."""
        while self._running:
            ok = await self._do_connect()
            if ok:
                self._reconnect_attempt = 0
                return
            self._reconnect_attempt += 1
            delay = self._compute_backoff(self._reconnect_attempt)
            print(f"[relay] Reconnect failed. Retry #{self._reconnect_attempt} in {delay:.1f}s")
            await asyncio.sleep(delay)

    async def _send(self, msg: dict):
        """Отправить сообщение relay (зашифрованное relay session key)."""
        if not self._relay_session:
            return
        envelope = self._relay_session.pack_encrypted(msg)
        data = json.dumps(envelope, separators=(",", ":")) + "\n"
        try:
            self._writer.write(data.encode())
            await self._writer.drain()
        except Exception:
            pass
