# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Phase 0 — Transport: лёгкий P2P pub/sub. Zero external dependencies.



Same-process: глобальный relay для обмена между инстансами.
Multi-process: TCP соединения между узлами (JSON lines over TCP).
v0.4: опциональное шифрование ChaCha20-Poly1305 (MESH_TLS=1).

API совместим с IPFSTransport — AgentMesh не требует изменений.
"""

import asyncio
import base64
import json
import uuid
from collections.abc import Callable

from phase0.handshake import (
    SecureSession,
    client_handshake,
    is_encrypted_envelope,
    server_handshake,
)
from phase0.identity import Identity

# === Внутренний message bus для same-process pub/sub ===
_bus: dict[str, list[tuple[str, Callable]]] = {}
_bus_lock = asyncio.Lock()


class P2PTransport:
    """Лёгкий P2P транспорт. Zero external dependencies.

    Два слоя:
    1. In-memory _bus — обмен между инстансами в одном процессе
    2. TCP — обмен между процессами/машинами (JSON lines, base64 payload)

    Опционально: TLS-шифрование (MESH_TLS=1).
    """

    def __init__(
        self,
        node_id: str | None = None,
        bootstrap_peers: list[str] | None = None,
        identity: Identity | None = None,
        use_tls: bool = False,
        relay: bool = False,
    ):
        self.node_id = node_id or f"node_{uuid.uuid4().hex[:8]}"
        self._subscribed_topics: set[str] = set()
        self._running = False
        self.peer_id: str | None = None

        # TLS / Identity
        self._identity = identity or Identity()
        self._use_tls = use_tls

        # Relay mode — форвардить полученные сообщения всем TCP пирам
        self._relay = relay

        # TCP слой
        self._tcp_server: asyncio.Server | None = None
        self._tcp_port: int = 0
        self._tcp_connections: dict[str, asyncio.StreamWriter] = {}  # peer_id -> writer
        self._tcp_readers: dict[str, asyncio.StreamReader] = {}  # peer_id -> reader
        self._tcp_peer_addrs: dict[str, tuple[str, int]] = {}  # peer_id -> (host, port)
        self._reconnect_tasks: dict[str, asyncio.Task] = {}  # peer_id -> task
        self._bootstrap_peers: list[str] = bootstrap_peers or []

        # TLS sessions (peer_pubkey_prefix -> SecureSession)
        self._tls_sessions: dict[str, SecureSession] = {}

    async def start(self, host: str = "127.0.0.1", port: int = 0):
        """Запустить транспорт + TCP сервер на host:port (0 = случайный)."""
        self._running = True
        self.peer_id = f"did:p2p:{self.node_id}"

        # Старт TCP сервера
        self._tcp_server = await asyncio.start_server(self._handle_tcp_client, host, port)
        self._tcp_port = self._tcp_server.sockets[0].getsockname()[1]

        tls_mode = "TLS" if self._use_tls else "PLAIN"
        print(
            f"[transport] Started. PeerID: {self.peer_id[:20]}... "
            f"TCP: {host}:{self._tcp_port} [{tls_mode}]"
        )

        # Подключение к bootstrap пирам
        for peer_spec in self._bootstrap_peers:
            parsed = self._parse_peer_spec(peer_spec)
            if parsed:
                peer_id, remote_host, remote_port = parsed
                self._start_reconnect_loop(peer_id, remote_host, remote_port)

        return self.peer_id

    async def publish(self, topic: str, data: bytes) -> None:
        """Опубликовать сообщение. В _bus + broadcast по TCP."""
        if not data.endswith(b"\n"):
            data = data + b"\n"

        # 1. Local bus
        async with _bus_lock:
            for subscriber_id, callback in _bus.get(topic, []):
                try:
                    callback(data)
                except Exception as e:
                    print(f"[transport] callback error on {topic}: {e}")

        # 2. TCP broadcast
        await self._tcp_broadcast(topic, data.rstrip(b"\n"))

    async def subscribe(self, topic: str, callback: Callable) -> None:
        """Подписаться на топик."""
        if topic in self._subscribed_topics:
            print(f"[transport] subscribe SKIP {topic} (already in {self._subscribed_topics})")
            return
        self._subscribed_topics.add(topic)
        async with _bus_lock:
            if topic not in _bus:
                _bus[topic] = []
            _bus[topic].append((self.node_id, callback))
            print(f"[transport] subscribe ADDED {topic} to _bus (now {len(_bus[topic])} subs, _bus keys: {list(_bus.keys())})")
        print(f"[transport] Subscribed to {topic}")

        # Уведомить TCP пиров
        await self._tcp_notify_sub(topic)

    async def unsubscribe(self, topic: str) -> None:
        """Отписаться от топика."""
        self._subscribed_topics.discard(topic)
        async with _bus_lock:
            if topic in _bus:
                _bus[topic] = [(sid, cb) for sid, cb in _bus[topic] if sid != self.node_id]
        print(f"[transport] Unsubscribed from {topic}")
        await self._tcp_notify_unsub(topic)

    async def peers(self, topic: str | None = None) -> list[str]:
        """Список подписчиков (local + TCP)."""
        local: set[str] = set()
        async with _bus_lock:
            if topic:
                for sid, _ in _bus.get(topic, []):
                    local.add(sid)
            else:
                for t, subs in _bus.items():
                    for sid, _ in subs:
                        local.add(sid)
            if self._running:
                local.add(self.node_id)

        tcp_peers = set(self._tcp_connections.keys())
        return sorted(local | tcp_peers)

    async def stop(self):
        """Остановить транспорт."""
        self._running = False

        for task in self._reconnect_tasks.values():
            task.cancel()
        self._reconnect_tasks.clear()

        for peer_id, writer in list(self._tcp_connections.items()):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self._tcp_connections.clear()
        self._tcp_readers.clear()
        self._tcp_peer_addrs.clear()
        self._tls_sessions.clear()

        if self._tcp_server:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()

        for topic in list(self._subscribed_topics):
            await self.unsubscribe(topic)

        print("[transport] Stopped.")

    # ───────────────────────── TCP Server ─────────────────────────

    async def _handle_tcp_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Обработчик входящего TCP соединения."""
        peer_addr = writer.get_extra_info("peername")
        peer_id = f"tcp:{peer_addr[0]}:{peer_addr[1]}"
        session: SecureSession | None = None

        # TLS handshake (если включён)
        if self._use_tls:
            session = await server_handshake(reader, writer, self._identity)
            if session is None:
                print(f"[transport] TLS handshake failed from {peer_addr}")
                writer.close()
                return
            assert session.peer_pubkey_hex is not None
            peer_id = f"tls:{session.peer_pubkey_hex[:16]}"
            self._tls_sessions[session.peer_pubkey_hex[:16]] = session
            print(f"[transport] TLS peer authenticated: {peer_id}")

        # P2P hello handshake (простой обмен peer_id)
        if not self._use_tls:
            try:
                hello_line = await asyncio.wait_for(reader.readline(), timeout=3.0)
                if hello_line:
                    try:
                        hello = json.loads(hello_line.decode().strip())
                        if hello.get("type") == "hello":
                            remote_peer = hello.get("peer_id", hello.get("node_id", ""))
                            if remote_peer:
                                old_id = peer_id
                                peer_id = remote_peer
                                print(f"[transport] Client identified: {remote_peer} (was {old_id})")
                    except json.JSONDecodeError:
                        pass
                # Ответное hello
                resp_hello = {
                    "type": "hello",
                    "node_id": self.node_id,
                    "peer_id": self.peer_id,
                }
                writer.write((json.dumps(resp_hello, separators=(",", ":")) + "\n").encode())
                await writer.drain()
            except asyncio.TimeoutError:
                print(f"[transport] No hello from {peer_addr} (timeout)")

        # Сохраняем соединение
        self._tcp_connections[peer_id] = writer
        self._tcp_readers[peer_id] = reader

        try:
            while self._running:
                line = await reader.readline()
                if not line:
                    break

                try:
                    raw = line.decode().strip()
                    msg = json.loads(raw)

                    # Если TLS — расшифровываем
                    if session and is_encrypted_envelope(msg):
                        msg = session.unpack_encrypted(msg)
                        raw = json.dumps(msg, separators=(",", ":"))

                    msg_type = msg.get("type", "")

                    if msg_type == "pub":
                        topic = msg["topic"]
                        payload_b64 = msg.get("data", "")
                        remote_id = msg.get("from", peer_id)
                        if remote_id != peer_id:
                            self._tcp_connections[remote_id] = writer
                            self._tcp_readers[remote_id] = reader
                            if peer_id in self._tcp_connections:
                                del self._tcp_connections[peer_id]
                            if peer_id in self._tcp_readers:
                                del self._tcp_readers[peer_id]
                            peer_id = remote_id

                        payload = base64.b64decode(payload_b64)
                        await self._deliver_local(topic, payload)

                        # Relay mode — форвард другим TCP пирам
                        if self._relay and len(self._tcp_connections) > 1:
                            await self._tcp_broadcast(topic, payload, exclude_peer=peer_id)

                    elif msg_type == "sub":
                        pass

                    elif msg_type == "ping":
                        pong = {"type": "pong"}
                        if session:
                            pong = session.pack_encrypted(pong)
                        writer.write(json.dumps(pong, separators=(",", ":")).encode() + b"\n")
                        await writer.drain()

                except (json.JSONDecodeError, KeyError, ValueError):
                    pass

        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            for key in list(self._tcp_connections.keys()):
                if self._tcp_connections.get(key) is writer:
                    del self._tcp_connections[key]
            for key in list(self._tcp_readers.keys()):
                if self._tcp_readers.get(key) is reader:
                    del self._tcp_readers[key]
            if session:
                assert session.peer_pubkey_hex is not None
                self._tls_sessions.pop(session.peer_pubkey_hex[:16], None)
            try:
                writer.close()
            except Exception:
                pass

    async def _tcp_recv_hello(self, reader: asyncio.StreamReader, timeout: float = 3.0) -> dict | None:
        """Прочитать hello от сервера."""
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if line:
                return json.loads(line.decode().strip())
        except (asyncio.TimeoutError, json.JSONDecodeError):
            pass
        return None

    # ───────────────────────── TCP Client ─────────────────────────

    def _parse_peer_spec(self, spec: str) -> tuple[str, str, int] | None:
        """Разобрать 'peer_id@host:port' → (peer_id, host, port)."""
        try:
            peer_id, addr = spec.rsplit("@", 1)
            host, port_str = addr.rsplit(":", 1)
            return peer_id, host, int(port_str)
        except (ValueError, IndexError):
            return None

    def _start_reconnect_loop(self, peer_id: str, host: str, port: int):
        task = asyncio.create_task(self._reconnect_loop(peer_id, host, port))
        self._reconnect_tasks[peer_id] = task

    async def _reconnect_loop(self, peer_id: str, host: str, port: int):
        """Цикл переподключения с exponential backoff + TLS handshake."""
        delay = 1.0
        max_delay = 30.0
        session: SecureSession | None = None

        while self._running:
            try:
                reader, writer = await asyncio.open_connection(host, port)

                # TLS handshake (если включён)
                if self._use_tls:
                    session = await client_handshake(reader, writer, self._identity)
                    if session is None:
                        print(f"[transport] TLS handshake failed to {host}:{port}")
                        writer.close()
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, max_delay)
                        continue

                    assert session.peer_pubkey_hex is not None
                    new_peer_id = f"tls:{session.peer_pubkey_hex[:16]}"
                    self._tls_sessions[session.peer_pubkey_hex[:16]] = session
                else:
                    new_peer_id = peer_id
                    session = None

                self._tcp_connections[new_peer_id] = writer
                self._tcp_readers[new_peer_id] = reader
                self._tcp_peer_addrs[new_peer_id] = (host, port)
                if new_peer_id != peer_id:
                    self._tcp_connections.pop(peer_id, None)
                    self._tcp_readers.pop(peer_id, None)
                peer_id = new_peer_id
                delay = 1.0

                # Отправить hello + peer_id
                await self._tcp_send(
                    writer,
                    {"type": "hello", "node_id": self.node_id, "peer_id": self.peer_id},
                    session,
                )
                resp = await self._tcp_recv_hello(reader)
                if resp and resp.get("type") == "hello":
                    remote_peer = resp.get("peer_id", resp.get("node_id", ""))
                    if remote_peer and remote_peer != peer_id:
                        print(f"[transport] Server identified as: {remote_peer}")
                        new_peer_id = remote_peer

                # Отправить текущие подписки
                for topic in self._subscribed_topics:
                    await self._tcp_send(
                        writer,
                        {"type": "sub", "topic": topic, "from": self.node_id},
                        session,
                    )

                # Читать сообщения от пира
                while self._running:
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        raw = line.decode().strip()
                        msg = json.loads(raw)

                        if session and is_encrypted_envelope(msg):
                            msg = session.unpack_encrypted(msg)

                        if msg.get("type") == "pub":
                            payload = base64.b64decode(msg["data"])
                            await self._deliver_local(msg["topic"], payload)
                            # Relay mode — форвард другим TCP пирам
                            if self._relay and len(self._tcp_connections) > 1:
                                await self._tcp_broadcast(msg["topic"], payload, exclude_peer=peer_id)
                        elif msg.get("type") == "ping":
                            pong = {"type": "pong"}
                            await self._tcp_send(writer, pong, session)
                    except (json.JSONDecodeError, ValueError, KeyError):
                        pass

            except (TimeoutError, OSError, ConnectionRefusedError):
                pass
            except asyncio.CancelledError:
                break
            except Exception:
                pass

            if not self._running:
                break

            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

        self._reconnect_tasks.pop(peer_id, None)
        self._tcp_connections.pop(peer_id, None)
        self._tcp_readers.pop(peer_id, None)
        if session:
            assert session.peer_pubkey_hex is not None
            self._tls_sessions.pop(session.peer_pubkey_hex[:16], None)

    # ───────────────────────── TCP helpers ─────────────────────────

    async def _tcp_broadcast(self, topic: str, payload: bytes, exclude_peer: str | None = None):
        """Разослать сообщение всем TCP пирам (опционально — исключая одного)."""
        if not self._tcp_connections:
            return
        msg = {
            "type": "pub",
            "topic": topic,
            "data": base64.b64encode(payload).decode(),
            "from": self.node_id,
        }
        encoded = json.dumps(msg, separators=(",", ":")) + "\n"
        data_bytes = encoded.encode()

        for conn_peer_id, writer in list(self._tcp_connections.items()):
            if exclude_peer and conn_peer_id == exclude_peer:
                continue
            try:
                # Если есть TLS сессия для этого пира — шифруем
                session = self._get_session_for_peer(conn_peer_id)
                if session:
                    out_msg = session.pack_encrypted(msg)
                    out_bytes = json.dumps(out_msg, separators=(",", ":")) + "\n"
                    writer.write(out_bytes.encode())
                else:
                    writer.write(data_bytes)
                await writer.drain()
            except Exception:
                pass

    async def _tcp_notify_sub(self, topic: str):
        """Уведомить TCP пиров о подписке."""
        msg = {"type": "sub", "topic": topic, "from": self.node_id}
        for conn_peer_id, writer in self._tcp_connections.items():
            try:
                await self._tcp_send(writer, msg, self._get_session_for_peer(conn_peer_id))
            except Exception:
                pass

    async def _tcp_notify_unsub(self, topic: str):
        """Уведомить TCP пиров об отписке."""
        msg = {"type": "unsub", "topic": topic, "from": self.node_id}
        for conn_peer_id, writer in self._tcp_connections.items():
            try:
                await self._tcp_send(writer, msg, self._get_session_for_peer(conn_peer_id))
            except Exception:
                pass

    @staticmethod
    async def _tcp_send(
        writer: asyncio.StreamWriter, msg: dict, session: SecureSession | None = None
    ):
        """Отправить JSON сообщение в TCP сокет."""
        if session:
            msg = session.pack_encrypted(msg)
        encoded = json.dumps(msg, separators=(",", ":")) + "\n"
        writer.write(encoded.encode())
        await writer.drain()

    async def _deliver_local(self, topic: str, payload: bytes):
        """Доставить сообщение в in-memory bus."""
        if not payload.endswith(b"\n"):
            payload = payload + b"\n"
        async with _bus_lock:
            subs = _bus.get(topic, [])
            if not subs:
                print(f"[transport] bus: no subs for '{topic}' (keys: {list(_bus.keys())[:6]})")
            for subscriber_id, callback in subs:
                try:
                    callback(payload)
                except Exception as e:
                    print(f"[transport] local delivery error on {topic}: {e}")

    def _get_session_for_peer(self, conn_peer_id: str) -> SecureSession | None:
        """Найти TLS сессию для пира по peer_id."""
        if not self._use_tls:
            return None
        # peer_id может быть tls:pubkey_prefix
        for prefix, session in self._tls_sessions.items():
            if prefix in conn_peer_id or conn_peer_id.startswith("tls:"):
                return session
        return None
