"""Relay Server — NAT traversal for P2P Agent Mesh.

Публичная нода, к которой подключаются агенты за NAT.
Relay форвардит зашифрованные сообщения (не видит контент).
"""

import asyncio
import json
import os
import sys
import time
from collections import deque, defaultdict
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from phase0.identity import Identity
from phase0.handshake import (
    SecureSession, server_handshake, is_encrypted_envelope,
)


# ─────────────────────────────────────────────
# Agent session on relay
# ─────────────────────────────────────────────

class AgentSession:
    """Подключённый агент на relay."""

    def __init__(self, pubkey: str, session: SecureSession,
                 writer: asyncio.StreamWriter, reader: asyncio.StreamReader,
                 capabilities: list[str]):
        self.pubkey = pubkey
        self.relay_session = session  # session key A↔Relay
        self.writer = writer
        self.reader = reader
        self.capabilities = capabilities
        self.connected = True

    @property
    def pubkey_prefix(self) -> str:
        return self.pubkey[:16]


# ─────────────────────────────────────────────
# Relay Server
# ─────────────────────────────────────────────

class RelayServer:
    """TCP relay сервер. Не расшифровывает E2E сообщения."""

    def __init__(self, identity: Optional[Identity] = None,
                 host: str = "0.0.0.0", port: int = 0):
        self._identity = identity or Identity()
        self._host = host
        self._port = port
        self._server: Optional[asyncio.Server] = None
        self._running = False
        self._agents: dict[str, AgentSession] = {}  # pubkey → session
        self._lock = asyncio.Lock()

        # Rate limiting (from env, со значениями по умолчанию)
        self._max_msgs_per_sec = int(os.getenv("RELAY_MAX_MSGS_PER_SEC", "10"))
        self._max_connections_per_ip = int(os.getenv("RELAY_MAX_CONN_PER_IP", "5"))
        self._max_payload_bytes = int(os.getenv("RELAY_MAX_PAYLOAD", "1048576"))  # 1MB
        self._rate_limits: dict[str, deque] = {}  # peer_id → deque of timestamps
        self._ip_connections: dict[str, int] = defaultdict(int)  # ip → count

    @property
    def port(self) -> int:
        if self._server:
            return self._server.sockets[0].getsockname()[1]
        return self._port

    @property
    def host(self) -> str:
        return self._host

    async def start(self, host: Optional[str] = None, port: Optional[int] = None):
        """Запустить relay сервер."""
        self._host = host or self._host
        self._port = port or self._port
        self._running = True

        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port
        )

        print(f"[relay] Started. Identity: {self._identity.public_key_hex[:16]}... "
              f"TCP: {self._host}:{self.port}")
        return self

    async def stop(self):
        """Остановить relay."""
        self._running = False

        # Отключить всех агентов
        async with self._lock:
            for pubkey, agent in list(self._agents.items()):
                try:
                    agent.writer.close()
                except Exception:
                    pass
            self._agents.clear()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
        print(f"[relay] Stopped.")

    async def serve_forever(self):
        """Держать сервер запущенным (только если нужен await)."""
        if self._server:
            await self._server.serve_forever()

    # ───────────────────────── Client handler ─────────────────────────

    async def _handle_client(self, reader: asyncio.StreamReader,
                              writer: asyncio.StreamWriter):
        """Обработчик входящего TCP соединения."""
        peer_addr = writer.get_extra_info('peername')
        peer_ip = peer_addr[0] if peer_addr else "unknown"
        agent: Optional[AgentSession] = None

        # Проверка лимита соединений с одного IP
        self._ip_connections[peer_ip] += 1
        if self._ip_connections[peer_ip] > self._max_connections_per_ip:
            print(f"[relay] Connection limit exceeded from {peer_ip} "
                  f"({self._ip_connections[peer_ip]}/{self._max_connections_per_ip})")
            self._ip_connections[peer_ip] -= 1
            writer.close()
            return

        try:
            # 1. TLS handshake
            relay_session = await server_handshake(reader, writer, self._identity)
            if relay_session is None:
                print(f"[relay] Handshake failed from {peer_addr}")
                writer.close()
                return

            # 2. Ждём REGISTER
            agent = await self._wait_register(reader, relay_session, writer)

            if agent is None:
                writer.close()
                return

            print(f"[relay] Agent registered: {agent.pubkey_prefix}... "
                  f"caps={agent.capabilities}")

            # Отправляем REGISTERED + текущие peers
            await self._send_encrypted(agent.writer, agent.relay_session, {
                "type": "registered", "agent_id": agent.pubkey_prefix,
            })
            await self._send_peers_list(agent)

            # Уведомляем других агентов о новом пире
            await self._broadcast_peers_update()

            # 3. Обработка сообщений от агента
            await self._handle_agent_messages(agent, reader, relay_session)

        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            if agent:
                async with self._lock:
                    self._agents.pop(agent.pubkey, None)
                print(f"[relay] Agent disconnected: {agent.pubkey_prefix}...")
                await self._broadcast_peers_update()
            self._ip_connections[peer_ip] = max(0, self._ip_connections[peer_ip] - 1)
            try:
                writer.close()
            except Exception:
                pass

    async def _wait_register(self, reader: asyncio.StreamReader,
                              relay_session: SecureSession,
                              writer: asyncio.StreamWriter,
                              timeout: float = 10.0) -> Optional[AgentSession]:
        """Ждём REGISTER сообщение от агента."""
        line = await asyncio.wait_for(reader.readline(), timeout)
        if not line:
            return None

        # Декодируем зашифрованное register сообщение
        try:
            raw = json.loads(line.decode().strip())
            if is_encrypted_envelope(raw):
                msg = relay_session.unpack_encrypted(raw)
            else:
                msg = raw  # fallback для plaintext (тесты)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return None

        pubkey = msg.get("pubkey", "")
        capabilities = msg.get("capabilities", [])

        if msg.get("type") != "register" or not pubkey:
            return None

        session = AgentSession(
            pubkey=pubkey,
            session=relay_session,
            writer=writer,
            reader=reader,
            capabilities=capabilities,
        )

        async with self._lock:
            self._agents[pubkey] = session
        return session

    async def _handle_agent_messages(self, agent: AgentSession,
                                      reader: asyncio.StreamReader,
                                      relay_session: SecureSession):
        """Цикл обработки сообщений от подключённого агента."""
        while self._running:
            line = await reader.readline()
            if not line:
                break

            try:
                msg = json.loads(line.decode().strip())
                if is_encrypted_envelope(msg):
                    msg = relay_session.unpack_encrypted(msg)

                msg_type = msg.get("type", "")

                # Rate limit check для сообщений, порождающих форвардинг
                if msg_type in ("send", "e2e_init", "e2e_accept", "register"):
                    if not self._check_rate_limit(agent.pubkey_prefix):
                        await self._send_encrypted(agent.writer, relay_session, {
                            "type": "error",
                            "code": "rate_limited",
                            "message": f"Max {self._max_msgs_per_sec} msgs/sec exceeded",
                        })
                        continue

                if msg_type == "register":
                    # Повторная регистрация (обновление capabilities)
                    agent.capabilities = msg.get("capabilities", agent.capabilities)
                    await self._send_encrypted(agent.writer, relay_session, {
                        "type": "registered", "agent_id": agent.pubkey_prefix,
                    })

                elif msg_type == "e2e_init":
                    # A хочет установить E2E с B
                    target_pubkey = msg["target"]
                    async with self._lock:
                        target = self._agents.get(target_pubkey)

                    if target and target.connected:
                        await self._send_encrypted(target.writer, target.relay_session, {
                            "type": "e2e_req",
                            "from": agent.pubkey,
                            "eph_pub": msg["eph_pub"],
                        })
                    else:
                        await self._send_encrypted(agent.writer, relay_session, {
                            "type": "error",
                            "code": "target_not_found",
                            "message": f"Agent {target_pubkey[:16]}... not connected",
                        })

                elif msg_type == "e2e_accept":
                    # B принимает E2E с A
                    target_pubkey = msg["target"]
                    async with self._lock:
                        target = self._agents.get(target_pubkey)

                    if target and target.connected:
                        await self._send_encrypted(target.writer, target.relay_session, {
                            "type": "e2e_ready",
                            "from": agent.pubkey,
                            "eph_pub": msg["eph_pub"],
                        })
                    else:
                        await self._send_encrypted(agent.writer, relay_session, {
                            "type": "error",
                            "code": "target_not_found",
                            "message": f"Agent {target_pubkey[:16]}... not connected",
                        })

                elif msg_type == "send":
                    # A отправляет E2E сообщение B
                    target_pubkey = msg["target"]
                    data = msg["data"]

                    # Проверка max payload
                    if len(data) > self._max_payload_bytes:
                        await self._send_encrypted(agent.writer, relay_session, {
                            "type": "error",
                            "code": "payload_too_large",
                            "message": f"Max payload {self._max_payload_bytes} bytes",
                        })
                        continue

                    async with self._lock:
                        target = self._agents.get(target_pubkey)

                    if target and target.connected:
                        await self._send_encrypted(target.writer, target.relay_session, {
                            "type": "recv",
                            "from": agent.pubkey,
                            "data": data,
                        })

                elif msg_type == "ping":
                    await self._send_encrypted(agent.writer, relay_session, {
                        "type": "pong",
                    })

            except (json.JSONDecodeError, KeyError, ValueError):
                pass

    # ───────────────────────── Helpers ─────────────────────────

    def _check_rate_limit(self, peer_id: str) -> bool:
        """Проверка rate limit: ≤ max_msgs_per_sec с одного пира."""
        now = time.time()
        if peer_id not in self._rate_limits:
            self._rate_limits[peer_id] = deque()
        # Clean old entries (>1 sec)
        q = self._rate_limits[peer_id]
        while q and q[0] < now - 1:
            q.popleft()
        if len(q) >= self._max_msgs_per_sec:
            return False
        q.append(now)
        return True

    async def _send_encrypted(self, writer: asyncio.StreamWriter,
                               session: SecureSession, msg: dict):
        """Отправить сообщение агенту (зашифрованное relay session key)."""
        envelope = session.pack_encrypted(msg)
        data = json.dumps(envelope, separators=(",", ":")) + "\n"
        try:
            writer.write(data.encode())
            await writer.drain()
        except Exception:
            pass

    async def _send_peers_list(self, agent: AgentSession):
        """Отправить агенту список подключённых пиров."""
        peers_list = []
        async with self._lock:
            for pubkey, other in self._agents.items():
                if pubkey != agent.pubkey and other.connected:
                    peers_list.append({
                        "pubkey": pubkey,
                        "capabilities": other.capabilities,
                    })

        await self._send_encrypted(agent.writer, agent.relay_session, {
            "type": "peers",
            "peers": peers_list,
        })

    async def _broadcast_peers_update(self):
        """Разослать всем агентам обновлённый список peers."""
        async with self._lock:
            agents = list(self._agents.values())

        for agent in agents:
            if agent.connected:
                await self._send_peers_list(agent)


# ───────────────────────── Main ─────────────────────────

async def main():
    identity = Identity()
    relay = RelayServer(identity=identity, host="0.0.0.0", port=9900)
    await relay.start()
    print(f"[relay] Public key: {identity.public_key_hex}")
    print(f"[relay] Listening on {relay.host}:{relay.port}")
    await relay.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
