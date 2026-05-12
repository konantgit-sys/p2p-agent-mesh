# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""Relay over WebSocket — для деплоя на *.v2.site.

Использует тот же протокол, что relay/server.py, но поверх WebSocket.
Не требует handshake.py — WSS шифрование обеспечивает *.v2.site.
"""

import asyncio
import json
import os
import socket
import sys
import time
from collections import defaultdict, deque

import websockets
from websockets.asyncio.server import ServerConnection

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class WSAgentSession:
    """Подключённый агент через WebSocket."""

    def __init__(self, pubkey: str, ws: ServerConnection, capabilities: list[str]):
        self.pubkey = pubkey
        self.ws = ws
        self.capabilities = capabilities
        self.connected = True

    @property
    def pubkey_prefix(self) -> str:
        return self.pubkey[:16]


class WSRelayServer:
    """Relay сервер поверх WebSocket для публичного деплоя."""

    def __init__(self, host: str = "0.0.0.0", port: int = 9900):
        self._host = host
        self._port = port
        self._agents: dict[str, WSAgentSession] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._pending: dict[str, list[dict]] = defaultdict(list)  # pubkey → [msgs]

        # Rate limiting
        self._max_msgs_per_sec = int(os.getenv("RELAY_MAX_MSGS_PER_SEC", "10"))
        self._rate_limits: dict[str, deque] = {}

    async def start(self):
        self._running = True
        self._start_time = time.time()
        # Создаём сокет с SO_REUSEADDR чтобы избежать EADDRINUSE
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._host, self._port))
        sock.listen(128)
        sock.setblocking(False)

        self._ws_server = await websockets.serve(
            self._handle_ws,
            sock=sock,
            process_request=self._http_handler,
        )
        actual_port = self._ws_server.sockets[0].getsockname()[1]
        print(f"[relay] WS started on ws://{self._host}:{actual_port}")

    async def _http_handler(self, connection, request):
        """HTTP healthcheck для front-proxy."""
        if request.path in ("/", "/status", "/health"):
            body = json.dumps({
                "status": "ok",
                "agents": len(self._agents),
                "uptime_sec": round(time.time() - self._start_time),
            }).encode()
            from websockets.server import Response
            return Response(200, "OK", [(b"Content-Type", b"application/json")], body)
        return None  # Let websockets handle WebSocket upgrade
        return self

    async def stop(self):
        self._running = False
        if self._ws_server:
            self._ws_server.close()
            await self._ws_server.wait_closed()
        print("[relay] WS stopped")

    async def serve_forever(self):
        await asyncio.Future()  # бесконечное ожидание

    # ───────────────────────── WS Handler ─────────────────────────

    async def _handle_ws(self, ws: ServerConnection):
        """Обработчик WebSocket соединения."""
        agent: WSAgentSession | None = None
        (
            ws.request.headers.get("X-Forwarded-For", ws.remote_address[0])
            if hasattr(ws, "remote_address")
            else "unknown"
        )

        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("type", "")

                # Первое сообщение должно быть register
                if msg_type == "register":
                    if agent:
                        continue  # уже зарегистрирован
                    agent = await self._do_register(ws, msg)
                    if agent:
                        print(
                            f"[relay] WS agent registered: {agent.pubkey_prefix}... "
                            f"caps={agent.capabilities}"
                        )
                        await self._send(
                            ws,
                            {
                                "type": "registered",
                                "agent_id": agent.pubkey_prefix,
                            },
                        )
                        await self._send_peers(agent)
                    continue

                if agent is None:
                    continue  # не зарегистрирован — игнорируем

                # Rate limit
                if msg_type in ("send", "e2e_init", "e2e_accept"):
                    if not self._check_rate_limit(agent.pubkey_prefix):
                        await self._send(
                            ws,
                            {
                                "type": "error",
                                "code": "rate_limited",
                                "message": f"Max {self._max_msgs_per_sec} msgs/sec",
                            },
                        )
                        continue

                if msg_type == "e2e_init":
                    target = await self._get_agent(msg["target"])
                    if target:
                        await self._send(
                            target.ws,
                            {
                                "type": "e2e_req",
                                "from": agent.pubkey,
                                "eph_pub": msg["eph_pub"],
                            },
                        )
                    else:
                        await self._send(
                            ws,
                            {
                                "type": "error",
                                "code": "target_not_found",
                            },
                        )

                elif msg_type == "e2e_accept":
                    target = await self._get_agent(msg["target"])
                    if target:
                        await self._send(
                            target.ws,
                            {
                                "type": "e2e_ready",
                                "from": agent.pubkey,
                                "eph_pub": msg["eph_pub"],
                            },
                        )

                elif msg_type == "send":
                    target = await self._get_agent(msg["target"])
                    if target:
                        await self._send(
                            target.ws,
                            {
                                "type": "recv",
                                "from": agent.pubkey,
                                "data": msg.get("data", ""),
                            },
                        )
                    else:
                        await self._send(
                            ws,
                            {
                                "type": "error",
                                "code": "target_not_found",
                            },
                        )

                elif msg_type == "ping":
                    await self._send(ws, {"type": "pong"})

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if agent:
                async with self._lock:
                    self._agents.pop(agent.pubkey, None)
                print(f"[relay] WS agent disconnected: {agent.pubkey_prefix}...")
                await self._broadcast_peers()

    # ───────────────────────── Internal ─────────────────────────

    async def _do_register(self, ws: ServerConnection, msg: dict) -> WSAgentSession | None:
        pubkey = msg.get("pubkey", "")
        capabilities = msg.get("capabilities", [])
        if not pubkey:
            return None
        session = WSAgentSession(pubkey, ws, capabilities)
        async with self._lock:
            self._agents[pubkey] = session
        return session

    async def _get_agent(self, pubkey: str) -> WSAgentSession | None:
        async with self._lock:
            return self._agents.get(pubkey)

    async def _send(self, ws: ServerConnection, msg: dict):
        try:
            await ws.send(json.dumps(msg, separators=(",", ":")))
        except Exception:
            pass

    async def _send_peers(self, agent: WSAgentSession):
        peers_list = []
        async with self._lock:
            for pubkey, other in self._agents.items():
                if pubkey != agent.pubkey:
                    peers_list.append(
                        {
                            "pubkey": pubkey,
                            "capabilities": other.capabilities,
                        }
                    )
        await self._send(agent.ws, {"type": "peers", "peers": peers_list})

    async def _broadcast_peers(self):
        async with self._lock:
            agents = list(self._agents.values())
        for agent in agents:
            await self._send_peers(agent)

    def _check_rate_limit(self, peer_id: str) -> bool:
        now = time.time()
        if peer_id not in self._rate_limits:
            self._rate_limits[peer_id] = deque()
        q = self._rate_limits[peer_id]
        while q and q[0] < now - 1:
            q.popleft()
        if len(q) >= self._max_msgs_per_sec:
            return False
        q.append(now)
        return True


# ───────────────────────── Main ─────────────────────────


async def main():
    port = int(os.environ.get("RELAY_PORT", "9900"))
    relay = WSRelayServer(host="0.0.0.0", port=port)
    await relay.start()
    print(f"[relay] WS relay ready on ws://0.0.0.0:{port}")
    await relay.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
