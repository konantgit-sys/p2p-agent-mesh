"""Phase 0 — Transport: лёгкий P2P pub/sub. Zero external dependencies.

Same-process: глобальный relay для обмена между инстансами.
Multi-process: TCP соединения между узлами (JSON lines over TCP).

API совместим с IPFSTransport — AgentMesh не требует изменений.
"""

import asyncio
import base64
import json
import os
import time
import uuid
from typing import Callable, Optional
from collections.abc import Callable as CallableABC


# === Внутренний message bus для same-process pub/sub ===
# topic -> [(subscriber_id, callback)]
_bus: dict[str, list[tuple[str, Callable]]] = {}
_bus_lock = asyncio.Lock()


class P2PTransport:
    """Лёгкий P2P транспорт. Zero external dependencies.

    Два слоя:
    1. In-memory _bus — обмен между инстансами в одном процессе
    2. TCP — обмен между процессами/машинами (JSON lines, base64 payload)

    API: start/stop/publish/subscribe/unsubscribe/peers — как у IPFSTransport.
    """

    def __init__(self, node_id: Optional[str] = None,
                 bootstrap_peers: Optional[list[str]] = None):
        self.node_id = node_id or f"node_{uuid.uuid4().hex[:8]}"
        self._subscribed_topics: set[str] = set()
        self._running = False
        self.peer_id: Optional[str] = None

        # TCP слой
        self._tcp_server: Optional[asyncio.Server] = None
        self._tcp_port: int = 0
        self._tcp_connections: dict[str, asyncio.StreamWriter] = {}  # peer_id -> writer
        self._tcp_readers: dict[str, asyncio.StreamReader] = {}      # peer_id -> reader
        self._tcp_peer_addrs: dict[str, tuple[str, int]] = {}        # peer_id -> (host, port)
        self._reconnect_tasks: dict[str, asyncio.Task] = {}          # peer_id -> task
        self._bootstrap_peers: list[str] = bootstrap_peers or []

    async def start(self, host: str = "127.0.0.1", port: int = 0):
        """Запустить транспорт + TCP сервер на host:port (0 = случайный)."""
        self._running = True
        self.peer_id = f"did:p2p:{self.node_id}"

        # Старт TCP сервера
        self._tcp_server = await asyncio.start_server(
            self._handle_tcp_client, host, port
        )
        self._tcp_port = self._tcp_server.sockets[0].getsockname()[1]

        # Подключение к bootstrap пирам
        for peer_spec in self._bootstrap_peers:
            parsed = self._parse_peer_spec(peer_spec)
            if parsed:
                peer_id, remote_host, remote_port = parsed
                self._start_reconnect_loop(peer_id, remote_host, remote_port)

        print(f"[transport] Started. PeerID: {self.peer_id[:20]}... "
              f"TCP: {host}:{self._tcp_port}")
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

        # 2. TCP broadcast (base64 payload, без \n в data)
        await self._tcp_broadcast(topic, data.rstrip(b"\n"))

    async def subscribe(self, topic: str, callback: Callable) -> None:
        """Подписаться на топик. Колбэк получает сырые bytes."""
        if topic in self._subscribed_topics:
            return
        self._subscribed_topics.add(topic)
        async with _bus_lock:
            if topic not in _bus:
                _bus[topic] = []
            _bus[topic].append((self.node_id, callback))
        print(f"[transport] Subscribed to {topic}")

        # Уведомить TCP пиров
        await self._tcp_notify_sub(topic)

    async def unsubscribe(self, topic: str) -> None:
        """Отписаться от топика."""
        self._subscribed_topics.discard(topic)
        async with _bus_lock:
            if topic in _bus:
                _bus[topic] = [
                    (sid, cb) for sid, cb in _bus[topic]
                    if sid != self.node_id
                ]
        print(f"[transport] Unsubscribed from {topic}")

        # Уведомить TCP пиров
        await self._tcp_notify_unsub(topic)

    async def peers(self, topic: Optional[str] = None) -> list[str]:
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
        """Остановить транспорт: TCP + local bus."""
        self._running = False

        # Остановить reconnect задачи
        for task in self._reconnect_tasks.values():
            task.cancel()
        self._reconnect_tasks.clear()

        # Закрыть TCP соединения
        for peer_id, writer in list(self._tcp_connections.items()):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self._tcp_connections.clear()
        self._tcp_readers.clear()
        self._tcp_peer_addrs.clear()

        # Остановить TCP сервер
        if self._tcp_server:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()

        # Отписаться от топиков
        for topic in list(self._subscribed_topics):
            await self.unsubscribe(topic)

        print(f"[transport] Stopped.")

    # ───────────────────────── TCP Server ─────────────────────────

    async def _handle_tcp_client(self, reader: asyncio.StreamReader,
                                  writer: asyncio.StreamWriter):
        """Обработчик входящего TCP соединения."""
        peer_addr = writer.get_extra_info('peername')
        peer_id = f"tcp:{peer_addr[0]}:{peer_addr[1]}"

        # Сохраняем соединение временно (peer_id может уточниться)
        self._tcp_connections[peer_id] = writer
        self._tcp_readers[peer_id] = reader

        try:
            while self._running:
                line = await reader.readline()
                if not line:
                    break

                try:
                    msg = json.loads(line.decode().strip())
                    msg_type = msg.get("type", "")

                    if msg_type == "pub":
                        topic = msg["topic"]
                        payload_b64 = msg.get("data", "")
                        # Если есть from — используем как peer_id
                        remote_id = msg.get("from", peer_id)
                        if remote_id != peer_id:
                            # Обновляем ключ соединения
                            self._tcp_connections[remote_id] = writer
                            self._tcp_readers[remote_id] = reader
                            if peer_id in self._tcp_connections:
                                del self._tcp_connections[peer_id]
                            if peer_id in self._tcp_readers:
                                del self._tcp_readers[peer_id]
                            peer_id = remote_id

                        payload = base64.b64decode(payload_b64)
                        # Доставить локально (без ретрансляции по TCP)
                        await self._deliver_local(topic, payload)

                    elif msg_type == "sub":
                        remote_id = msg.get("from", peer_id)
                        # Просто подтверждаем что peer жив
                        pass

                    elif msg_type == "ping":
                        writer.write(b'{"type":"pong"}\n')
                        await writer.drain()

                except (json.JSONDecodeError, KeyError, ValueError):
                    pass  # Игнорируем битые сообщения

        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            # Очистка при отключении
            for key in list(self._tcp_connections.keys()):
                if self._tcp_connections.get(key) is writer:
                    del self._tcp_connections[key]
            for key in list(self._tcp_readers.keys()):
                if self._tcp_readers.get(key) is reader:
                    del self._tcp_readers[key]
            try:
                writer.close()
            except Exception:
                pass

    # ───────────────────────── TCP Client ─────────────────────────

    def _parse_peer_spec(self, spec: str) -> Optional[tuple[str, str, int]]:
        """Разобрать 'peer_id@host:port' → (peer_id, host, port)."""
        try:
            peer_id, addr = spec.rsplit("@", 1)
            host, port_str = addr.rsplit(":", 1)
            return peer_id, host, int(port_str)
        except (ValueError, IndexError):
            return None

    def _start_reconnect_loop(self, peer_id: str, host: str, port: int):
        """Запустить фоновую задачу reconnect."""
        task = asyncio.create_task(
            self._reconnect_loop(peer_id, host, port)
        )
        self._reconnect_tasks[peer_id] = task

    async def _reconnect_loop(self, peer_id: str, host: str, port: int):
        """Цикл переподключения с exponential backoff."""
        delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                self._tcp_connections[peer_id] = writer
                self._tcp_readers[peer_id] = reader
                self._tcp_peer_addrs[peer_id] = (host, port)
                delay = 1.0  # Сброс после успешного подключения

                # Отправить текущие подписки
                for topic in self._subscribed_topics:
                    await self._tcp_send(writer, {
                        "type": "sub", "topic": topic, "from": self.node_id
                    })

                # Читать сообщения от пира
                while self._running:
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        msg = json.loads(line.decode().strip())
                        if msg.get("type") == "pub":
                            payload = base64.b64decode(msg["data"])
                            await self._deliver_local(msg["topic"], payload)
                        elif msg.get("type") == "ping":
                            await self._tcp_send(writer, {"type": "pong"})
                    except (json.JSONDecodeError, ValueError, KeyError):
                        pass

            except (OSError, ConnectionRefusedError, asyncio.TimeoutError):
                pass  # retry with backoff
            except asyncio.CancelledError:
                break
            except Exception:
                pass

            if not self._running:
                break

            # Exponential backoff
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

        # Cleanup
        self._reconnect_tasks.pop(peer_id, None)
        self._tcp_connections.pop(peer_id, None)
        self._tcp_readers.pop(peer_id, None)

    # ───────────────────────── TCP helpers ─────────────────────────

    async def _tcp_broadcast(self, topic: str, payload: bytes):
        """Разослать сообщение всем TCP пирам."""
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
        for peer_id, writer in list(self._tcp_connections.items()):
            try:
                writer.write(data_bytes)
                await writer.drain()
            except Exception:
                pass  # reconnect task поднимет

    async def _tcp_notify_sub(self, topic: str):
        """Уведомить TCP пиров о подписке."""
        msg = {
            "type": "sub",
            "topic": topic,
            "from": self.node_id,
        }
        encoded = json.dumps(msg, separators=(",", ":")) + "\n"
        data_bytes = encoded.encode()
        for writer in self._tcp_connections.values():
            try:
                writer.write(data_bytes)
                await writer.drain()
            except Exception:
                pass

    async def _tcp_notify_unsub(self, topic: str):
        """Уведомить TCP пиров об отписке."""
        msg = {
            "type": "unsub",
            "topic": topic,
            "from": self.node_id,
        }
        encoded = json.dumps(msg, separators=(",", ":")) + "\n"
        data_bytes = encoded.encode()
        for writer in self._tcp_connections.values():
            try:
                writer.write(data_bytes)
                await writer.drain()
            except Exception:
                pass

    @staticmethod
    async def _tcp_send(writer: asyncio.StreamWriter, msg: dict):
        """Отправить JSON сообщение в TCP сокет."""
        encoded = json.dumps(msg, separators=(",", ":")) + "\n"
        writer.write(encoded.encode())
        await writer.drain()

    async def _deliver_local(self, topic: str, payload: bytes):
        """Доставить сообщение в in-memory bus (без ретрансляции по TCP)."""
        if not payload.endswith(b"\n"):
            payload = payload + b"\n"
        async with _bus_lock:
            for subscriber_id, callback in _bus.get(topic, []):
                try:
                    callback(payload)
                except Exception as e:
                    print(f"[transport] local delivery error on {topic}: {e}")
