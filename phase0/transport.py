"""Phase 0 — Transport: реальный IPFS PubSub бэкенд через asyncio subprocess.

Использует `ipfs pubsub pub` и `ipfs pubsub sub` CLI команды.
Поддерживает подписку на несколько топиков, каждый в отдельном процессе.
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Callable, Optional

IPFS_BIN = os.environ.get("IPFS_BIN", "ipfs")
IPFS_PATH = os.environ.get("IPFS_PATH") or str(Path.home() / ".ipfs")


class IPFSTransport:
    """Обёртка над IPFS PubSub CLI через asyncio subprocess."""

    def __init__(self, ipfs_bin: str = IPFS_BIN, ipfs_path: str = IPFS_PATH):
        self.ipfs_bin = ipfs_bin
        self.env = {**os.environ, "IPFS_PATH": ipfs_path}
        self._subscribers: dict[str, list[Callable]] = {}
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._running = False

    async def start(self):
        """Проверяет что IPFS daemon работает."""
        self._running = True
        proc = await asyncio.create_subprocess_exec(
            self.ipfs_bin, "id",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"IPFS daemon not running: {stderr.decode()}")
        info = json.loads(stdout.decode())
        self.peer_id = info["ID"]
        print(f"[transport] Connected. PeerID: {self.peer_id[:20]}...")
        return self.peer_id

    async def publish(self, topic: str, data: bytes) -> None:
        """Опубликовать сообщение в топик через ipfs pubsub pub.

        Python 3.11 bug: communicate(input=data) не закрывает stdin.
        Используем manual write + close.
        """
        proc = await asyncio.create_subprocess_exec(
            self.ipfs_bin, "pubsub", "pub", topic,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            env=self.env
        )
        if data:
            proc.stdin.write(data)
            if not data.endswith(b"\n"):
                proc.stdin.write(b"\n")
            await proc.stdin.drain()
        proc.stdin.close()
        await proc.wait()
        if proc.returncode != 0:
            err = (await proc.stderr.read()).decode().strip()
            if err and "canceled" not in err:
                print(f"[transport] publish error on {topic}: {err}")

    async def subscribe(self, topic: str, callback: Callable) -> None:
        """Подписаться на топик. Запускает долгоживущий процесс.

        Использует stdbuf -o0 для отключения буферизации stdout,
        иначе сообщения не доходят через pipe (C stdio буфер).
        """
        if topic in self._processes:
            self._subscribers[topic].append(callback)
            return

        self._subscribers[topic] = [callback]
        proc = await asyncio.create_subprocess_exec(
            "stdbuf", "-o0", self.ipfs_bin, "pubsub", "sub", topic,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env
        )
        self._processes[topic] = proc
        self._tasks[topic] = asyncio.create_task(
            self._reader_loop(topic, proc)
        )
        print(f"[transport] Subscribed to {topic}")

    async def _reader_loop(self, topic: str, proc: asyncio.subprocess.Process):
        """Читает stdout процесса subscribe и dispatch по callback."""
        try:
            while self._running and proc.returncode is None:
                line = await proc.stdout.readline()
                if not line:
                    break
                for cb in self._subscribers.get(topic, []):
                    try:
                        cb(line)
                    except Exception as e:
                        print(f"[transport] callback error on {topic}: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()

    async def unsubscribe(self, topic: str) -> None:
        """Отписаться от топика — убить процесс."""
        if topic in self._tasks:
            self._tasks[topic].cancel()
            del self._tasks[topic]
        if topic in self._processes:
            proc = self._processes[topic]
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
            del self._processes[topic]
        self._subscribers.pop(topic, None)
        print(f"[transport] Unsubscribed from {topic}")

    async def peers(self, topic: Optional[str] = None) -> list[str]:
        """Список пиров в сети / на топике."""
        args = [self.ipfs_bin, "pubsub", "peers"]
        if topic:
            args.append(topic)
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env
        )
        stdout, _ = await proc.communicate()
        return [p.strip() for p in stdout.decode().split("\n") if p.strip()]

    async def stop(self):
        """Остановить все подписки."""
        self._running = False
        for topic in list(self._tasks.keys()):
            await self.unsubscribe(topic)
