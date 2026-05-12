# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""DePIN SDK — Основной класс устройства.



DePINDevice подключается к P2P mesh, публикует телеметрию,
подписывается на команды, синхронизируется через Merkle-дерево.

Использование:
    device = DePINDevice(
        device_id="sensor_kitchen_01",
        device_type="temperature_sensor",
        private_key_hex="...",
    )
    await device.connect()

    # Публикация телеметрии
    await device.publish_telemetry({
        "temperature": 23.5,
        "humidity": 60.2,
    })

    # Подписка на команды
    @device.on_command
    async def handle(cmd):
        if cmd["action"] == "reboot":
            await device.reboot()
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from collections.abc import Callable

from depin.merkle_sync import MerkleTree
from depin.registry import DeviceRegistry
from depin.wal import DePINWAL
from sdk.agent import AgentMesh

# Глобальный реестр (один на процесс)
_global_registry = DeviceRegistry()


class DePINDevice:
    """DePIN-устройство в P2P mesh.

    Концепция:
      - Подключается к mesh как AgentMesh
      - Публикует телеметрию в топик "depin:{device_id}:telemetry"
      - Пишет всё в локальный WAL (offline-first)
      - При reconnect — Merkle-sync догоняет пропущенное
      - Слушает "depin:commands:{device_id}" для управления
    """

    def __init__(
        self,
        device_id: str,
        device_type: str = "generic",
        wal_path: str | None = None,
        db_path: str | None = None,
        port: int = 0,
        relay_host: str | None = None,
        relay_port: int = 0,
        measure_interval: float = 60.0,  # сек между публикациями
    ):
        self.device_id = device_id
        self.device_type = device_type
        self.measure_interval = measure_interval

        # Mesh-агент (ключ генерируется автоматически)
        self._mesh = AgentMesh(
            agent_id=device_id,
            capabilities=[
                f"depin:{device_id}:telemetry",
                f"depin:{device_type}:events",
                f"depin:commands:{device_id}",
            ],
            db_path=db_path or tempfile.mktemp(suffix=f"_depin_{device_id}.db"),
            port=port,
            relay_host=relay_host,
            relay_port=relay_port,
        )

        # WAL для offline-first
        wal_path = wal_path or tempfile.mktemp(suffix=f"_wal_{device_id}.db")
        self.wal = DePINWAL(wal_path, device_id)
        self.wal.open()

        # Callback команды
        self._command_cb: Callable | None = None

        # Состояние
        self._connected = False
        self._last_sync: float = 0
        self._missed_during_offline: int = 0
        self._merkle_diff: int = 0

    # ─────────────────────── Lifecycle ───────────────────────

    async def connect(self) -> str:
        """Подключиться к mesh и зарегистрироваться."""
        # Стартуем mesh
        did = await self._mesh.start()
        self._connected = True

        # Подписка на команды
        await self._mesh.listen(
            {"capability": f"depin:commands:{self.device_id}"},
            self._on_command,
        )

        # Регистрация в реестре
        _global_registry.register(
            self.device_id,
            {
                "device_type": self.device_type,
                "pubkey": self._mesh.identity.public_key_hex,
                "capabilities": self._mesh.capabilities,
                "connected_at": time.time(),
            },
        )

        # Публикация heartbeat
        await self._publish_heartbeat()

        print(f"[DePIN:{self.device_id}] Connected. DID: {did[:16]}...")
        return did

    async def disconnect(self):
        """Отключиться от mesh."""
        self._connected = False
        _global_registry.unregister(self.device_id)
        await self._mesh.stop()
        self.wal.close()
        print(f"[DePIN:{self.device_id}] Disconnected.")

    # ─────────────────────── Telemetry ───────────────────────

    async def publish_telemetry(
        self,
        metrics: dict[str, float],
        status: dict[str, str] | None = None,
    ) -> str:
        """Публикация телеметрии в mesh + запись в WAL.

        Args:
            metrics: числовые метрики {"temperature": 23.5}
            status: строковый статус {"battery": "85%"}

        Returns:
            hash записи
        """
        payload = {
            "device_id": self.device_id,
            "device_type": self.device_type,
            "metrics": metrics,
            "status": status or {},
            "ts": time.time(),
        }

        # WAL (offline-first — пишем даже без сети)
        hash_val = self.wal.append(
            topic=f"depin:{self.device_id}:telemetry",
            telemetry=payload,
        )

        # Публикуем в mesh
        if self._connected:
            await self._mesh.emit(
                f"depin:{self.device_id}:telemetry",
                payload,
            )
            print(f"[DePIN:{self.device_id}] Telemetry published: {metrics}")
        else:
            print(f"[DePIN:{self.device_id}] Offline — telemetry saved to WAL")

        return hash_val

    async def _publish_heartbeat(self):
        """Публикация heartbeat (регистрация в реестре)."""
        await self._mesh.emit(
            f"depin:{self.device_type}:events",
            {
                "device_id": self.device_id,
                "device_type": self.device_type,
                "event": "heartbeat",
                "ts": time.time(),
            },
        )

    # ─────────────────────── Commands ───────────────────────

    def on_command(self, callback: Callable):
        """Декоратор: подписка на команды устройству.

        Пример:
            @device.on_command
            async def handle(cmd):
                if cmd["action"] == "reboot":
                    await device.reboot()
        """
        self._command_cb = callback
        return callback

    def _on_command(self, event: dict):
        """Обработчик входящих команд."""
        payload = event.get("payload", event)
        if self._command_cb:
            try:
                if asyncio.iscoroutinefunction(self._command_cb):
                    asyncio.create_task(self._command_cb(payload))
                else:
                    self._command_cb(payload)
            except Exception as e:
                print(f"[DePIN:{self.device_id}] Command error: {e}")

    # ─────────────────────── Merkle Sync ───────────────────────

    async def sync(self) -> dict:
        """Синхронизация WAL через Merkle-дерево.

        Returns:
            Статус синхронизации
        """
        if not self._connected:
            return {"error": "not connected"}

        # Строим Merkle-дерево текущего WAL
        messages = self.wal.get_all()
        tree = MerkleTree(messages)

        # Публикуем root
        sync_msg = {
            "device_id": self.device_id,
            "type": "merkle_root",
            "root_hash": tree.root_hash,
            "leaf_count": len(messages),
            "ts": time.time(),
        }
        await self._mesh.emit(
            f"depin:{self.device_id}:telemetry",
            sync_msg,
        )

        self._last_sync = time.time()
        self._merkle_diff = 0

        status = {
            "connected": self._connected,
            "wal_messages": len(messages),
            "last_sync": self._last_sync,
            "missed_during_offline": self._missed_during_offline,
            "merkle_diff": self._merkle_diff,
        }
        return status

    async def sync_status(self) -> dict:
        """Текущий статус синхронизации."""
        return {
            "connected": self._connected,
            "wal_messages": self.wal.count(),
            "last_sync": self._last_sync,
            "missed_during_offline": self._missed_during_offline,
            "merkle_diff": self._merkle_diff,
        }

    # ─────────────────────── Utility ───────────────────────

    async def reboot(self):
        """Перезагрузка устройства (заглушка)."""
        print(f"[DePIN:{self.device_id}] Reboot signal received")
        await self.disconnect()
        # В реальном устройстве — аппаратная перезагрузка

    @property
    def did(self) -> str:
        return self._mesh.identity.did

    @staticmethod
    def get_registry() -> DeviceRegistry:
        return _global_registry
