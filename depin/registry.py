# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""DePIN SDK — Device Registry.

Регистрация устройств в DHT: device_id → {device_type, pubkey, last_seen, capabilities}.

Каждое устройство регистрируется при connect с подписью.
Соседние узлы верифицируют и реплицируют (K=3).
"""

from __future__ import annotations

import time

# ─────────────────────── Реестр устройств (in-memory) ────────────────────────


class DeviceRegistry:
    """Локальный реестр DePIN-устройств.

    В production реплицируется через DHT (phase0/dht.py).
    Для MVP — in-memory dict с TTL.
    """

    def __init__(self, ttl_sec: int = 86400):  # 24h TTL
        self._devices: dict[str, dict] = {}
        self.ttl_sec = ttl_sec

    def register(self, device_id: str, info: dict) -> bool:
        """Зарегистрировать устройство.

        Args:
            device_id: уникальный ID устройства
            info: {device_type, pubkey, capabilities, ...}

        Returns:
            True если новое устройство, False если обновление
        """
        is_new = device_id not in self._devices
        info["_last_seen"] = time.time()
        info["_device_id"] = device_id
        self._devices[device_id] = info
        return is_new

    def unregister(self, device_id: str) -> bool:
        """Удалить устройство."""
        return self._devices.pop(device_id, None) is not None

    def get(self, device_id: str) -> dict | None:
        """Получить информацию об устройстве."""
        info = self._devices.get(device_id)
        if info is None:
            return None
        # Проверка TTL
        if time.time() - info.get("_last_seen", 0) > self.ttl_sec:
            self.unregister(device_id)
            return None
        return info

    def list_by_type(self, device_type: str) -> list[dict]:
        """Список устройств по типу."""
        return [d for d in self._devices.values() if d.get("device_type") == device_type]

    def list_all(self) -> list[dict]:
        """Список всех активных устройств."""
        now = time.time()
        active = []
        for d in list(self._devices.values()):
            if now - d.get("_last_seen", 0) <= self.ttl_sec:
                active.append(d)
        return active

    def count(self) -> int:
        """Количество активных устройств."""
        return len(self.list_all())

    def heartbeat(self, device_id: str) -> bool:
        """Обновить last_seen (пульс устройства)."""
        info = self._devices.get(device_id)
        if info is None:
            return False
        info["_last_seen"] = time.time()
        return True

    def to_dict(self) -> dict:
        """Сериализация (для mesh публикации)."""
        return {
            "devices": {
                did: {k: v for k, v in info.items() if not k.startswith("_")}
                for did, info in self._devices.items()
            },
            "count": len(self._devices),
        }
