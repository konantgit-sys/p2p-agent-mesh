"""DePIN SDK — тесты.

Проверяет:
  - Merkle-дерево: построение, root_hash, proof
  - MerkleSync: diff между двумя WAL
  - DePINWAL: запись, чтение, seq
  - DeviceRegistry: регистрация, TTL, heartbeat
  - DePINDevice: создание, connect/disconnect, telemetry
"""

import asyncio
import os
import tempfile
import time

import pytest

from depin.device import DePINDevice
from depin.merkle_sync import MerkleSync, MerkleTree
from depin.registry import DeviceRegistry
from depin.wal import DePINWAL

# ─────────────────────────── Merkle Tree ─────────────────────────


class TestMerkleTree:
    def test_empty_tree(self):
        tree = MerkleTree([])
        assert tree.root_hash == MerkleTree([]).root_hash
        assert len(tree.leaves) == 0

    def test_single_leaf(self):
        tree = MerkleTree([b"hello"])
        assert len(tree.leaves) == 1
        assert tree.root is not None
        assert tree.root.is_leaf()

    def test_two_leaves(self):
        tree = MerkleTree([b"msg1", b"msg2"])
        assert len(tree.leaves) == 2
        assert tree.root is not None
        assert not tree.root.is_leaf()  # internal node
        # root hash = sha256(sha256(msg1) + sha256(msg2))
        assert len(tree.root_hash) == 64  # SHA256 hex

    def test_three_leaves_odd_count(self):
        tree = MerkleTree([b"a", b"b", b"c"])
        assert len(tree.leaves) == 3
        assert tree.root is not None

    def test_root_hash_stability(self):
        messages = [b"data1", b"data2", b"data3"]
        tree1 = MerkleTree(messages)
        tree2 = MerkleTree(messages)
        assert tree1.root_hash == tree2.root_hash

    def test_different_data_different_hash(self):
        assert MerkleTree([b"hello"]).root_hash != MerkleTree([b"world"]).root_hash

    def test_serialize_deserialize(self):
        messages = [b"msg1", b"msg2", b"msg3"]
        tree = MerkleTree(messages)
        data = tree.serialize()
        restored = MerkleTree.deserialize(data)
        assert restored.root_hash == tree.root_hash
        assert len(restored.leaves) == len(tree.leaves)

    def test_proof_generation(self):
        messages = [b"msg0", b"msg1", b"msg2", b"msg3"]
        tree = MerkleTree(messages)
        proof = tree.get_proof(1)  # proof for leaf 1
        assert len(proof) >= 1, "Proof должен содержать хеши"


# ─────────────────────────── Merkle Sync ────────────────────────


class TestMerkleSync:
    def test_identical_trees_no_diff(self):
        wal = [b"msg1", b"msg2"]
        sync = MerkleSync(wal)
        diff = sync.compute_diff(sync.local_tree.root_hash, len(wal))
        assert diff == [], "Одинаковые деревья — diff пуст"

    def test_empty_remote_returns_all(self):
        wal = [b"msg1", b"msg2"]
        sync = MerkleSync(wal)
        diff = sync.compute_diff("different_hash", 0)
        assert len(diff) == 2, "Пустой удалённый → отдаём всё"

    def test_newer_local_has_diff(self):
        local_wal = [b"msg1", b"msg2", b"msg3", b"msg4"]
        sync = MerkleSync(local_wal)
        diff = sync.compute_diff("old_hash", 2)  # у удалённого только 2
        assert len(diff) == 2, "Должны быть 2 новых сообщения"

    def test_verify_message(self):
        assert MerkleSync.verify_message(b"valid")
        assert not MerkleSync.verify_message(b"")


# ─────────────────────────── DePIN WAL ──────────────────────────


class TestDePINWAL:
    @pytest.fixture
    def wal(self):
        path = tempfile.mktemp(suffix="_test_wal.db")
        w = DePINWAL(path, "sensor_test_01")
        w.open()
        yield w
        w.close()
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_append_and_count(self, wal):
        h = wal.append("depin:sensor:telemetry", {"temperature": 23.5})
        assert h is not None
        assert len(h) == 64  # SHA256 hex
        assert wal.count() == 1

    def test_get_all(self, wal):
        wal.append("t1", {"val": 1})
        wal.append("t1", {"val": 2})
        wal.append("t2", {"val": 3})
        messages = wal.get_all()
        assert len(messages) == 3

    def test_get_since(self, wal):
        wal.append("t1", {"val": 1})
        wal.append("t1", {"val": 2})
        time.sleep(0.01)
        wal.append("t1", {"val": 3})
        recent = wal.get_since(1)  # after seq 1
        assert len(recent) >= 1

    def test_last_seq(self, wal):
        wal.append("t1", {"val": 1})
        wal.append("t1", {"val": 2})
        assert wal.last_seq() == 2

    def test_count_by_topic(self, wal):
        wal.append("depin:sensor:telemetry", {"t": 1})
        wal.append("depin:sensor:telemetry", {"t": 2})
        wal.append("depin:alerts", {"alert": "high_temp"})
        stats = wal.count_by_topic()
        assert stats.get("depin:sensor:telemetry") == 2
        assert stats.get("depin:alerts") == 1


# ─────────────────────────── Device Registry ─────────────────────


class TestDeviceRegistry:
    @pytest.fixture
    def registry(self):
        return DeviceRegistry(ttl_sec=3600)  # 1h TTL

    def test_register_new(self, registry):
        is_new = registry.register(
            "sensor_01",
            {
                "device_type": "temperature_sensor",
                "pubkey": "abc123",
            },
        )
        assert is_new is True

    def test_register_update(self, registry):
        registry.register("sensor_01", {"device_type": "temp"})
        is_new = registry.register("sensor_01", {"device_type": "temp"})
        assert is_new is False  # обновление

    def test_get_device(self, registry):
        registry.register("sensor_01", {"device_type": "humidity"})
        info = registry.get("sensor_01")
        assert info is not None
        assert info["device_type"] == "humidity"

    def test_get_nonexistent(self, registry):
        assert registry.get("nonexistent") is None

    def test_unregister(self, registry):
        registry.register("sensor_01", {"device_type": "temp"})
        assert registry.unregister("sensor_01") is True
        assert registry.unregister("sensor_01") is False

    def test_list_by_type(self, registry):
        registry.register("s1", {"device_type": "temp"})
        registry.register("s2", {"device_type": "temp"})
        registry.register("s3", {"device_type": "humidity"})
        temps = registry.list_by_type("temp")
        assert len(temps) == 2

    def test_heartbeat(self, registry):
        registry.register("sensor_01", {"device_type": "temp"})
        assert registry.heartbeat("sensor_01") is True
        assert registry.heartbeat("nonexistent") is False

    def test_count(self, registry):
        registry.register("s1", {"device_type": "temp"})
        registry.register("s2", {"device_type": "humidity"})
        assert registry.count() == 2

    def test_ttl_expiry(self):
        # TTL = 0 — устройства истекают сразу
        registry = DeviceRegistry(ttl_sec=0)
        registry.register("s1", {"device_type": "temp"})
        time.sleep(0.01)
        assert registry.get("s1") is None
        assert registry.count() == 0


# ─────────────────────────── DePIN Device (интеграционные) ──────


class TestDePINDevice:
    @pytest.mark.asyncio
    async def test_device_create_and_connect(self):
        device = DePINDevice(
            device_id="test_sensor_01",
            device_type="test_sensor",
        )
        did = await device.connect()
        assert did.startswith("did:"), "DID должен быть сформирован"
        assert device._connected is True

        # Реестр должен видеть устройство
        reg_info = DePINDevice.get_registry().get("test_sensor_01")
        assert reg_info is not None, "Устройство должно быть в реестре"

        await device.disconnect()
        assert device._connected is False

    @pytest.mark.asyncio
    async def test_device_publish_telemetry(self):
        device = DePINDevice(
            device_id="temp_sensor",
            device_type="temperature_sensor",
        )
        await device.connect()

        hash_val = await device.publish_telemetry(
            metrics={"temperature": 23.5, "humidity": 60.2},
            status={"battery": "85%", "firmware": "2.1.0"},
        )
        assert hash_val is not None
        assert len(hash_val) == 64  # SHA256

        # WAL должен содержать запись
        assert device.wal.count() == 1

        await device.disconnect()

    @pytest.mark.asyncio
    async def test_device_sync_status(self):
        device = DePINDevice(device_id="sync_test", device_type="sensor")
        await device.connect()

        # Публикуем телеметрию
        for i in range(3):
            await device.publish_telemetry({"value": float(i)})

        # Проверяем статус
        status = await device.sync_status()
        assert status["connected"] is True
        assert status["wal_messages"] == 3

        # Sync
        sync_result = await device.sync()
        assert "last_sync" in sync_result
        assert sync_result["wal_messages"] == 3

        await device.disconnect()

    @pytest.mark.asyncio
    async def test_device_command_callback(self):
        device = DePINDevice(device_id="cmd_test", device_type="actuator")
        await device.connect()

        received = []

        @device.on_command
        async def handle(cmd):
            received.append(cmd)

        # Симулируем входящую команду
        device._on_command({"payload": {"action": "reboot", "params": {"delay": 5}}})

        await asyncio.sleep(0.5)

        assert len(received) >= 1, "Command callback должен сработать"

        await device.disconnect()

    @pytest.mark.asyncio
    async def test_offline_telemetry_saved_to_wal(self):
        """Телеметрия пишется в WAL даже без mesh."""
        device = DePINDevice(device_id="offline_test", device_type="sensor")
        # Не вызываем connect() — устройство offline

        hash_val = await device.publish_telemetry({"temperature": 30.0})
        assert hash_val is not None, "WAL запись должна быть и без mesh"

        # WAL не открыт — publish_telemetry внутри wal.append
        # wal уже открыт в __init__
        assert device.wal.count() >= 1

        device.wal.close()
        try:
            os.unlink(device.wal.path)
        except OSError:
            pass

    @pytest.mark.asyncio
    async def test_two_devices_through_mesh(self):
        """Два DePIN устройства общаются через mesh."""
        d1 = DePINDevice(device_id="sender", device_type="sensor")
        d2 = DePINDevice(device_id="receiver", device_type="sensor")

        await asyncio.gather(d1.connect(), d2.connect())
        await asyncio.sleep(1)

        # Регистрация обоих
        assert DePINDevice.get_registry().count() == 2

        # Публикуем телеметрию от sender
        hash_val = await d1.publish_telemetry({"value": 42.0})
        assert hash_val is not None
        assert d1.wal.count() >= 1

        await asyncio.sleep(2)

        await asyncio.gather(d1.disconnect(), d2.disconnect())

    @pytest.mark.asyncio
    async def test_merkle_sync_after_offline(self):
        """Симуляция: устройство было offline, потом sync."""
        device = DePINDevice(device_id="merkle_test", device_type="sensor")
        await device.connect()

        # Публикуем несколько сообщений
        for i in range(5):
            await device.publish_telemetry({"seq": float(i)})

        assert device.wal.count() == 5

        # Синхронизация
        status = await device.sync()
        assert status["wal_messages"] == 5
        assert status["connected"] is True

        await device.disconnect()
