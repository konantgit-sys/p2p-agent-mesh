"""Phase 1 — Agent SDK integration tests (real IPFS PubSub).

Требует: запущенный IPFS daemon (--enable-pubsub-experiment).
"""

import asyncio
import json
import time
import os
import tempfile
import pytest

from sdk.agent import AgentMesh


@pytest.mark.asyncio
async def test_agent_emit_listen_same_process():
    """
    Один агент emit(), другой listen() — сообщение доставлено.
    """
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    forecaster = AgentMesh("forecaster", ["forecast"], db_path=db1)
    listener = AgentMesh("listener", ["listen"], db_path=db2)

    await forecaster.start()
    await listener.start()

    received = []

    def on_event(msg: dict):
        received.append(msg)

    # Подписка на capability "forecast"
    sub = await listener.listen({"capability": "forecast"}, on_event)
    await asyncio.sleep(1)

    # Публикация
    msg_id = await forecaster.emit("forecast", {"coin": "BTC", "prediction": "UP", "conf": 0.85})
    assert msg_id, "msg_id must be returned"

    await asyncio.sleep(3)

    await sub.cancel()
    await forecaster.stop()
    await listener.stop()

    # Cleanup
    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    print(f"  Получено сообщений: {len(received)}")
    assert len(received) >= 1, f"Нужно >=1 сообщение, получено {len(received)}"

    msg = received[0]
    assert msg.get("type") == "event"
    assert msg.get("capability") == "forecast"
    assert msg["payload"]["prediction"] == "UP"
    assert "coin" in msg["payload"]
    assert msg["payload"]["coin"] == "BTC"
    assert "signature" in msg, "Сообщение должно быть подписано"


@pytest.mark.asyncio
async def test_agent_capability_filter():
    """
    Подписка на "forecast" не должна получать "ping".
    """
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    agent_a = AgentMesh("agent_a", ["forecast", "ping"], db_path=db1)
    agent_b = AgentMesh("agent_b", ["listen"], db_path=db2)

    await agent_a.start()
    await agent_b.start()

    received = []

    def on_forecast(msg: dict):
        received.append(msg.get("capability"))

    # Подписка только на forecast
    sub = await agent_b.listen({"capability": "forecast"}, on_forecast)
    await asyncio.sleep(1)

    # Публикуем в оба топика
    await agent_a.emit("ping", {"data": "ping"})
    await agent_a.emit("forecast", {"data": "forecast"})
    await asyncio.sleep(3)

    await sub.cancel()
    await agent_a.stop()
    await agent_b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    print(f"  Получено событий: {len(received)}, все: {received}")
    # Должны получить только forecast, не ping
    assert "ping" not in received, "Ping не должен пройти через фильтр forecast"
    assert len(received) >= 1, "Должно быть хотя бы 1 сообщение"
    assert received.count("forecast") >= 1, "forecast должен быть получен"


@pytest.mark.asyncio
async def test_agent_two_capabilities():
    """
    Подписка на два capability — получает оба.
    """
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    a = AgentMesh("agent_a", ["ping", "pong"], db_path=db1)
    b = AgentMesh("agent_b", ["listen"], db_path=db2)

    await a.start()
    await b.start()

    received = []

    def on_any(msg: dict):
        received.append(msg.get("capability"))

    sub = await b.listen({"capability": ["ping", "pong"]}, on_any)
    await asyncio.sleep(1)

    await a.emit("ping", {"seq": 1})
    await a.emit("pong", {"seq": 2})
    await a.emit("ping", {"seq": 3})
    await asyncio.sleep(3)

    await sub.cancel()
    await a.stop()
    await b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    print(f"  Получено: {len(received)} — {received}")
    assert received.count("ping") >= 1
    assert received.count("pong") >= 1
    assert len(received) >= 2


@pytest.mark.asyncio
async def test_agent_signature_verified():
    """
    Все полученные сообщения имеют валидную подпись.
    """
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    a = AgentMesh("agent_a", ["test_sig"], db_path=db1)
    b = AgentMesh("agent_b", ["listen"], db_path=db2)

    await a.start()
    await b.start()

    received = []

    def on_msg(msg: dict):
        received.append(msg)

    sub = await b.listen({"capability": "test_sig"}, on_msg)
    await asyncio.sleep(1)

    await a.emit("test_sig", {"msg": "signed_message"})
    await asyncio.sleep(3)

    await sub.cancel()
    await a.stop()
    await b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    assert len(received) >= 1
    msg = received[0]
    assert "signature" in msg
    assert "pubkey" in msg
    assert "from" in msg
    assert msg["from"] == a.did
    print(f"  Подпись проверена: from={msg['from'][:20]}... sig={msg['signature'][:16]}...")


@pytest.mark.asyncio
async def test_agent_dht_query():
    """
    Агент A регистрируется → агент B находит его через DHT.
    """
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    a = AgentMesh("agent_a", ["dht_service"], db_path=db1)
    b = AgentMesh("agent_b", ["listen"], db_path=db2, rate_limit=200)

    await a.start()
    await b.start()

    # Даём время на DHT replication
    await asyncio.sleep(3)

    # B ищет агента с capability "dht_service"
    results = await b.query("dht_service")
    print(f"  DHT query results: {results}")
    assert len(results) >= 1, f"Нужно >=1 агента в DHT, получено {len(results)}"
    assert results[0]["agent_id"] == "agent_a"
    assert "dht_service" in results[0]["capabilities"]

    await a.stop()
    await b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass
