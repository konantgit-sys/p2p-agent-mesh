"""Phase 0 — Transport test: P2P pub/sub через глобальный relay."""

import asyncio
import json
import pytest
from phase0.transport import P2PTransport


@pytest.mark.asyncio
async def test_transport_pubsub_local():
    """
    1. Подписаться на топик
    2. Опубликовать сообщение в тот же топик
    3. Получить через callback (gossip на localhost)
    """
    transport = P2PTransport()
    peer_id = await transport.start()
    assert peer_id

    received = []
    topic = "test-p2p-local-" + peer_id[-8:]

    def on_message(data: bytes):
        received.append(data)

    await transport.subscribe(topic, on_message)
    # Ждём установки подписки
    await asyncio.sleep(0.2)

    msg = json.dumps({"hello": "from_phase0", "ts": asyncio.get_event_loop().time()}).encode()
    await transport.publish(topic, msg)
    # Ждём propagation
    await asyncio.sleep(0.3)

    await transport.unsubscribe(topic)
    await transport.stop()

    print(f"\nПолучено сообщений: {len(received)}")
    for i, r in enumerate(received):
        print(f"  [{i}] {r[:120]}")

    assert len(received) >= 1, f"Нужно >=1 сообщение, получено {len(received)}"
    assert b"hello" in received[0]


@pytest.mark.asyncio
async def test_transport_pubsub_two_topics():
    """Подписка на два топика, публикация в оба."""
    transport = P2PTransport()
    await transport.start()
    received = {1: [], 2: []}

    def make_cb(idx):
        def cb(data):
            received[idx].append(data)
        return cb

    await transport.subscribe("t1-" + transport.peer_id[-4:], make_cb(1))
    await transport.subscribe("t2-" + transport.peer_id[-4:], make_cb(2))
    await asyncio.sleep(0.2)

    await transport.publish("t1-" + transport.peer_id[-4:], b"msg_1")
    await transport.publish("t2-" + transport.peer_id[-4:], b"msg_2")
    await asyncio.sleep(0.3)

    for k, v in received.items():
        print(f"  topic {k}: {len(v)} сообщений")

    await transport.stop()

    assert len(received[1]) >= 1, f"Topic 1: {len(received[1])}"
    assert len(received[2]) >= 1, f"Topic 2: {len(received[2])}"


@pytest.mark.asyncio
async def test_transport_peers():
    """Проверка pubsub peers — должны видеть других пиров."""
    transport = P2PTransport()
    await transport.start()

    peers = await transport.peers()
    print(f"\nPeers в сети: {len(peers)}")
    for p in peers[:5]:
        print(f"  {p[:20]}...")
    assert len(peers) > 0, "Нужен хотя бы 1 peer"

    await transport.stop()
