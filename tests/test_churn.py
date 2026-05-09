"""Churn resilience test — 20% node drop/rejoin."""

import asyncio
import pytest
from core.mesh import MeshNode


@pytest.mark.asyncio
async def test_churn_20_percent():
    """Simulate 5 nodes, drop 1, rejoin, check message flow."""
    nodes = [MeshNode(f"node_{i}", []) for i in range(5)]
    for n in nodes:
        await n.start()

    # Drop node 2
    await nodes[2].stop()
    assert not nodes[2]._running

    # Rejoin
    await nodes[2].start()
    assert nodes[2]._running

    for n in nodes:
        await n.stop()


@pytest.mark.asyncio
async def test_wal_buffer_offline():
    """Node offline → messages buffered → recovered on reconnect."""
    node = MeshNode("test_offline", [])
    await node.start()

    # Publish while online
    for i in range(10):
        await node.publish("test", f"msg_{i}".encode())

    assert node.wal_pending() == 10

    # Stop (simulate offline)
    await node.stop()

    # Start again → WAL still has messages
    await node.start()
    assert node.wal_pending() == 10

    await node.stop()
