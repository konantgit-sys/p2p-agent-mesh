"""Latency benchmark — p50/p99 under load."""

import asyncio
import time
import pytest
from core.mesh import MeshNode


@pytest.mark.asyncio
async def test_latency_p2p():
    """Measure publish → receive latency for 100 messages."""
    sender = MeshNode("sender", [])
    receiver = MeshNode("receiver", [])

    await sender.start()
    await receiver.start()

    latencies = []
    done = asyncio.Event()

    def measure(msg):
        lat = time.time() - msg.get("ts", time.time())
        latencies.append(lat * 1000)  # ms
        if len(latencies) >= 100:
            done.set()

    receiver.subscribe("bench", measure)

    for i in range(100):
        await sender.publish("bench", f"payload_{i}".encode())
        await asyncio.sleep(0.001)

    await asyncio.wait_for(done.wait(), timeout=5.0)

    await sender.stop()
    await receiver.stop()

    assert len(latencies) >= 90, f"Only {len(latencies)} messages received"
    p50 = sorted(latencies)[len(latencies) // 2]
    p99 = sorted(latencies)[int(len(latencies) * 0.99)]
    print(f"\nLatency: p50={p50:.2f}ms  p99={p99:.2f}ms  n={len(latencies)}")
