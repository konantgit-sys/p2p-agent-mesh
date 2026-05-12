"""LangGraph adapter tests (real IPFS PubSub).

Проверяет MeshTopic (pub/sub), MeshStateSync, MeshRPC.
"""

import asyncio
import os
import tempfile

import pytest

from adapters.langgraph_channel import MeshRPC, MeshStateSync, MeshTopic
from sdk.agent import AgentMesh


@pytest.mark.asyncio
async def test_mesh_topic_publish_subscribe():
    """Agent A publish → Agent B subscribe через MeshTopic."""
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    agent_a = AgentMesh("pub_agent", ["price_alerts"], db_path=db1)
    agent_b = AgentMesh("sub_agent", ["listen"], db_path=db2)

    await agent_a.start()
    await agent_b.start()

    topic_a = MeshTopic(agent_a, "price_alerts")
    topic_b = MeshTopic(agent_b, "price_alerts")

    received = []

    def on_alert(msg: dict):
        received.append(msg)

    await topic_b.subscribe(on_alert)
    await asyncio.sleep(1)

    msg_id = await topic_a.publish({"coin": "BTC", "price": 85000})
    assert msg_id, "msg_id must be returned"

    await asyncio.sleep(3)

    # Check topic B buffer
    latest = topic_b.get_latest()
    buffer = topic_b.get_buffer(5)
    status = topic_b.status()

    print(f"  Received: {len(received)} msgs")
    print(f"  Latest: {latest}")
    print(f"  Buffer: {buffer}")
    print(f"  Status: {status}")

    await topic_a.cancel()
    await topic_b.cancel()
    await agent_a.stop()
    await agent_b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    assert len(received) >= 1
    assert latest is not None
    assert latest.get("coin") == "BTC"
    assert latest.get("price") == 85000


@pytest.mark.asyncio
async def test_mesh_state_sync():
    """Agent A публикует состояние → Agent B видит его через MeshStateSync."""
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    agent_a = AgentMesh("state_a", ["sync"], db_path=db1)
    agent_b = AgentMesh("state_b", ["listen"], db_path=db2)

    await agent_a.start()
    await agent_b.start()

    sync_a = MeshStateSync(agent_a, "test_namespace")
    sync_b = MeshStateSync(agent_b, "test_namespace")

    await sync_b.start()
    await sync_a.start()
    await asyncio.sleep(1)

    await sync_a.update({"balance": 1.5, "positions": ["BTC"]})
    await asyncio.sleep(3)

    state_of_a = sync_b.get_latest("state_a")
    all_states = sync_b.get_all()
    print(f"  State of A: {state_of_a}")
    print(f"  All states: {all_states}")

    await sync_a.stop()
    await sync_b.stop()
    await agent_a.stop()
    await agent_b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    assert state_of_a is not None, "B должен видеть состояние A"
    assert state_of_a.get("balance") == 1.5
    assert "BTC" in state_of_a.get("positions", [])
    assert "state_a" in all_states


@pytest.mark.asyncio
async def test_mesh_rpc():
    """RPC request-response между двумя агентами."""
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    agent_a = AgentMesh("rpc_client", ["listen"], db_path=db1)
    agent_b = AgentMesh("rpc_server", ["calc_risk"], db_path=db2, rate_limit=200)

    await agent_a.start()
    await agent_b.start()

    rpc_client = MeshRPC(agent_a)
    rpc_server = MeshRPC(agent_b)

    # Server: register handler
    @rpc_server.on_request("calc_risk")
    async def handle_risk(req):
        # req — payload from the request
        return {"risk_score": 0.75, "max_loss": 500}

    # Start server listener
    await rpc_server.listen()
    await asyncio.sleep(1)

    # Client: send request
    result = await rpc_client.request("calc_risk", {"position": "BTC", "size": 1.0}, timeout=10.0)

    print(f"  RPC result: {result}")

    await agent_a.stop()
    await agent_b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    assert result is not None, "RPC должен вернуть результат"
    assert result.get("result", {}).get("risk_score") == 0.75
    assert result.get("result", {}).get("max_loss") == 500
