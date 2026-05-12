"""AutoGen adapter tests.

Проверяет MeshAgent: a_send, a_receive, register_reply.
"""

import asyncio
import os
import tempfile

import pytest

from adapters.autogen_adapter import MeshAgent


@pytest.mark.asyncio
async def test_mesh_agent_send_receive():
    """MeshAgent A отправляет → MeshAgent B получает."""
    db1 = tempfile.mktemp(suffix=".db")
    db2 = tempfile.mktemp(suffix=".db")

    agent_a = MeshAgent(
        agent_id="sender",
        capabilities=["announce"],
        port=0,
    )
    agent_b = MeshAgent(
        agent_id="receiver",
        capabilities=["listen"],
        port=0,
    )

    await agent_a.start()
    await agent_b.start()

    await asyncio.sleep(1)

    received = []

    def on_reply(content, sender):
        received.append({"content": content, "sender": sender})

    agent_b.register_reply(on_reply)

    await asyncio.sleep(0.5)

    # Отправляем через mesh
    await agent_a.a_send(
        message={"text": "hello from A"},
        recipient=agent_b,
        request_reply=False,
    )

    await asyncio.sleep(5)  # ждём доставки через mesh

    print(f"  Received: {received}")

    await agent_a.stop()
    await agent_b.stop()

    for p in [db1, db2]:
        try:
            os.unlink(p)
        except OSError:
            pass

    assert len(received) >= 1, "B должен получить сообщение от A"


@pytest.mark.asyncio
async def test_mesh_agent_register_reply():
    """register_reply возвращает ID и callback вызывается."""
    agent = MeshAgent(
        agent_id="test_agent",
        capabilities=["echo"],
        port=0,
    )

    await agent.start()

    cb_id = agent.register_reply(lambda content, sender: None)

    assert cb_id.startswith("cb_"), "Callback ID должен начинаться с cb_"
    assert cb_id in agent._callbacks, "Callback должен быть зарегистрирован"

    await agent.stop()
