#!/usr/bin/env python3
"""Соединить p2p-peer c p2p-dash через TCP.

Запускает mesh-агента и подключает его к p2p-dash (127.0.0.1:34983).
"""
import asyncio
import sys

sys.path.insert(0, "/home/agent/data/projects/p2p-agent-mesh")
from sdk.agent import AgentMesh

async def main():
    agent = AgentMesh(
        agent_id="p2p-connector",
        capabilities=["echo", "dash", "ping", "forecast"],
        port=9092,
        bootstrap_peers=["did:p2p:dashboard@127.0.0.1:34983"],
    )
    ok = await agent.start()
    if not ok:
        print("[connector] FAILED to start agent")
        sys.exit(1)

    print(f"[connector] Started. DID: {agent.did}")
    print(f"[connector] TCP port: {agent.transport._tcp_port}")
    print(f"[connector] Connected to: did:p2p:dashboard@127.0.0.1:34983")

    caps = {"capabilities": ["echo", "dash", "ping", "forecast"]}

    async def on_event(event):
        print(f"[connector] Received: {json.dumps(event, default=str)[:200]}")

    sub = await agent.listen(caps, on_event)
    print(f"[connector] Listening for: echo,dash,ping,forecast (sub={sub._id})")

    # Keep alive
    try:
        while True:
            await asyncio.sleep(60)
            print(f"[connector] Heartbeat. Peers: {list(agent.transport._tcp_connections.keys())}")
    except asyncio.CancelledError:
        await agent.stop()

if __name__ == "__main__":
    import json
    asyncio.run(main())
