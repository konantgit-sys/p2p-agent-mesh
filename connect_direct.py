#!/usr/bin/env python3
"""Соединить p2p-peer c p2p-dash через прямой TCP.

Запускает P2PTransport и подключается к dashboard на 127.0.0.1:34983.
"""
import asyncio, json, signal, sys

sys.path.insert(0, "/home/agent/data/projects/p2p-agent-mesh")
from phase0.transport import P2PTransport
from phase0.identity import Identity

async def main():
    ident = Identity()
    ident.generate()
    
    transport = P2PTransport(identity=ident)
    peer_id = await transport.start(host="127.0.0.1", port=9093)
    print(f"[peer] Started. PeerID: {peer_id}")
    print(f"[peer] TCP: 127.0.0.1:{transport._tcp_port}")
    
    # Подключаемся к dashboard
    await transport.connect_peer("did:p2p:dashboard", "127.0.0.1", 34983)
    print(f"[peer] Connected to dashboard:127.0.0.1:34983")
    
    # Подписка на топики
    received = []
    async def on_echo(data):
        print(f"[peer] Echo: {data}")
        received.append(data)
    
    async def on_dash(data):
        print(f"[peer] Dash: {data}")
        received.append(data)
    
    await transport.subscribe("agent:echo", on_echo)
    await transport.subscribe("agent:dash", on_dash)
    print("[peer] Subscribed to agent:echo, agent:dash")
    
    # Публикуем heartbeat
    async def heartbeat():
        while True:
            await asyncio.sleep(30)
            msg = {"type": "heartbeat", "from": "p2p-connector", "ts": asyncio.get_event_loop().time()}
            await transport.publish("agent:echo", json.dumps(msg))
            print(f"[peer] Heartbeat sent. Peers: {transport._tcp_connections.keys()}")
    
    asyncio.create_task(heartbeat())
    
    # Ждём
    stop = asyncio.Future()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(s, lambda: stop.set_result(True))
        except NotImplementedError:
            pass
    await stop
    await transport.stop()

if __name__ == "__main__":
    asyncio.run(main())
