"""
P2P Agent Mesh — Relay Redundancy тесты (фаза 0.6)

1. Multi-relay client failover
2. RelayLink connect + forward
3. Message forwarding между relay
"""
import asyncio, json, sys, unittest
sys.path.insert(0, '/home/agent/data/projects/p2p-agent-mesh')

from relay.redundancy import RelayLinkClient, MultiRelayClient


class FakeRelayPeer:
    """Заглушка relay для теста."""
    
    def __init__(self):
        self.messages: list[dict] = []
        self.accept_handshake = True
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not line:
                break
            msg = json.loads(line.decode().strip())
            self.messages.append(msg)
            
            if msg.get("type") == "relay_handshake" and self.accept_handshake:
                writer.write((json.dumps({
                    "type": "relay_handshake_ack",
                    "pubkey": "test_relay_peer",
                    "version": "0.6"
                }) + "\n").encode())
                await writer.drain()
            elif msg.get("type") == "relay_forward":
                writer.write((json.dumps({
                    "type": "relay_forward_ack",
                    "target": msg.get("target", ""),
                    "status": "delivered"
                }) + "\n").encode())
                await writer.drain()
        
        writer.close()


class TestRelayRedundancy(unittest.IsolatedAsyncioTestCase):
    
    async def test_1_relay_link_connect(self):
        """RelayLink подключается к peer relay"""
        fake = FakeRelayPeer()
        
        async def on_msg(target, payload):
            pass
        
        # Запускаем фейковый relay
        server = await asyncio.start_server(
            fake.handle_client, '127.0.0.1', 0
        )
        port = server.sockets[0].getsockname()[1]
        
        link = RelayLinkClient("test_local", on_msg)
        result = await link.connect('127.0.0.1', port)
        
        self.assertTrue(result)
        await asyncio.sleep(0.1)
        self.assertGreater(len(fake.messages), 0)
        self.assertEqual(fake.messages[0].get("type"), "relay_handshake")
        
        await link.stop()
        server.close()
        print(f"✅ TEST 1: RelayLink connect OK")
    
    async def test_2_relay_forward_message(self):
        """RelayLink форвардит сообщение на другой relay"""
        fake = FakeRelayPeer()
        
        server = await asyncio.start_server(
            fake.handle_client, '127.0.0.1', 0
        )
        port = server.sockets[0].getsockname()[1]
        
        forwarded = []
        async def on_msg(target, payload):
            forwarded.append((target, payload))
        
        link = RelayLinkClient("test_local", on_msg)
        await link.connect('127.0.0.1', port)
        await asyncio.sleep(0.1)
        
        result = await link.forward(
            {"type": "send", "data": "hello"},
            "target_pubkey_123"
        )
        self.assertTrue(result)
        await asyncio.sleep(0.1)
        
        self.assertGreater(len(fake.messages), 1)
        self.assertEqual(fake.messages[1].get("type"), "relay_forward")
        self.assertEqual(fake.messages[1].get("target"), "target_pubkey_123")
        
        await link.stop()
        server.close()
        print(f"✅ TEST 2: RelayLink forward OK")
    
    async def test_3_multi_relay_client_failover(self):
        """MultiRelayClient переключается при недоступности первого relay"""
        # Первый relay — фейковый (недоступен)
        # Второй — рабочий
        fake2 = FakeRelayPeer()
        
        server2 = await asyncio.start_server(
            fake2.handle_client, '127.0.0.1', 0
        )
        port2 = server2.sockets[0].getsockname()[1]
        
        client = MultiRelayClient([
            ("127.0.0.1", 1),     # заведомо недоступный порт
            ("127.0.0.1", port2),  # рабочий
        ])
        
        async def factory(host, port):
            """Пытаемся подключиться к порту, эмулируем RelayClient"""
            try:
                r, w = await asyncio.open_connection(host, port)
                w.close()
                return object()  # эмулируем успешный RelayClient
            except Exception:
                return None
        
        result = await client.connect(factory)
        self.assertTrue(result)
        self.assertEqual(client.current_relay, ("127.0.0.1", port2))
        
        await client.stop()
        server2.close()
        print(f"✅ TEST 3: MultiRelay failover → relay2 OK")
    
    async def test_4_multi_relay_all_down(self):
        """MultiRelayClient не подключается если все relay упали"""
        client = MultiRelayClient([
            ("127.0.0.1", 1),
            ("127.0.0.1", 2),
        ])
        
        async def factory(host, port):
            return None
        
        result = await client.connect(factory)
        self.assertFalse(result)
        self.assertIsNone(client.current_relay)
        print(f"✅ TEST 4: MultiRelay все недоступны → False")


if __name__ == "__main__":
    print("=== Relay Redundancy — 4 теста ===\n")
    unittest.main(verbosity=0)
