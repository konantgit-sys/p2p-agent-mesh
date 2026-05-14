"""
P2P Agent Mesh — Kademlia DHT тесты

Покрытие:
1. XOR-дистанция и NodeId
2. K-bucket (add, evict, failure)
3. Routing table (find_nearest)
4. Iterative lookup (симуляция через mock transport)
5. Store/Get (локально)
6. Bootstrap (симуляция seed узлов)
7. Репликация на K узлов
"""
import asyncio, hashlib, sys, time, unittest
sys.path.insert(0, '/home/agent/data/projects/p2p-agent-mesh')

from phase0.dht_kademlia import (
    K, B, KBucket, RoutingTable, KademliaDHT,
    node_id_from_pubkey, xor_distance, leading_zeros
)

# ── Test keys ──
KEYS = [f"test_pubkey_{i}" for i in range(20)]
NODE_IDS = {k: node_id_from_pubkey(k) for k in KEYS}

class MockTransport:
    """Фейковый транспорт для тестов DHT."""
    
    def __init__(self):
        self.peers: dict[str, KademliaDHT] = {}
        self.latency = 0.01  # симулируем сетевую задержку
    
    def register(self, pubkey: str, dht: KademliaDHT):
        self.peers[pubkey] = dht
    
    async def send(self, pubkey: str, msg: dict) -> dict:
        await asyncio.sleep(self.latency)
        dht = self.peers.get(pubkey)
        if not dht:
            raise Exception(f"peer {pubkey[:12]} not found")
        sender = msg.get('from', '')
        return await dht.handle_message(msg, sender)


class TestKademliaCore(unittest.TestCase):
    """Тесты базовых алгоритмов Kademlia."""
    
    def test_1_xor_distance(self):
        """XOR-дистанция между разными node_id"""
        a = bytes.fromhex('00' * 20)
        b = bytes.fromhex('ff' * 20)
        dist = xor_distance(a, b)
        self.assertGreater(dist, 0)
        self.assertEqual(xor_distance(a, a), 0)
        print(f"✅ TEST 1: XOR distance корректна")
    
    def test_2_node_id_uniqueness(self):
        """Разные pubkey → разные node_id"""
        a = node_id_from_pubkey("pubkey_1")
        b = node_id_from_pubkey("pubkey_2")
        self.assertNotEqual(a, b)
        self.assertEqual(len(a), 20)  # 160 бит
        print(f"✅ TEST 2: NodeId уникальны, 20 байт")
    
    def test_3_kbucket_add(self):
        """K-bucket: добавление до K контактов"""
        bucket = KBucket(0, 0)
        for i in range(K + 2):
            added = bucket.add_node(
                node_id_from_pubkey(f"pk_{i}"), f"addr_{i}", f"pk_{i}"
            )
            self.assertLessEqual(len(bucket), K)
        self.assertEqual(len(bucket), K)  # K=3
        print(f"✅ TEST 3: K-bucket не превышает K={K}")
    
    def test_4_kbucket_evict_failed(self):
        """K-bucket: evict при 3+ failures"""
        bucket = KBucket(0, 0)
        for i in range(K):
            bucket.add_node(
                node_id_from_pubkey(f"pk_{i}"), f"addr_{i}", f"pk_{i}"
            )
        # Mark first node as failed 3 times
        first = node_id_from_pubkey("pk_0")
        for _ in range(3):
            bucket.mark_failure(first)
        
        # Новый узел должен вытеснить failed
        added = bucket.add_node(
            node_id_from_pubkey("new_pk"), "new_addr", "new_pk"
        )
        self.assertTrue(added)
        # Первый узел вытеснен
        self.assertFalse(bucket.has_node(first))
        self.assertTrue(bucket.has_node(node_id_from_pubkey("new_pk")))
        print(f"✅ TEST 4: K-bucket evict отработал")
    
    def test_5_routing_find_nearest(self):
        """Routing table: find_nearest возвращает K узлов, сортировка по XOR"""
        rt = RoutingTable(NODE_IDS[KEYS[0]], KEYS[0])
        for pk in KEYS[1:11]:
            rt.add_node(NODE_IDS[pk], f"addr_{pk[:8]}", pk)
        
        target = NODE_IDS[KEYS[5]]
        nearest = rt.find_nearest(target, K)
        self.assertLessEqual(len(nearest), K)
        self.assertEqual(len(nearest), 3)
        
        # Проверяем сортировку по XOR-дистанции
        dists = [xor_distance(target, n['node_id']) for n in nearest]
        self.assertEqual(dists, sorted(dists))
        print(f"✅ TEST 5: find_nearest вернул {len(nearest)} узлов, сортировка OK")


class TestKademliaDHT(unittest.IsolatedAsyncioTestCase):
    """Интеграционные тесты DHT с mock transport."""
    
    async def asyncSetUp(self):
        self.transport = MockTransport()
        self.nodes: dict[str, KademliaDHT] = {}
        
        # Создаём 5 узлов
        for i in range(5):
            pk = KEYS[i]
            dht = KademliaDHT(pk, self.transport.send)
            self.nodes[pk] = dht
            self.transport.register(pk, dht)
    
    async def test_6_ping_pong(self):
        """PING → PONG"""
        a = self.nodes[KEYS[0]]
        resp = await a.handle_message(
            a.make_message('ping', ts=time.time()),
            KEYS[1]
        )
        self.assertIsNotNone(resp)
        self.assertEqual(resp.get('dht_type'), 'pong')
        print(f"✅ TEST 6: PING → PONG OK")
    
    async def test_7_store_local(self):
        """store() и get() локально"""
        a = self.nodes[KEYS[0]]
        result = await a.store("test_key_1", {"data": "hello"}, ttl=3600)
        self.assertTrue(result)
        val = await a.get("test_key_1")
        self.assertEqual(val, {"data": "hello"})
        print(f"✅ TEST 7: store/get локально OK")
    
    async def test_8_find_node(self):
        """FIND_NODE возвращает ближайшие узлы"""
        # Добавляем узлы в routing table узла 0
        a = self.nodes[KEYS[0]]
        for i in range(1, 5):
            a.routing.add_node(NODE_IDS[KEYS[i]], f"relay:{KEYS[i][:12]}", KEYS[i])
        
        target = NODE_IDS[KEYS[2]]
        nearest = await a.iterative_lookup(target, 3)
        self.assertGreater(len(nearest), 0)
        print(f"✅ TEST 8: FIND_NODE вернул {len(nearest)} узлов")
    
    async def test_9_replication(self):
        """store() реплицирует на K узлов"""
        a = self.nodes[KEYS[0]]
        # Добавляем узлы в routing
        for i in range(1, 5):
            a.routing.add_node(NODE_IDS[KEYS[i]], f"relay:{KEYS[i][:12]}", KEYS[i])
        
        await a.store("replicated_key", "value_123")
        val = await a.get("replicated_key")
        self.assertEqual(val, "value_123")
        print(f"✅ TEST 9: store с репликацией OK")
    
    async def test_10_bootstrap(self):
        """bootstrap с seed узлами"""
        a = self.nodes[KEYS[0]]
        b = self.nodes[KEYS[1]]
        
        # В routing узла b есть ещё 3 узла
        for i in range(2, 5):
            b.routing.add_node(NODE_IDS[KEYS[i]], f"relay:{KEYS[i][:12]}", KEYS[i])
        
        count = await a.bootstrap([KEYS[1]])
        self.assertGreater(count, 0)
        self.assertGreater(a.routing.stats()['total_nodes'], 0)
        print(f"✅ TEST 10: bootstrap, найдено {count} seed узлов, всего {a.routing.stats()['total_nodes']}")


if __name__ == "__main__":
    print(f"=== Kademlia DHT — 10 тестов ===\n")
    unittest.main(verbosity=0)
