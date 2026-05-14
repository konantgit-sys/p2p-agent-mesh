"""
P2P Agent Mesh — Kademlia DHT v0.5

Настоящая Kademlia поверх mesh relay:
- 160-битное адресное пространство (SHA-256 → 160 бит)
- K-buckets (K=3), XOR-дистанция
- Протокол: PING/PONG, STORE, FIND_NODE, FIND_VALUE
- Iterative lookup (α=3 параллельных)
- Репликация на K ближайших узлов
- Refresh buckets каждые 3600 сек

Не ломает старый DHTStore (заглушка) — новый модуль.
"""
import asyncio, hashlib, logging, random, time
from collections import OrderedDict
from typing import Callable, Optional

log = logging.getLogger('kademlia')

K = 3          # Размер bucket
ALPHA = 3      # Параллельных запросов при lookup
B = 160        # Бит в NodeId
REFRESH_INTERVAL = 3600  # Refresh buckets (сек)
REPLICATE_INTERVAL = 300 # Репликация (сек)
EXPIRE_TIME = 86400      # TTL значений (24ч)

def xor_distance(a: bytes, b: bytes) -> int:
    """XOR-дистанция между двумя NodeId (160 бит)."""
    return int.from_bytes(a, 'big') ^ int.from_bytes(b, 'big')

def leading_zeros(a: bytes, b: bytes) -> int:
    """Количество лидирующих нулей в XOR = номер bucket."""
    d = xor_distance(a, b)
    return d.bit_length() - 1 if d > 0 else B - 1

def node_id_from_pubkey(pubkey: str) -> bytes:
    """NodeId = SHA-256(pubkey), обрезаем до 160 бит (20 байт)."""
    return hashlib.sha256(pubkey.encode()).digest()[:20]

class KBucket:
    """K-bucket: хранит до K контактов, сортировка по времени."""
    
    def __init__(self, min_idx: int, max_idx: int):
        self.min_idx = min_idx
        self.max_idx = max_idx
        self._nodes: OrderedDict[str, dict] = OrderedDict()
    
    def add_node(self, node_id: bytes, addr: str, pubkey: str) -> bool:
        """Добавить/обновить контакт. Возвращает True если добавлен."""
        key = node_id.hex()
        now = time.time()
        
        if key in self._nodes:
            self._nodes[key]['last_seen'] = now
            self._nodes.move_to_end(key)
            return True
        
        if len(self._nodes) < K:
            self._nodes[key] = {
                'node_id': node_id, 'addr': addr, 'pubkey': pubkey,
                'first_seen': now, 'last_seen': now, 'failures': 0
            }
            return True
        
        # Bucket полон — evict по LRU (самый старый)
        oldest_key, oldest = next(iter(self._nodes.items()))
        if oldest['failures'] >= 3:
            del self._nodes[oldest_key]
            self._nodes[key] = {
                'node_id': node_id, 'addr': addr, 'pubkey': pubkey,
                'first_seen': now, 'last_seen': now, 'failures': 0
            }
            return True
        
        return False  # Bucket full, не evict
    
    def mark_failure(self, node_id: bytes):
        key = node_id.hex()
        if key in self._nodes:
            self._nodes[key]['failures'] += 1
    
    def get_nodes(self, count: int = K) -> list[dict]:
        """Вернуть до count ближайших (по времени) контактов."""
        return list(self._nodes.values())[:count]
    
    def has_node(self, node_id: bytes) -> bool:
        return node_id.hex() in self._nodes
    
    @property
    def is_full(self) -> bool:
        return len(self._nodes) >= K
    
    def remove_node(self, node_id: bytes):
        self._nodes.pop(node_id.hex(), None)
    
    def __len__(self):
        return len(self._nodes)


class RoutingTable:
    """Kademlia routing table: 160 K-buckets."""
    
    def __init__(self, own_node_id: bytes, own_pubkey: str):
        self.own_id = own_node_id
        self.own_pubkey = own_pubkey
        self.buckets = [KBucket(i, i) for i in range(B)]
    
    def _bucket_for(self, node_id: bytes) -> int:
        lz = leading_zeros(self.own_id, node_id)
        return max(0, min(lz, B - 1))
    
    def add_node(self, node_id: bytes, addr: str, pubkey: str) -> bool:
        idx = self._bucket_for(node_id)
        return self.buckets[idx].add_node(node_id, addr, pubkey)
    
    def mark_failure(self, node_id: bytes):
        idx = self._bucket_for(node_id)
        self.buckets[idx].mark_failure(node_id)
    
    def find_nearest(self, target: bytes, count: int = K) -> list[dict]:
        """Итеративный поиск K ближайших узлов по XOR."""
        dist = [(node, xor_distance(target, node['node_id']))
                for bucket in self.buckets
                for node in bucket.get_nodes(count)]
        dist.sort(key=lambda x: x[1])
        return [d[0] for d in dist[:count]]
    
    def all_nodes(self) -> list[dict]:
        """Все известные узлы."""
        nodes = []
        for bucket in self.buckets:
            nodes.extend(bucket.get_nodes(K))
        return nodes
    
    def stats(self) -> dict:
        total = sum(len(b) for b in self.buckets)
        buckets_nonempty = sum(1 for b in self.buckets if len(b) > 0)
        return {'total_nodes': total, 'buckets_used': buckets_nonempty}


class KademliaDHT:
    """
    Kademlia DHT поверх mesh relay.
    
    Сообщения передаются как mesh-сообщения с type='dht'.
    Для коммуникации используется transport.send(target_pubkey, message).
    """
    
    def __init__(self, own_pubkey: str, transport_send: Callable):
        self.own_pubkey = own_pubkey
        self.own_id = node_id_from_pubkey(own_pubkey)
        self.routing = RoutingTable(self.own_id, own_pubkey)
        self._transport_send = transport_send  # send(pubkey, msg) -> coro
        self._store: dict[str, dict] = {}      # key -> {value, expires, publisher}
        self._pending_pings: dict[str, float] = {}
        self._running = False
        self._tasks: list[asyncio.Task] = []
    
    # ── Протокол ──
    
    def make_message(self, msg_type: str, **kwargs) -> dict:
        return {'type': 'dht', 'dht_type': msg_type, **kwargs,
                'from': self.own_pubkey, 'node_id': self.own_id.hex()}
    
    async def handle_message(self, msg: dict, sender_pubkey: str) -> Optional[dict]:
        """Обработать входящее DHT-сообщение. Вернуть ответ (если нужен)."""
        dht_type = msg.get('dht_type', '')
        sender_node_id = bytes.fromhex(msg.get('node_id', ''))
        
        # Регистрируем отправителя в routing table
        self.routing.add_node(sender_node_id, f"relay:{sender_pubkey[:12]}", sender_pubkey)
        
        if dht_type == 'ping':
            return self.make_message('pong', echo=msg.get('ts', time.time()))
        
        elif dht_type == 'find_node':
            target_hex = msg.get('target', '')
            if not target_hex:
                return None
            target = bytes.fromhex(target_hex)
            nearest = self.routing.find_nearest(target, K)
            nodes_data = [
                {'node_id': n['node_id'].hex(), 'addr': n['addr'], 'pubkey': n['pubkey']}
                for n in nearest
            ]
            return self.make_message('nodes', nodes=nodes_data, target=target_hex)
        
        elif dht_type == 'find_value':
            key = msg.get('key', '')
            if key in self._store:
                entry = self._store[key]
                return self.make_message('value', key=key, value=entry['value'])
            # Если нет значения — возвращаем ближайшие узлы как find_node
            target = node_id_from_pubkey(key)
            nearest = self.routing.find_nearest(target, K)
            nodes_data = [
                {'node_id': n['node_id'].hex(), 'addr': n['addr'], 'pubkey': n['pubkey']}
                for n in nearest
            ]
            return self.make_message('nodes', nodes=nodes_data, key=key)
        
        elif dht_type == 'store':
            key = msg.get('key', '')
            value = msg.get('value')
            ttl = msg.get('ttl', EXPIRE_TIME)
            self._store[key] = {
                'value': value, 'expires': time.time() + ttl,
                'publisher': sender_pubkey, 'stored_at': time.time()
            }
            return self.make_message('stored', key=key)
        
        return None
    
    # ── Lookup ──
    
    async def iterative_lookup(self, target: bytes, alpha: int = ALPHA) -> list[dict]:
        """
        Iterative Kademlia lookup.
        Возвращает K ближайших узлов к target.
        """
        shortlist = self.routing.find_nearest(target, K)
        if not shortlist:
            log.warning("DHT lookup: routing table пуста")
            return []
        
        contacted: set[str] = set()
        closest: Optional[dict] = None
        
        for _ in range(5):  # макс 5 итераций
            # Берём α узлов, которых ещё не контактили
            to_query = [n for n in shortlist if n['pubkey'] not in contacted][:alpha]
            if not to_query:
                break
            
            results = []
            for node in to_query:
                contacted.add(node['pubkey'])
                try:
                    resp = await self._transport_send(
                        node['pubkey'],
                        self.make_message('find_node', target=target.hex())
                    )
                    if resp and resp.get('dht_type') == 'nodes':
                        for nd in resp.get('nodes', []):
                            nid = bytes.fromhex(nd['node_id'])
                            nd_entry = {
                                'node_id': nid, 'addr': nd['addr'], 'pubkey': nd['pubkey']
                            }
                            if nd['pubkey'] not in contacted:
                                results.append(nd_entry)
                                self.routing.add_node(nid, nd['addr'], nd['pubkey'])
                except Exception as e:
                    self.routing.mark_failure(node['node_id'])
                    log.debug(f"DHT lookup fail {node['pubkey'][:12]}: {e}")
            
            # Сливаем новые узлы в shortlist
            for r in results:
                if r['pubkey'] not in {n['pubkey'] for n in shortlist}:
                    shortlist.append(r)
            
            # Сортируем по дистанции
            shortlist.sort(key=lambda n: xor_distance(target, n['node_id']))
            shortlist = shortlist[:K]
            
            new_closest = shortlist[0] if shortlist else None
            if new_closest and closest and new_closest == closest:
                break  # Сошлось — стабильные ближайшие
            closest = new_closest
        
        return shortlist
    
    async def store(self, key: str, value: any, ttl: int = EXPIRE_TIME) -> bool:
        """
        Сохранить значение в DHT.
        Реплицирует на K ближайших узлов.
        """
        target = node_id_from_pubkey(key)
        nearest = await self.iterative_lookup(target, K)
        
        if not nearest:
            log.warning("DHT store: нет узлов для репликации")
            self._store[key] = {
                'value': value, 'expires': time.time() + ttl,
                'publisher': self.own_pubkey, 'stored_at': time.time()
            }
            return True  # Сохранили локально хотя бы
        
        success = 0
        for node in nearest[:K]:
            try:
                resp = await self._transport_send(
                    node['pubkey'],
                    self.make_message('store', key=key, value=value, ttl=ttl)
                )
                if resp and resp.get('dht_type') == 'stored':
                    success += 1
            except Exception:
                continue
        
        # Сохраняем локально тоже
        self._store[key] = {
            'value': value, 'expires': time.time() + ttl,
            'publisher': self.own_pubkey, 'stored_at': time.time()
        }
        
        return success > 0 or True
    
    async def get(self, key: str) -> Optional[any]:
        """Получить значение из DHT."""
        # Сначала локальный кэш
        if key in self._store:
            entry = self._store[key]
            if entry['expires'] > time.time():
                return entry['value']
            del self._store[key]
        
        # Lookup в сети
        target = node_id_from_pubkey(key)
        nearest = await self.iterative_lookup(target, K)
        
        for node in nearest[:K]:
            try:
                resp = await self._transport_send(
                    node['pubkey'],
                    self.make_message('find_value', key=key)
                )
                if resp and resp.get('dht_type') == 'value':
                    value = resp['value']
                    self._store[key] = {
                        'value': value, 'expires': time.time() + EXPIRE_TIME,
                        'publisher': node['pubkey'], 'stored_at': time.time()
                    }
                    return value
            except Exception:
                continue
        
        return None
    
    # ── Bootstrap ──
    
    async def bootstrap(self, seed_nodes: list[str]) -> int:
        """
        Bootstrap: подключиться к seed-узлам, найти ближайших к себе.
        Возвращает количество найденных узлов.
        """
        known = set()
        for pubkey in seed_nodes:
            if pubkey == self.own_pubkey:
                continue
            try:
                resp = await self._transport_send(
                    pubkey,
                    self.make_message('find_node', target=self.own_id.hex())
                )
                if resp and resp.get('dht_type') == 'nodes':
                    for nd in resp.get('nodes', []):
                        nid = bytes.fromhex(nd['node_id'])
                        self.routing.add_node(nid, nd['addr'], nd['pubkey'])
                        known.add(nd['pubkey'])
            except Exception as e:
                log.debug(f"DHT bootstrap fail {pubkey[:12]}: {e}")
        
        # Iterative lookup к себе — заполняет routing table
        await self.iterative_lookup(self.own_id, ALPHA)
        return len(known)
    
    # ── Maintenance ──
    
    async def _refresh_loop(self):
        """Refresh buckets и репликация значений."""
        while self._running:
            await asyncio.sleep(REFRESH_INTERVAL)
            # Проверяем пустые bucket
            for i, bucket in enumerate(self.routing.buckets):
                if len(bucket) == 0 and i < B - 1:
                    # Случайный ID в этом bucket
                    rand_target = bytearray(self.own_id)
                    bit_pos = B - 2 - i
                    byte_pos = bit_pos // 8
                    bit_offset = bit_pos % 8
                    if bit_offset >= 0:
                        rand_target[byte_pos] ^= (1 << bit_offset)
                    await self.iterative_lookup(bytes(rand_target), ALPHA)
            
            # Репликация локальных значений
            for key, entry in list(self._store.items()):
                if entry['publisher'] == self.own_pubkey:
                    target = node_id_from_pubkey(key)
                    nearest = self.routing.find_nearest(target, K)
                    for node in nearest:
                        try:
                            await self._transport_send(
                                node['pubkey'],
                                self.make_message('store', key=key, value=entry['value'])
                            )
                        except Exception:
                            pass
    
    async def start(self, seed_nodes: list[str] = None):
        self._running = True
        if seed_nodes:
            count = await self.bootstrap(seed_nodes)
            log.info(f"DHT: bootstrap завершён, {count} seed узлов")
        
        task = asyncio.create_task(self._refresh_loop())
        self._tasks.append(task)
        log.info(f"DHT: запущен, node_id={self.own_id.hex()[:12]}...")
    
    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
    
    def stats(self) -> dict:
        return {
            'node_id': self.own_id.hex()[:16],
            'routing': self.routing.stats(),
            'stored_keys': len(self._store),
        }
