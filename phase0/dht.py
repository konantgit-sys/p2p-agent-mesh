"""Phase 0 — DHT: key-value хранилище поверх IPFS PubSub.

Не классическая Kademlia, а distributed hash table через pub/sub:
- Все узлы подписаны на топик `_dht`
- PUT(key, val) → публикация в `_dht`, все узлы кэшируют
- GET(key) → проверка локального кэша
- K=3 репликация: последние 3 узла, которые PUT-или, хранят значение
- TTL: по умолчанию 86400 сек (24ч)
"""

import json
import time
from collections import OrderedDict
from typing import Optional


class DHTStore:
    """Локальный DHT кэш. Общается через transport.publish/subscribe."""

    def __init__(self, node_id: str, max_keys: int = 1000):
        self.node_id = node_id
        self._cache: OrderedDict[str, dict] = OrderedDict()
        self._max_keys = max_keys
        self._topic = "_dht"

    def handle_message(self, msg: dict) -> Optional[str]:
        """Обработать входящее DHT-сообщение."""
        payload = msg.get("payload", {})
        op = payload.get("op")
        if op == "put":
            key = payload.get("key")
            value = payload.get("value")
            ttl = payload.get("ttl", 86400)
            # Не реплицируем свои же сообщения
            if msg.get("from") == self.node_id:
                return None
            return self._store(key, value, ttl, msg.get("from"))
        elif op == "get":
            key = payload.get("key")
            return self._lookup(key)
        return None

    def _store(self, key: str, value, ttl: int, source: str) -> str:
        """Сохранить значение в локальном кэше (репликация)."""
        expires = time.time() + ttl
        entry = {
            "value": value,
            "expires": expires,
            "source": source,
            "stored_at": time.time()
        }
        self._cache[key] = entry
        self._cache.move_to_end(key)
        # Trim to max size
        while len(self._cache) > self._max_keys:
            self._cache.popitem(last=False)
        return key

    def put(self, key: str, value, ttl: int = 86400) -> dict:
        """Подготовить PUT сообщение для публикации в mesh."""
        return {
            "topic": self._topic,
            "payload": {
                "op": "put",
                "key": key,
                "value": value,
                "ttl": ttl
            }
        }

    def get(self, key: str) -> Optional[dict]:
        """Получить значение из локального кэша."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        if entry["expires"] < time.time():
            del self._cache[key]
            return None
        return {"key": key, "value": entry["value"], "source": entry["source"]}

    def get_topic(self) -> str:
        return self._topic
