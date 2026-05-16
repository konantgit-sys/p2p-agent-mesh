# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Agent-to-Agent SDK — high-level API поверх Phase 0 (IPFS PubSub + WAL + Identity + DHT).



Агент может emit() события, listen() на события других агентов,
query() по DHT, request() с ответом.
"""

import asyncio
import hashlib
import json
import time
from collections.abc import Callable

from phase0.dht import DHTStore
from phase0.identity import Identity
from phase0.sig_gate import SigGate
from phase0.transport import P2PTransport
from phase0.wal import WALBuffer

# RelayClient — опциональный импорт (только при использовании relay)
try:
    from relay.client import RelayClient  # type: ignore[import-untyped]
except ImportError:
    RelayClient = None  # type: ignore


class Subscription:
    """Подписка на события с фильтром. Можно отписаться через .cancel()."""

    def __init__(self, filter_dict: dict, callback: Callable, owner: "AgentMesh"):
        self.filter = filter_dict
        self.callback = callback
        self._owner = owner
        self._id = hashlib.md5(json.dumps(filter_dict, sort_keys=True).encode()).hexdigest()[:12]
        self._cancelled = False

    def matches(self, msg: dict) -> bool:
        """Проверить, подходит ли сообщение под фильтр."""
        if self._cancelled:
            return False

        caps = self.filter.get("capabilities") or self.filter.get("capability")
        if caps is None:
            return True  # без фильтра — всё подходит

        msg_cap = msg.get("capability", "")
        if isinstance(caps, list):
            return msg_cap in caps
        return msg_cap == caps

    async def cancel(self):
        """Отписаться."""
        self._cancelled = True
        # Если это была последняя подписка на топик — отписываем транспорт
        await self._owner._cleanup_topic(self.filter)

    def __repr__(self):
        return f"<Subscription {self._id} filter={self.filter}>"


class AgentMesh:
    """Высокоуровневый SDK для AI-агентов поверх IPFS PubSub mesh.

    Usage:
        agent = AgentMesh("forecaster_v2", ["crypto_analysis", "forecast"])
        await agent.start()

        sub = await agent.listen({"capability": "price_update"}, callback)
        msg_id = await agent.emit("forecast", {"coin": "BTC", "prediction": "UP"})

        agents = await agent.query("forecast")
        await agent.stop()
    """

    def __init__(
        self,
        agent_id: str,
        capabilities: list[str],
        identity: Identity | None = None,
        db_path: str | None = None,
        rate_limit: int = 50,
        port: int = 0,
        relay_host: str | None = None,
        relay_port: int = 0,
    ):
        self.agent_id = agent_id
        self.capabilities = capabilities
        self.identity = identity or Identity()
        self.transport = P2PTransport(node_id=agent_id)
        self.wal = WALBuffer(db_path or f"/tmp/p2p_mesh_{agent_id}.db")
        self.sig_gate = SigGate(rate_limit=rate_limit)
        self.dht = DHTStore(self.identity.did)
        self._port = port
        self._relay_host = relay_host
        self._relay_port = relay_port
        self._relay: RelayClient | None = None
        self._subscriptions: list[Subscription] = []
        self._subscribed_topics: set[str] = set()
        self._running = False
        self._last_msg_id: str | None = None
        self._last_dht_repub: float = 0  # cooldown для DHT republish

    async def start(self) -> str:
        """Подключиться к IPFS, подписаться на DHT, зарегистрироваться."""
        self._running = True
        peer_id = await self.transport.start(port=self._port)

        # Если указан relay — подключаемся
        if self._relay_host and self._relay_port:
            from relay.client import RelayClient

            self._relay = RelayClient(
                identity=self.identity,
                relay_host=self._relay_host,
                relay_port=self._relay_port,
                capabilities=self.capabilities,
            )
            relay_ok = await self._relay.connect()
            print(f"[agent] Relay {'connected' if relay_ok else 'FAILED'}")
        else:
            self._relay = None

        # Подписка на DHT топик (для discovery)
        await self.transport.subscribe(self.dht.get_topic(), self._on_dht_msg)
        self._subscribed_topics.add(self.dht.get_topic())

        # Публикация своего профиля в DHT
        await self._publish_metadata()
        # Републикация каждые 60 сек для DHT convergence
        self._dht_repub_task = asyncio.create_task(self._dht_republish_loop())

        print(f"[agent:{self.agent_id}] Started. DID={self.identity.did[:24]}...")
        return peer_id

    async def _publish_metadata(self):
        """Опубликовать метаданные агента в DHT."""
        dht_key = f"agent:{self.identity.did}"
        dht_value = {
            "agent_id": self.agent_id,
            "did": self.identity.did,
            "capabilities": self.capabilities,
            "reputation": 1.0,
            "ts": time.time(),
        }
        msg = self.dht.put(dht_key, dht_value)
        signed = self.identity.sign_message(msg)
        self.wal.append(signed)
        await self.transport.publish(self.dht.get_topic(), json.dumps(signed).encode())

    async def _dht_republish_loop(self):
        """Периодическая републикация DHT метаданных."""
        try:
            while self._running:
                await asyncio.sleep(60)
                await self._publish_metadata()
        except asyncio.CancelledError:
            pass

    def _on_dht_msg(self, raw: bytes):
        """Обработчик DHT сообщений (репликация каталога агентов).

        При получении от незнакомого пира — републикует свои метаданные.
        Это компенсирует отсутствие истории в gossipsub (late-joiners).
        """
        import json
        import time

        try:
            json.loads(raw)
        except json.JSONDecodeError:
            return
        # Верифицируем подпись
        checked = self.sig_gate.check(raw)
        if checked is not None:
            sender_did = checked.get("from", "")
            # Если это не наше сообщение — реплицируем
            if sender_did != self.identity.did:
                self.dht.handle_message(checked)
                # Republish свои метаданные для late-joiners (с cooldown)
                now = time.time()
                if now - self._last_dht_repub > 1.0:
                    self._last_dht_repub = now
                    asyncio.create_task(self._publish_metadata())

    def _route_message(self, topic: str, data: bytes):
        """Принять сырое сообщение из транспорта, проверить, разослать подписчикам.

        Синхронная — вызывается из транспортного callback.
        WAL append синхронный (SQLite).
        """
        import json

        try:
            msg = json.loads(data)
        except json.JSONDecodeError:
            return

        # Sig gate: верификация подписи + rate limit (синхронно)
        passed = self.sig_gate.check(data)
        if passed is None:
            return  # rejected

        # WAL: сохраняем (синхронный SQLite)
        self.wal.append(msg)

        # Запоминаем последний msg_id для sync_on_reconnect
        self._last_msg_id = msg.get("id", "")

        # Рассылка подписчикам (синхронные callback'и)
        for sub in list(self._subscriptions):
            if sub.matches(msg):
                try:
                    res = sub.callback(msg)
                    # Если callback — async def, запускаем в event loop
                    if asyncio.iscoroutine(res):
                        asyncio.create_task(res)
                except Exception as e:
                    print(f"[agent:{self.agent_id}] callback error: {e}")

    async def emit(self, capability: str, payload: dict) -> str:
        """Опубликовать событие от имени агента.

        Args:
            capability: строка-идентификатор (например "crypto_analysis")
            payload: произвольный dict с данными события

        Returns:
            msg_id: ID сообщения (хеш sha256[:16])
        """
        topic = f"agent:{capability}"
        msg = {
            "type": "event",
            "topic": topic,
            "capability": capability,
            "from": self.identity.did,
            "payload": payload,
            "ts": time.time(),
            "agent_id": self.agent_id,
        }
        # Подписываем
        signed = self.identity.sign_message(msg)

        # WAL first (durable storage до отправки)
        msg_id = self.wal.append(signed)

        # Публикуем
        await self.transport.publish(topic, json.dumps(signed).encode())
        return msg_id

    async def listen(self, filter_dict: dict, callback: Callable) -> Subscription:
        """Подписаться на события с фильтром.

        Args:
            filter_dict:
                {"capability": "crypto_analysis"} — точное совпадение
                {"capability": ["a", "b"]} — любой из списка
                {"capabilities": ["a"]} — legacy сокращение
            callback: Callable[[dict], None] — получит dict сообщения

        Returns:
            Subscription — можно .cancel()
        """
        sub = Subscription(filter_dict, callback, self)
        self._subscriptions.append(sub)

        # Определяем топики для подписки
        caps = filter_dict.get("capabilities") or filter_dict.get("capability")
        if caps is None:
            # Подписываемся на все agent:* топики
            topics_to_sub = ["agent:*"]
        elif isinstance(caps, str):
            topics_to_sub = [f"agent:{caps}"]
        else:
            topics_to_sub = [f"agent:{c}" for c in caps]

        for topic in topics_to_sub:
            if topic not in self._subscribed_topics:
                self._subscribed_topics.add(topic)
                # Если wildcard — здесь бы потребовалась routing-нода,
                # для MVP игнорируем (подписка только на явные топики)
                if topic != "agent:*":
                    await self.transport.subscribe(
                        topic, lambda d, t=topic: self._route_message(t, d)
                    )

        return sub

    async def _cleanup_topic(self, filter_dict: dict):
        """Проверить, можно ли отписаться от топика (если нет активных подписок)."""
        caps = filter_dict.get("capabilities") or filter_dict.get("capability", "")
        topics = [f"agent:{c}" for c in (caps if isinstance(caps, list) else [caps])]

        for topic in topics:
            # Проверяем, есть ли ещё подписки на этот топик
            active = any(
                s is not None
                and not s._cancelled
                and (
                    s.filter.get("capability") == c
                    or (
                        isinstance(s.filter.get("capability"), list)
                        and c in s.filter.get("capability")
                    )
                    or (
                        isinstance(s.filter.get("capabilities"), list)
                        and c in s.filter.get("capabilities")
                    )
                )
                for s in self._subscriptions
                for c in ([caps] if isinstance(caps, str) else caps)
            )
            if not active and topic in self._subscribed_topics:
                self._subscribed_topics.discard(topic)
                await self.transport.unsubscribe(topic)

    async def query(self, capability: str, min_reputation: float = 0.0) -> list[dict]:
        """Поиск агентов по capability через DHT.

        Args:
            capability: искомая capability
            min_reputation: минимальная репутация (0.0 = все)

        Returns:
            Список dict с полями: agent_id, did, capabilities, reputation
        """
        results = []
        # Ищем в DHT-кэше агентов с подходящей capability
        # DHT хранит ключи "agent:{did}" → метаданные
        # Для поиска по capability — линейный просмотр кэша (MVP)
        # В production: capability bloom filter в DHT
        for key, entry in list(self.dht._cache.items()):
            if not key.startswith("agent:"):
                continue
            val = entry.get("value", {})
            if not isinstance(val, dict):
                continue
            caps = val.get("capabilities", [])
            rep = val.get("reputation", 0)
            if capability in caps and rep >= min_reputation:
                results.append(
                    {
                        "agent_id": val.get("agent_id", ""),
                        "did": val.get("did", ""),
                        "capabilities": caps,
                        "reputation": rep,
                    }
                )
        return results

    async def request(
        self, target_capability: str, payload: dict, timeout: float = 30.0
    ) -> dict | None:
        """Request-response через mesh.

        Отправляет запрос в топик request:{capability},
        подписывается напрямую на транспорт (reply:{did}),
        ждёт ответ до timeout.
        """
        topic = f"request:{target_capability}"
        reply_to = f"reply:{self.identity.did}"

        msg = {
            "type": "request",
            "topic": topic,
            "capability": target_capability,
            "from": self.identity.did,
            "payload": payload,
            "reply_to": reply_to,
            "ts": time.time(),
        }
        signed = self.identity.sign_message(msg)

        # Создаём Event для ожидания ответа
        response_event = asyncio.Event()
        response_data = [None]

        def on_reply_raw(raw: bytes):
            """Прямой callback из транспорта (минует agent: префикс)."""
            import json

            try:
                reply_msg = json.loads(raw)
            except json.JSONDecodeError:
                return
            # Проверка подписи
            if self.sig_gate.check(raw) is not None:
                response_data[0] = reply_msg.get("payload", {})
                response_event.set()

        # Подписываемся напрямую на reply:{did} (без listen())
        self._subscribed_topics.add(reply_to)
        await self.transport.subscribe(reply_to, on_reply_raw)

        # Отправляем запрос
        self.wal.append(signed)
        await self.transport.publish(topic, json.dumps(signed).encode())

        # Ждём ответ с таймаутом
        try:
            await asyncio.wait_for(response_event.wait(), timeout=timeout)
        except TimeoutError:
            await self.transport.unsubscribe(reply_to)
            self._subscribed_topics.discard(reply_to)
            return None

        await self.transport.unsubscribe(reply_to)
        self._subscribed_topics.discard(reply_to)
        return response_data[0]

    async def reply_to(self, request_msg: dict, response_payload: dict):
        """Ответить на request (вызывается агентом-обработчиком)."""
        reply_topic = request_msg.get("reply_to", "")
        if not reply_topic:
            return

        msg = {
            "type": "response",
            "topic": reply_topic,
            "capability": reply_topic,
            "from": self.identity.did,
            "payload": response_payload,
            "in_reply_to": request_msg.get("id", ""),
            "ts": time.time(),
        }
        signed = self.identity.sign_message(msg)
        self.wal.append(signed)
        await self.transport.publish(reply_topic, json.dumps(signed).encode())

    async def sync_on_reconnect(self, topics: list[str] | None = None) -> int:
        """Догнать пропущенные сообщения из WAL после reconnection.

        Args:
            topics: список топиков для replay (None = все)

        Returns:
            Количество догнанных сообщений
        """
        if not self._last_msg_id:
            return 0

        count = 0
        replay_topics = topics or [t for t in self._subscribed_topics if t != self.dht.get_topic()]
        for topic in replay_topics:
            replayed = self.wal.replay(topic, since_id=self._last_msg_id)
            for msg in replayed:
                # Dispatch to subscribers
                for sub in list(self._subscriptions):
                    if sub.matches(msg):
                        try:
                            sub.callback(msg)
                        except Exception as e:
                            print(f"[agent:{self.agent_id}] sync callback error: {e}")
                count += 1
        return count

    async def stop(self):
        """Остановить все подписки и транспорт."""
        self._running = False
        if hasattr(self, "_dht_repub_task"):
            self._dht_repub_task.cancel()
        for topic in list(self._subscribed_topics):
            await self.transport.unsubscribe(topic)
        self._subscribed_topics.clear()
        self._subscriptions.clear()
        await self.transport.stop()
        self.wal.close()

    def status(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "did": self.identity.did,
            "capabilities": self.capabilities,
            "subscriptions": len(self._subscriptions),
            "topics": list(self._subscribed_topics),
            "wal_count": self.wal.count(),
            "sig_gate": self.sig_gate.stats(),
        }

    @property
    def did(self) -> str:
        return self.identity.did
