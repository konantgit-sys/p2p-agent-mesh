# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""LangGraph adapter — MeshChannel для multi-agent коммуникации через P2P mesh.

Позволяет агентам на разных LangGraph-инстансах обмениваться событиями
через IPFS PubSub вместо Redis/Kafka/REST.

Архитектура:
- Каждый агент запускает свой LangGraph StateGraph
- MeshChannel подписывается на входящие события от других агентов
- При publish() — подписанное сообщение уходит в mesh, все получают
- Буфер хранит последние N сообщений (configurable)
"""

import asyncio
import json
from collections.abc import Callable

from sdk.agent import AgentMesh


class MeshTopic:
    """Канал для multi-agent коммуникации, совместимый с LangGraph patterns.

    Пересылает сообщения между LangGraph-нодами через P2P mesh.
    Каждый узел может быть отдельным агентом или рантаймом.

    Usage:
        # На агенте-подписчике:
        topic = MeshTopic(mesh, "price_alerts")
        def on_alert(msg):
            print(f"Получен алерт: {msg}")
        await topic.subscribe(on_alert)

        # На агенте-публикаторе (внутри LangGraph node):
        await topic.publish({"coin": "BTC", "price": 85000, "action": "BUY"})
    """

    def __init__(self, mesh: AgentMesh, capability: str, max_buffer: int = 100):
        self.mesh = mesh
        self.capability = capability
        self._buffer: list[dict] = []
        self._max_buffer = max_buffer
        self._callbacks: list[Callable] = []
        self._subscription: object | None = None

    async def publish(self, payload: dict) -> str:
        """Опубликовать событие в mesh через AgentMesh.emit()."""
        msg_id = await self.mesh.emit(self.capability, payload)
        self._buffer.append(payload)
        # Trim buffer
        if len(self._buffer) > self._max_buffer:
            self._buffer = self._buffer[-self._max_buffer :]
        return msg_id

    async def subscribe(self, callback: Callable):
        """Подписаться на события capability через AgentMesh.listen()."""
        self._callbacks.append(callback)

        if self._subscription is None:

            def route(msg: dict):
                self._buffer.append(msg.get("payload", {}))
                if len(self._buffer) > self._max_buffer:
                    self._buffer = self._buffer[-self._max_buffer :]
                for cb in self._callbacks:
                    try:
                        cb(msg)
                    except Exception as e:
                        print(f"[MeshTopic] callback error: {e}")

            self._subscription = await self.mesh.listen({"capability": self.capability}, route)

    async def cancel(self):
        """Отписаться от топика."""
        if self._subscription:
            await self._subscription.cancel()
            self._subscription = None
        self._callbacks.clear()

    def get_latest(self) -> dict | None:
        """Последнее полученное значение из буфера."""
        return self._buffer[-1] if self._buffer else None

    def get_buffer(self, limit: int = 10) -> list[dict]:
        """Последние N сообщений из буфера."""
        return self._buffer[-limit:]

    def status(self) -> dict:
        return {
            "capability": self.capability,
            "buffer_size": len(self._buffer),
            "subscribers": len(self._callbacks),
            "subscribed": self._subscription is not None,
        }


class MeshStateSync:
    """Синхронизация состояния между LangGraph-агентами.

    Каждый агент публикует своё состояние в mesh.
    Другие агенты получают обновления и обновляют локальный StateGraph.

    Usage:
        sync = MeshStateSync(mesh, "trading_state")
        await sync.start()

        # Внутри LangGraph node:
        sync.update({"balance": 1.5, "positions": ["BTC"]})
        other_state = sync.get_latest("agent_forecaster")
    """

    def __init__(self, mesh: AgentMesh, namespace: str):
        self.mesh = mesh
        self.namespace = namespace
        self._states: dict[str, dict] = {}
        self._topic: MeshTopic | None = None

    async def start(self):
        """Подписаться на обновления состояний других агентов."""
        self._topic = MeshTopic(self.mesh, f"state:{self.namespace}")

        def on_state(msg: dict):
            payload = msg.get("payload", {})
            sender = payload.get("agent_id") or msg.get("agent_id", "")
            if sender and sender != self.mesh.agent_id:
                self._states[sender] = payload.get("state", {})

        await self._topic.subscribe(on_state)

    async def update(self, state: dict):
        """Опубликовать своё состояние."""
        await self._topic.publish(
            {
                "agent_id": self.mesh.agent_id,
                "state": state,
            }
        )

    def get_latest(self, agent_id: str) -> dict | None:
        """Получить последнее состояние другого агента."""
        return self._states.get(agent_id)

    def get_all(self) -> dict[str, dict]:
        """Получить состояния всех известных агентов."""
        return dict(self._states)

    async def stop(self):
        if self._topic:
            await self._topic.cancel()


class MeshRPC:
    """Request-response между LangGraph-агентами через mesh.

    Позволяет одному агенту синхронно запросить выполнение
    действия у другого агента и получить результат.

    Usage:
        rpc = MeshRPC(mesh)

        # Сервер: обработчик запросов
        @rpc.on_request("calc_risk")
        async def handle_risk(req):
            return {"risk_score": 0.75, "max_loss": 500}

        # Клиент:
        result = await rpc.request("calc_risk", {"position": "BTC", "size": 1.0})
    """

    def __init__(self, mesh: AgentMesh):
        self.mesh = mesh
        self._handlers: dict[str, Callable] = {}

    def on_request(self, method: str):
        """Декоратор для регистрации обработчика RPC."""

        def decorator(func):
            self._handlers[method] = func
            return func

        return decorator

    async def request(self, method: str, params: dict, timeout: float = 30.0) -> dict | None:
        """Отправить RPC-запрос другому агенту."""
        return await self.mesh.request(
            method,
            {
                "method": method,
                "params": params,
            },
            timeout=timeout,
        )

    async def listen(self):
        """Запустить RPC-сервер — подписаться на входящие запросы."""

        async def _handle_async(msg: dict, method: str):
            """Async часть обработчика."""
            params = msg.get("payload", {}).get("params", {})
            handler = self._handlers.get(method)
            if handler:
                try:
                    result = handler(params)
                    if asyncio.iscoroutine(result):
                        result = await result
                    await self.mesh.reply_to(msg, {"result": result})
                except Exception as e:
                    await self.mesh.reply_to(msg, {"error": str(e)})

        def handle_raw(raw: bytes):
            """Синхронный callback из транспорта."""
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                return
            # Проверка подписи
            from phase0.identity import Identity

            if not Identity.verify(msg):
                return
            method = msg.get("payload", {}).get("method", "")
            if method in self._handlers:
                asyncio.create_task(_handle_async(msg, method))

        # Подписываемся напрямую через транспорт на request:{method}
        for method in self._handlers:
            topic = f"request:{method}"
            if topic not in self.mesh._subscribed_topics:
                self.mesh._subscribed_topics.add(topic)
                await self.mesh.transport.subscribe(topic, handle_raw)
