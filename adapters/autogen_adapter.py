# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""AutoGen adapter — MeshAgent для P2P коммуникации.

Позволяет AutoGen-агентам общаться через P2P mesh вместо
внутреннего message bus AutoGen.

Использование:
    from autogen import AssistantAgent, UserProxyAgent
    from adapters.autogen_adapter import MeshAgent

    mesh_agent = MeshAgent(
        agent_id="auto_assistant",
        capabilities=["code_gen", "analysis"],
    )
    await mesh_agent.start()

    # AutoGen AssistantAgent с mesh-бэкендом
    assistant = AssistantAgent(
        name="Assistant",
        llm_config={...},
        agent=mesh_agent,         # ← P2P mesh вместо стандартного bus
    )
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from sdk.agent import AgentMesh


class MeshAgent:
    """AutoGen-compatible wrapper вокруг P2P Agent Mesh.

    Реализует минимальный интерфейс для работы с AssistantAgent:
    - receive: получает сообщения из mesh (по подписке)
    - send: отправляет сообщения в mesh (публикует в capability)
    """

    def __init__(
        self,
        agent_id: str,
        capabilities: list[str],
        port: int = 0,
        relay_host: str | None = None,
        relay_port: int = 0,
    ):
        self.agent_id = agent_id
        self.capabilities = capabilities
        self._mesh = AgentMesh(
            agent_id=agent_id,
            capabilities=capabilities,
            port=port,
            relay_host=relay_host,
            relay_port=relay_port,
        )
        self._callbacks: dict[str, Callable] = {}

    async def start(self) -> str:
        """Подключиться к mesh."""
        peer_id = await self._mesh.start()
        # Подписка на все свои capability
        await self._mesh.listen(
            {"capability": self.capabilities},
            callback=self._on_mesh_event,
        )
        return peer_id

    async def stop(self):
        """Отключиться от mesh."""
        await self._mesh.stop()

    # ───────────────────────── AutoGen interface ─────────────────────────

    async def a_send(
        self,
        message: dict[str, Any],
        recipient: MeshAgent,
        request_reply: bool = False,
    ) -> None:
        """Отправить сообщение через mesh (публикация в capability получателя)."""
        payload = {
            "type": "autogen_message",
            "from": self.agent_id,
            "to": recipient.agent_id,
            "content": message,
            "request_reply": request_reply,
            "ts": time.time(),
        }
        for cap in recipient.capabilities:
            await self._mesh.emit(cap, payload)

    async def a_receive(
        self,
        message: dict[str, Any],
        sender: MeshAgent,
        request_reply: bool | None = None,
    ) -> None:
        """Получить сообщение (вызывается подпиской mesh)."""
        content = message.get("content", message)
        for cb in self._callbacks.values():
            cb(content, sender.agent_id)

    def register_reply(self, callback: Callable) -> str:
        """Зарегистрировать callback на входящие сообщения."""
        cb_id = f"cb_{int(time.time() * 1000)}"
        self._callbacks[cb_id] = callback
        return cb_id

    # ───────────────────────── Internal ─────────────────────────

    def _on_mesh_event(self, event):
        """Обработчик входящих mesh-событий."""
        import asyncio

        # event — это dict из mesh (с обёрткой emit)
        if isinstance(event, dict):
            payload = event.get("payload", event)
        else:
            payload = getattr(event, "payload", event)

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                pass

        if isinstance(payload, dict) and payload.get("type") == "autogen_message":
            sender_name = payload.get("from", "unknown")
            content = payload.get("content", payload)
            print(f"  [_on_mesh_event] from={sender_name} content={content}")
            for cb in self._callbacks.values():
                try:
                    if asyncio.iscoroutinefunction(cb):
                        asyncio.create_task(cb(content, sender_name))
                    else:
                        cb(content, sender_name)
                except Exception as e:
                    print(f"  [_on_mesh_event] callback error: {e}")
