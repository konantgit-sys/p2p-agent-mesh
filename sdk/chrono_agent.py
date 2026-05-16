"""
CRYTER V10.5 — ChronoAgentMesh
AgentMesh с интеграцией ChronoDB (временные ряды событий).

Надстройка над AgentMesh: каждое emit/listen событие сохраняется в ChronoDB.
"""

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from core.chrono_db import ChronoDB
from sdk.agent import AgentMesh, Subscription

logger = logging.getLogger("chrono_agent")


class ChronoAgentMesh(AgentMesh):
    """AgentMesh с хронологическим логированием событий в ChronoDB."""

    def __init__(self, chrono_db: Optional[ChronoDB] = None, **kwargs):
        super().__init__(**kwargs)
        self.chrono = chrono_db or ChronoDB()
        logger.info(f"ChronoAgentMesh: init, db={self.chrono.db_path}")

    # ── Emit с хранением ──

    async def emit(self, capability: str, payload: dict) -> str:
        """Опубликовать событие + сохранить в ChronoDB."""
        msg_id = await super().emit(capability, payload)

        try:
            self.chrono.save_event(
                agent_id=self.agent_id,
                msg_id=msg_id,
                capability=capability,
                event_type="emit",
                payload={
                    "from": self.identity.did[:16],
                    "payload_summary": str(payload)[:100],
                }
            )
        except Exception as e:
            logger.debug(f"Chrono: save emit error: {e}")

        # Метрика — размер сообщения
        try:
            payload_size = len(json.dumps(payload))
            self.chrono.save_metric(self.agent_id, "msg_size", float(payload_size))
        except Exception:
            pass

        return msg_id

    # ── Route с хранением ──

    async def _route_message(self, topic: str, data: bytes):
        """Обработать входящее сообщение + сохранить в ChronoDB."""
        # Перехватываем перед маршрутизацией подписчикам
        try:
            signed = json.loads(data.decode())
            payload = signed.get("payload", {})
            from_agent = signed.get("agent_id", signed.get("from", "unknown"))
            capability = signed.get("capability", topic.replace("agent:", ""))

            self.chrono.save_event(
                agent_id=from_agent[:24],
                msg_id=signed.get("msg_id", "") or signed.get("id", ""),
                capability=capability,
                event_type="receive",
                payload={
                    "from": from_agent[:24],
                    "topic": topic,
                    "payload_summary": str(payload)[:100],
                }
            )
        except Exception as e:
            logger.debug(f"Chrono: route save error: {e}")

        # Прокидываем в родительский обработчик
        await super()._route_message(topic, data)

    # ── Listen с логированием ──

    async def listen(self, filter_dict: dict, callback: Callable) -> Subscription:
        """Подписаться + залогировать подписку."""
        sub = await super().listen(filter_dict, callback)

        try:
            caps = filter_dict.get("capabilities") or filter_dict.get("capability", "unknown")
            self.chrono.save_metric(
                self.agent_id, "subscribe", 1.0,
                payload={"filter": str(filter_dict)[:200]}
            )
        except Exception:
            pass

        logger.info(f"Chrono: subscribed {self.agent_id} → {filter_dict}")
        return sub

    # ── Дополнительно: сохранение состояния ──

    def save_state(self, state: Dict, npub: str = ""):
        """Сохранить текущее состояние агента в ChronoDB."""
        self.chrono.save_agent_state(self.agent_id, state, npub)

    def load_state(self) -> Optional[Dict]:
        """Загрузить последнее состояние агента из ChronoDB."""
        return self.chrono.get_agent_state(self.agent_id)

    def get_history(self, capability: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """История событий этого агента."""
        return self.chrono.get_events(
            agent_id=self.agent_id,
            capability=capability,
            limit=limit,
        )

    # ── Stats ──

    def get_chrono_stats(self) -> Dict:
        """Статистика ChronoDB."""
        return self.chrono.get_stats()

    # ── Cleanup ──

    async def stop(self):
        await super().stop()
        self.chrono.close()
        logger.info("ChronoAgentMesh: stopped")
