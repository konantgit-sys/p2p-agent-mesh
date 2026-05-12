# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""Consumer Groups — ordered delivery группе потребителей.

Алгоритм:
  1. Группа = set потребителей (consumer_id)
  2. Каждое сообщение = seq + topic + payload
  3. Consumer group гарантирует ordered delivery всем участникам
  4. Offset = последний processed seq (для restart)
  5. Rebalance — при добавлении/удалении consumer'а

Использует Raft для координации offset'ов.
"""

from __future__ import annotations

from collections.abc import Callable


class ConsumerGroup:
    """Группа потребителей с ordered delivery.

    Использование:
        group = ConsumerGroup(
            group_id="signal_consumers",
            consumer_id="forecaster_v2",
        )

        # Подписка на топик
        group.subscribe("crypto_signals", callback)

        # Commit offset
        group.commit("crypto_signals", 42)

        # Получить offset для рестарта
        offset = group.get_offset("crypto_signals")
    """

    def __init__(
        self,
        group_id: str,
        consumer_id: str,
        offset_store: dict | None = None,
    ):
        self.group_id = group_id
        self.consumer_id = consumer_id
        self._topics: dict[str, list[Callable]] = {}
        self._offsets: dict[str, int] = offset_store or {}
        self._pending: dict[str, list[dict]] = {}  # буфер упорядочивания

    def subscribe(self, topic: str, callback: Callable):
        """Подписаться на топик в группе."""
        if topic not in self._topics:
            self._topics[topic] = []
        self._topics[topic].append(callback)
        # Инициализируем буфер
        if topic not in self._pending:
            self._pending[topic] = []

    def on_message(self, topic: str, seq: int, payload: dict):
        """Обработать входящее сообщение (с seq для ordering).

        Если seq совпадает с ожидаемым (last_offset + 1) — вызываем callback.
        Если seq ahead — буферизируем до прихода пропущенных.
        """
        expected = self._offsets.get(topic, 0) + 1

        if seq == expected:
            self._deliver(topic, payload)
            self._offsets[topic] = seq
            # Проверяем буфер
            self._flush_buffer(topic)
        elif seq > expected:
            # Сохраняем в буфер (упорядоченная вставка)
            self._pending[topic].append({"seq": seq, "payload": payload})
            self._pending[topic].sort(key=lambda x: x["seq"])

    def _flush_buffer(self, topic: str):
        """Опустошить буфер, доставить накопившиеся сообщения по порядку."""
        expected = self._offsets.get(topic, 0) + 1
        while self._pending[topic] and self._pending[topic][0]["seq"] == expected:
            msg = self._pending[topic].pop(0)
            self._deliver(topic, msg["payload"])
            self._offsets[topic] = expected
            expected += 1

    def _deliver(self, topic: str, payload: dict):
        """Доставить сообщение всем подписчикам топика."""
        for cb in self._topics.get(topic, []):
            try:
                cb(payload)
            except Exception as e:
                print(f"[ConsumerGroup:{self.group_id}/{self.consumer_id}] callback error: {e}")

    def commit(self, topic: str, seq: int):
        """Зафиксировать offset (для restart).

        Вызывается после успешной обработки сообщения.
        """
        if seq > self._offsets.get(topic, 0):
            self._offsets[topic] = seq

    def get_offset(self, topic: str) -> int:
        """Текущий offset (начать с него после restart)."""
        return self._offsets.get(topic, 0)

    def pending_count(self, topic: str) -> int:
        """Количество буферизированных сообщений."""
        return len(self._pending.get(topic, []))

    def get_state(self) -> dict:
        """Состояние группы (для сохранения)."""
        return {
            "group_id": self.group_id,
            "consumer_id": self.consumer_id,
            "offsets": dict(self._offsets),
            "topics": list(self._topics.keys()),
            "pending_total": sum(len(v) for v in self._pending.values()),
        }

    @classmethod
    def load_state(cls, state: dict) -> ConsumerGroup:
        """Восстановить группу из сохранённого состояния."""
        return cls(
            group_id=state["group_id"],
            consumer_id=state["consumer_id"],
            offset_store=state.get("offsets", {}),
        )


class GroupCoordinator:
    """Координатор групп потребителей.

    Управляет членством в группах, ребалансировкой,
    хранением offset'ов (через Raft или локально).
    """

    def __init__(self):
        self._groups: dict[str, dict[str, ConsumerGroup]] = {}
        # group_id → set of consumer_ids
        self._membership: dict[str, set[str]] = {}

    def join_group(self, group_id: str, consumer_id: str) -> ConsumerGroup:
        """Присоединиться к группе (или создать новую)."""
        if group_id not in self._membership:
            self._membership[group_id] = set()
        self._membership[group_id].add(consumer_id)

        if group_id not in self._groups:
            self._groups[group_id] = {}

        if consumer_id not in self._groups[group_id]:
            self._groups[group_id][consumer_id] = ConsumerGroup(
                group_id=group_id,
                consumer_id=consumer_id,
            )

        print(
            f"[Coordinator] {consumer_id} joined group '{group_id}' "
            f"({len(self._membership[group_id])} members)"
        )
        return self._groups[group_id][consumer_id]

    def leave_group(self, group_id: str, consumer_id: str) -> bool:
        """Покинуть группу."""
        if group_id in self._membership:
            self._membership[group_id].discard(consumer_id)
            if not self._membership[group_id]:
                del self._membership[group_id]
        if group_id in self._groups and consumer_id in self._groups[group_id]:
            del self._groups[group_id][consumer_id]
            print(f"[Coordinator] {consumer_id} left group '{group_id}'")
            return True
        return False

    def get_group(self, group_id: str) -> dict[str, ConsumerGroup] | None:
        """Получить всех consumer'ов группы."""
        return self._groups.get(group_id)

    def get_consumer(self, group_id: str, consumer_id: str) -> ConsumerGroup | None:
        """Получить consumer в группе."""
        return self._groups.get(group_id, {}).get(consumer_id)

    def broadcast(self, group_id: str, topic: str, seq: int, payload: dict):
        """Разослать сообщение всем участникам группы."""
        consumers = self._groups.get(group_id, {})
        for consumer in consumers.values():
            consumer.on_message(topic, seq, payload)

    def member_count(self, group_id: str) -> int:
        """Количество участников группы."""
        return len(self._membership.get(group_id, set()))

    def list_groups(self) -> list[dict]:
        """Список всех групп."""
        return [
            {
                "group_id": gid,
                "members": list(members),
                "consumer_count": len(self._groups.get(gid, {})),
            }
            for gid, members in self._membership.items()
        ]

    def get_state(self) -> dict:
        """Полное состояние координатора."""
        return {
            "groups": [
                {
                    "group_id": gid,
                    "members": list(members),
                    "consumers": {
                        cid: cg.get_state() for cid, cg in self._groups.get(gid, {}).items()
                    },
                }
                for gid, members in self._membership.items()
            ]
        }
