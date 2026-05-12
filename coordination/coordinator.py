# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Coordination Layer — главный координатор.



Объединяет:
  - Микро-Raft (consensus, ordered log)
  - Consumer Groups (ordered delivery)
  - Exactly-once dedup (idempotent processing)

Использование:
    coord = Coordinator(node_id="node_1", peers=["node_2", "node_3"])
    await coord.start()

    # Ordered broadcast
    await coord.ordered_broadcast("signals", {"price": 85000, "asset": "BTC"})

    # Consumer group
    group = coord.create_group("analysts", "forecaster_v2")
    group.subscribe("signals", callback)

    await coord.stop()
"""

from __future__ import annotations

import asyncio
import tempfile
import time

from coordination.consumer_group import ConsumerGroup, GroupCoordinator
from coordination.dedup import DedupLog
from coordination.raft import MicroRaft, RaftState


class Coordinator:
    """Главный координатор ordered-сценариев.

    Интегрирует Raft + ConsumerGroups + Dedup в единый API.
    """

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        dedup_path: str | None = None,
        raft_election_timeout: tuple[float, float] = (0.5, 1.5),
        raft_heartbeat: float = 0.3,
    ):
        self.node_id = node_id
        self.peers = peers

        # Dedup log
        self.dedup = DedupLog(
            dedup_path or tempfile.mktemp(suffix=f"_dedup_{node_id}.db"),
        )

        # Consumer groups
        self.group_coordinator = GroupCoordinator()

        # Raft
        self.raft = MicroRaft(
            node_id=node_id,
            peers=peers,
            election_timeout_min=raft_election_timeout[0],
            election_timeout_max=raft_election_timeout[1],
            heartbeat_interval=raft_heartbeat,
        )

        # Sequence counter для ordered_broadcast
        self._seq_counter = 0
        self._topic_seqs: dict[str, int] = {}

        # Committed commands → consumer groups
        self.raft.on_commit(self._on_commit)

    async def start(self):
        """Запустить координатор."""
        self.dedup.open()
        await self.raft.start()
        print(f"[Coordinator:{self.node_id}] Started. Peers: {self.peers}")

    async def stop(self):
        """Остановить координатор."""
        await self.raft.stop()
        self.dedup.close()
        print(f"[Coordinator:{self.node_id}] Stopped.")

    # ─────────────────────── Ordered Broadcast ───────────────────────

    async def ordered_broadcast(self, topic: str, payload: dict) -> int | None:
        """Ordered broadcast через Raft.

        Публикует команду в Raft-лог.
        После commit — доставляет consumer groups.

        Args:
            topic: топик
            payload: данные

        Returns:
            seq номер, или None если не leader
        """
        if self.raft.state != RaftState.LEADER:
            return None

        # Инкремент seq для топика
        self._topic_seqs[topic] = self._topic_seqs.get(topic, 0) + 1
        seq = self._topic_seqs[topic]

        command = {
            "type": "ordered_message",
            "topic": topic,
            "seq": seq,
            "payload": payload,
            "ts": time.time(),
        }

        # Публикуем в Raft-лог
        index = await self.raft.propose(command)
        print(
            f"[Coordinator:{self.node_id}] Ordered broadcast: "
            f"topic={topic} seq={seq} raft_index={index}"
        )
        return seq

    def _on_commit(self, command: dict):
        """Callback при коммите Raft-записи.

        Доставляет сообщение всем consumer groups на соответствующий топик.
        """
        if command.get("type") != "ordered_message":
            return

        topic = command["topic"]
        seq = command["seq"]
        payload = command["payload"]

        # Проверка dedup
        msg_id = f"{topic}:{seq}"
        if self.dedup.is_processed(msg_id):
            print(f"[Coordinator] Skipping duplicate: {msg_id}")
            return

        # Доставка в consumer groups
        self.group_coordinator.broadcast(
            f"cg:{topic}",
            topic,
            seq,
            payload,
        )

        self.dedup.mark_processed(msg_id)

    # ─────────────────────── Consumer Groups API ───────────────────────

    def create_group(self, group_id: str, consumer_id: str) -> ConsumerGroup:
        """Создать/присоединиться к consumer group."""
        return self.group_coordinator.join_group(group_id, consumer_id)

    def leave_group(self, group_id: str, consumer_id: str):
        """Покинуть consumer group."""
        self.group_coordinator.leave_group(group_id, consumer_id)

    # ─────────────────────── Raft API ───────────────────────

    def propose(self, command: dict) -> None:
        """Предложить команду в Raft (если leader)."""
        asyncio.create_task(self.raft.propose(command))

    @property
    def is_leader(self) -> bool:
        return self.raft.state == RaftState.LEADER

    @property
    def leader_id(self) -> str | None:
        if self.is_leader:
            return self.node_id
        return None

    # ─────────────────────── Status ───────────────────────

    def get_status(self) -> dict:
        """Полный статус координатора."""
        return {
            "node_id": self.node_id,
            "peers": self.peers,
            "raft": self.raft.get_state(),
            "dedup": self.dedup.get_stats(),
            "groups": self.group_coordinator.list_groups(),
            "topic_seqs": dict(self._topic_seqs),
        }
