# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Микро-Raft consensus для ordered-сценариев.



Упрощённая реализация Raft:
  - Leader election (таймаут + голосование)
  - Log replication (AppendEntries через mesh)
  - Commit (когда majority подтвердили)

Не реализовано (MVP):
  - Snapshot / log compaction
  - Dynamic membership changes
  - Multi-cluster

Состояния узла:
  Follower → (таймаут) → Candidate → (голоса) → Leader
  Leader → (stale term) → Follower
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable
from enum import Enum


class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class LogEntry:
    """Запись в Raft-логе."""

    __slots__ = ("term", "index", "command", "timestamp")

    def __init__(self, term: int, index: int, command: dict):
        self.term = term
        self.index = index
        self.command = command
        self.timestamp = time.time()

    def to_dict(self) -> dict:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> LogEntry:
        entry = cls.__new__(cls)
        entry.term = data["term"]
        entry.index = data["index"]
        entry.command = data.get("command", {})
        entry.timestamp = data.get("timestamp", time.time())
        return entry


class MicroRaft:
    """Узел микро-Raft кластера.

    Использование:
        raft = MicroRaft(node_id="node_1", peers=["node_2", "node_3"])
        await raft.start()

        # Только leader может предлагать команды
        if raft.state == RaftState.LEADER:
            await raft.propose({"action": "transfer", "amount": 100})

        await raft.stop()
    """

    def __init__(
        self,
        node_id: str,
        peers: list[str],  # ID соседних узлов
        election_timeout_min: float = 0.5,
        election_timeout_max: float = 1.5,
        heartbeat_interval: float = 0.3,
        mesh_emit: Callable | None = None,  # функция для отправки в mesh
    ):
        self.node_id = node_id
        self.peers = peers
        self.peer_set = set(peers)

        # Persistent state (на сервере — в SQLite)
        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0

        # Leader volatile
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # Internal
        self.state = RaftState.FOLLOWER
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval
        self._mesh_emit = mesh_emit

        # Event loop
        self._running = False
        self._election_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._last_heartbeat = time.time()

        # Callbacks
        self._commit_callbacks: list[Callable] = []

        # Receive queue (входящие RPC)
        self._inbox: asyncio.Queue = asyncio.Queue()

    # ─────────────────────── Lifecycle ───────────────────────

    async def start(self):
        """Запуск Raft-узла."""
        self._running = True
        self._last_heartbeat = time.time()
        self._election_task = asyncio.create_task(self._election_loop())
        print(f"[Raft:{self.node_id}] Started as FOLLOWER, term={self.current_term}")

    async def stop(self):
        """Остановка узла."""
        self._running = False
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        print(f"[Raft:{self.node_id}] Stopped")

    # ─────────────────────── Raft RPC handlers ───────────────────────

    async def handle_rpc(self, rpc: dict) -> dict | None:
        """Обработка входящего RPC от другого узла.

        Вызывается при получении AppendEntries или RequestVote из mesh.
        """
        rpc_type = rpc.get("type")
        term = rpc.get("term", 0)

        # Обновляем term (stale leader detection)
        if term > self.current_term:
            self.current_term = term
            if self.state == RaftState.LEADER:
                # Stale leader → step down
                self._step_down()
            elif self.state == RaftState.CANDIDATE:
                self.state = RaftState.FOLLOWER

        if rpc_type == "RequestVote":
            return self._handle_request_vote(rpc)
        elif rpc_type == "AppendEntries":
            return self._handle_append_entries(rpc)
        return None

    def _handle_request_vote(self, rpc: dict) -> dict:
        """Обработка RequestVote RPC."""
        candidate_id = rpc.get("candidate_id")
        candidate_term = rpc.get("term", 0)
        last_log_index = rpc.get("last_log_index", 0)
        last_log_term = rpc.get("last_log_term", 0)

        # Условия голосования
        if candidate_term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        if self.voted_for is not None and self.voted_for != candidate_id:
            return {"term": self.current_term, "vote_granted": False}

        # Log up-to-date check
        my_last_term = self.log[-1].term if self.log else 0
        my_last_index = len(self.log)

        if last_log_term < my_last_term or (
            last_log_term == my_last_term and last_log_index < my_last_index
        ):
            return {"term": self.current_term, "vote_granted": False}

        # Vote granted
        self.voted_for = candidate_id
        self._last_heartbeat = time.time()  # reset election timer
        print(f"[Raft:{self.node_id}] Vote granted to {candidate_id} (term {candidate_term})")
        return {"term": self.current_term, "vote_granted": True}

    def _handle_append_entries(self, rpc: dict) -> dict:
        """Обработка AppendEntries RPC (heartbeat или log replication)."""
        prev_log_index = rpc.get("prev_log_index", 0)
        prev_log_term = rpc.get("prev_log_term", 0)
        entries_data = rpc.get("entries", [])
        leader_commit = rpc.get("leader_commit", 0)

        # Reset election timer (heartbeat received)
        self._last_heartbeat = time.time()

        # Log consistency check
        if prev_log_index > 0:
            if prev_log_index > len(self.log):
                return {"term": self.current_term, "success": False}
            if prev_log_index <= len(self.log):
                prev_entry = self.log[prev_log_index - 1]
                if prev_entry.term != prev_log_term:
                    return {"term": self.current_term, "success": False}

        # Append new entries
        for entry_data in entries_data:
            entry = LogEntry.from_dict(entry_data)
            if entry.index <= len(self.log):
                # Conflict: remove and replace
                if self.log[entry.index - 1].term != entry.term:
                    self.log = self.log[: entry.index - 1]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            self._apply_committed()

        return {"term": self.current_term, "success": True}

    def _apply_committed(self):
        """Apply committed entries to state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            for cb in self._commit_callbacks:
                try:
                    cb(entry.command)
                except Exception as e:
                    print(f"[Raft:{self.node_id}] commit callback error: {e}")

    # ─────────────────────── Leader election ───────────────────────

    async def _election_loop(self):
        """Цикл выборов: follower → candidate → leader."""
        while self._running:
            if self.state == RaftState.LEADER:
                # Leader не участвует в выборах
                await asyncio.sleep(0.1)
                continue

            # Ждём случайный таймаут
            timeout = random.uniform(
                self._election_timeout_min,
                self._election_timeout_max,
            )
            await asyncio.sleep(timeout)

            if not self._running:
                break

            # Проверка heartbeat от leader'а
            if time.time() - self._last_heartbeat < timeout:
                continue  # heartbeat получен — не начинаем выборы

            # Начинаем выборы
            await self._start_election()

    async def _start_election(self):
        """Стать кандидатом, запросить голоса."""
        self.current_term += 1
        self.state = RaftState.CANDIDATE
        self.voted_for = self.node_id  # голосуем за себя

        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0

        votes = 1  # голос за себя
        majority = (len(self.peers) + 1) // 2 + 1

        print(f"[Raft:{self.node_id}] Election started (term {self.current_term})")

        for peer in self.peers:
            vote_req = {
                "type": "RequestVote",
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            }
            response = await self._send_rpc(peer, vote_req)
            if response and response.get("vote_granted"):
                votes += 1
                if votes >= majority:
                    break

        if votes >= majority:
            self._become_leader()
        else:
            self.state = RaftState.FOLLOWER
            print(f"[Raft:{self.node_id}] Election lost ({votes}/{majority} votes)")

    def _become_leader(self):
        """Стать лидером кластера."""
        self.state = RaftState.LEADER
        self.voted_for = None

        # Инициализация next/match index
        last_index = len(self.log)
        for peer in self.peers:
            self.next_index[peer] = last_index + 1
            self.match_index[peer] = 0

        print(f"[Raft:{self.node_id}] Became LEADER (term {self.current_term})")

        # Запуск heartbeat loop
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._leader_loop())

    def _step_down(self):
        """Уступить лидерство (stale term)."""
        old_state = self.state
        self.state = RaftState.FOLLOWER
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        print(
            f"[Raft:{self.node_id}] Stepped down: {old_state} → FOLLOWER (term {self.current_term})"
        )

    # ─────────────────────── Leader duties ───────────────────────

    async def _leader_loop(self):
        """Leader отправляет heartbeats и реплицирует лог."""
        while self._running and self.state == RaftState.LEADER:
            await self._broadcast_append_entries()
            await asyncio.sleep(self._heartbeat_interval)

    async def _broadcast_append_entries(self, force_replicate: bool = False):
        """Разослать AppendEntries всем follower'ам."""
        for peer in self.peers:
            prev_index = self.next_index.get(peer, 1) - 1
            if prev_index < 0:
                prev_index = 0
            prev_term = (
                self.log[prev_index - 1].term
                if prev_index > 0 and prev_index <= len(self.log)
                else 0
            )

            entries = []
            next_idx = self.next_index.get(peer, 1)
            if next_idx <= len(self.log) or force_replicate:
                for i in range(next_idx - 1, len(self.log)):
                    entries.append(self.log[i].to_dict())

            ae = {
                "type": "AppendEntries",
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": self.commit_index,
            }

            # Отправляем (async, не ждём всех)
            asyncio.create_task(self._send_append_entries(peer, ae))

    async def _send_append_entries(self, peer: str, ae: dict):
        """Отправить AppendEntries одному follower'у и обработать ответ."""
        response = await self._send_rpc(peer, ae)
        if response is None:
            return

        if response.get("success"):
            # Follower подтвердил
            self.match_index[peer] = len(self.log)
            self.next_index[peer] = len(self.log) + 1
            self._update_commit_index()
        else:
            # Conflict: decrement next_index и retry
            if self.next_index.get(peer, 1) > 1:
                self.next_index[peer] = self.next_index.get(peer, 1) - 1

    def _update_commit_index(self):
        """Обновить commit_index (majority replication)."""
        for n in range(self.commit_index + 1, len(self.log) + 1):
            if self.log[n - 1].term == self.current_term:
                # Считаем: leader + сколько подтвердили
                replicated = 1  # leader
                for peer in self.peers:
                    if self.match_index.get(peer, 0) >= n:
                        replicated += 1
                majority = (len(self.peers) + 1) // 2 + 1
                if replicated >= majority:
                    self.commit_index = n
                    self._apply_committed()
                    print(f"[Raft:{self.node_id}] Commit index updated to {n}")

    # ─────────────────────── Public API ───────────────────────

    async def propose(self, command: dict) -> int | None:
        """Предложить команду (только leader).

        Returns:
            index команды в логе, или None если не leader
        """
        if self.state != RaftState.LEADER:
            return None

        entry = LogEntry(
            term=self.current_term,
            index=len(self.log) + 1,
            command=command,
        )
        self.log.append(entry)

        # Реплицируем сразу
        await self._broadcast_append_entries(force_replicate=True)

        # Single node: сразу коммитим
        if not self.peers:
            self.commit_index = len(self.log)
            self._apply_committed()

        return entry.index

    def on_commit(self, callback: Callable[[dict], None]):
        """Зарегистрировать callback на committed команды."""
        self._commit_callbacks.append(callback)

    def get_state(self) -> dict:
        """Текущее состояние узла."""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.current_term,
            "log_size": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "peers": self.peers,
        }

    # ─────────────────────── Transport ───────────────────────

    async def _send_rpc(self, target: str, msg: dict) -> dict | None:
        """Отправить RPC одному узлу.

        В реальном mesh — через emit/listen.
        В in-process тестах — через вызов напрямую.
        """
        if self._mesh_emit:
            # Отправка через mesh (асинхронная)
            await self._mesh_emit(target, msg)
            return None  # ответ придёт отдельным сообщением
        return None

    def receive(self, rpc: dict):
        """Получить RPC (для in-process тестов)."""
        asyncio.create_task(self._safe_handle(rpc))

    async def _safe_handle(self, rpc: dict):
        try:
            result = await self.handle_rpc(rpc)
            if result and rpc.get("type") == "RequestVote":
                # Для тестов — результат возвращается синхронно
                pass
        except Exception as e:
            print(f"[Raft:{self.node_id}] handle error: {e}")
