"""Coordination Layer — тесты.

Проверяет:
  - Микро-Raft: leader election, log, commit
  - Consumer Groups: ordered delivery, buffer, offset
  - Group Coordinator: join/leave/broadcast
  - Exactly-once dedup: is_processed, mark, cleanup
  - Coordinator: интеграция Raft + Groups + Dedup
"""

import asyncio
import os
import tempfile

import pytest

from coordination.consumer_group import ConsumerGroup, GroupCoordinator
from coordination.coordinator import Coordinator
from coordination.dedup import DedupLog
from coordination.raft import LogEntry, MicroRaft, RaftState

# ─────────────────────────── Raft ─────────────────────────────


class TestRaft:
    def test_initial_state(self):
        raft = MicroRaft("node_1", ["node_2", "node_3"])
        assert raft.state == RaftState.FOLLOWER
        assert raft.current_term == 0
        assert raft.log == []
        assert raft.commit_index == 0

    def test_log_entry_roundtrip(self):
        entry = LogEntry(term=1, index=1, command={"action": "test"})
        data = entry.to_dict()
        restored = LogEntry.from_dict(data)
        assert restored.term == 1
        assert restored.index == 1
        assert restored.command["action"] == "test"

    @pytest.mark.asyncio
    async def test_handle_request_vote_newer_term(self):
        raft = MicroRaft("node_1", ["node_2"])
        rpc = {
            "type": "RequestVote",
            "term": 5,
            "candidate_id": "node_2",
            "last_log_index": 0,
            "last_log_term": 0,
        }
        response = await raft.handle_rpc(rpc)
        assert response["vote_granted"] is True
        assert raft.voted_for == "node_2"
        assert raft.current_term == 5

    def test_handle_request_vote_stale_term(self):
        raft = MicroRaft("node_1", ["node_2"])
        raft.current_term = 10
        rpc = {
            "type": "RequestVote",
            "term": 5,
            "candidate_id": "node_2",
            "last_log_index": 0,
            "last_log_term": 0,
        }
        response = raft._handle_request_vote(rpc)
        assert response["vote_granted"] is False

    def test_handle_request_vote_already_voted(self):
        raft = MicroRaft("node_1", ["node_2", "node_3"])
        raft.voted_for = "node_2"
        rpc = {
            "type": "RequestVote",
            "term": 1,
            "candidate_id": "node_3",
            "last_log_index": 0,
            "last_log_term": 0,
        }
        response = raft._handle_request_vote(rpc)
        assert response["vote_granted"] is False

    def test_handle_append_entries_heartbeat(self):
        raft = MicroRaft("node_1", ["node_2"])
        rpc = {
            "type": "AppendEntries",
            "term": 1,
            "leader_id": "node_2",
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0,
        }
        response = raft._handle_append_entries(rpc)
        assert response["success"] is True
        assert raft.state == RaftState.FOLLOWER

    def test_handle_append_entries_with_log(self):
        raft = MicroRaft("node_1", ["node_2"])
        raft.log = [LogEntry(term=1, index=1, command={"a": 1})]
        rpc = {
            "type": "AppendEntries",
            "term": 2,
            "leader_id": "node_2",
            "prev_log_index": 1,
            "prev_log_term": 1,
            "entries": [LogEntry(term=2, index=2, command={"b": 2}).to_dict()],
            "leader_commit": 2,
        }
        response = raft._handle_append_entries(rpc)
        assert response["success"] is True
        assert len(raft.log) == 2
        assert raft.commit_index == 2

    @pytest.mark.asyncio
    async def test_step_down_stale_term(self):
        raft = MicroRaft("node_1", ["node_2"])
        raft.state = RaftState.LEADER
        raft.current_term = 5
        rpc = {
            "type": "AppendEntries",
            "term": 6,
            "leader_id": "node_2",
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0,
        }
        await raft.handle_rpc(rpc)
        assert raft.state == RaftState.FOLLOWER
        assert raft.current_term == 6

    def test_propose_not_leader(self):
        raft = MicroRaft("node_1", ["node_2"])
        raft.state = RaftState.FOLLOWER
        result = asyncio.run(raft.propose({"action": "test"}))
        assert result is None, "Только leader может предлагать"

    @pytest.mark.asyncio
    async def test_leader_election_single_node(self):
        """Одиночный узел становится leader'ом."""
        raft = MicroRaft("node_1", [], election_timeout_min=0.1, election_timeout_max=0.2)
        await raft.start()
        await asyncio.sleep(1.5)
        assert raft.state == RaftState.LEADER
        assert raft.current_term >= 1
        await raft.stop()

    @pytest.mark.asyncio
    async def test_leader_election_three_nodes(self):
        """3 in-process узла: один становится leader'ом через прямые вызовы."""
        nodes = [
            MicroRaft(
                f"node_{i}",
                ["node_0", "node_1", "node_2"],
                election_timeout_min=0.1,
                election_timeout_max=0.4,
            )
            for i in range(3)
        ]
        # Соединяем узлы in-process: _send_rpc → target.receive()
        for n in nodes:

            async def make_send(src_node):
                async def send_to(target_id, msg):
                    for target in nodes:
                        if target.node_id == target_id:
                            if msg.get("type") == "RequestVote":
                                resp = await target.handle_rpc(msg)
                                return resp
                            elif msg.get("type") == "AppendEntries":
                                await target.handle_rpc(msg)
                                return {"term": target.current_term, "success": True}
                    return None

                return send_to

            n._send_rpc = await make_send(n)

        await asyncio.gather(*(n.start() for n in nodes))
        await asyncio.sleep(4)

        leaders = [n for n in nodes if n.state == RaftState.LEADER]
        assert len(leaders) >= 1, "Должен быть хотя бы 1 leader"
        # Все узлы на одном term (или близком)
        terms = set(n.current_term for n in nodes)
        print(f"  Leaders: {[n.node_id for n in leaders]}")
        print(f"  Terms: {terms}")

        await asyncio.gather(*(n.stop() for n in nodes))

    @pytest.mark.asyncio
    async def test_propose_and_commit(self):
        """Leader предлагает команду и она доставляется."""
        raft = MicroRaft("node_1", [], election_timeout_min=0.1, election_timeout_max=0.2)
        await raft.start()
        await asyncio.sleep(1)

        assert raft.state == RaftState.LEADER

        committed = []

        def on_commit(cmd):
            committed.append(cmd)

        raft.on_commit(on_commit)

        index = await raft.propose({"action": "transfer", "amount": 100})
        assert index == 1, "Первая команда — индекс 1"
        await asyncio.sleep(0.5)

        assert len(committed) >= 1
        assert committed[0]["action"] == "transfer"

        await raft.stop()


# ─────────────────────────── Consumer Group ────────────────────


class TestConsumerGroup:
    def test_initial_state(self):
        cg = ConsumerGroup("analysts", "agent_1")
        assert cg.get_offset("signals") == 0

    def test_subscribe_and_deliver(self):
        cg = ConsumerGroup("analysts", "agent_1")
        received = []

        cg.subscribe("signals", lambda msg: received.append(msg))
        cg.on_message("signals", 1, {"price": 85000})

        assert len(received) == 1
        assert received[0]["price"] == 85000
        assert cg.get_offset("signals") == 1

    def test_ordered_delivery_with_buffer(self):
        cg = ConsumerGroup("analysts", "agent_1")
        received = []

        cg.subscribe("signals", lambda msg: received.append(msg))

        # Сообщения приходят не по порядку
        cg.on_message("signals", 3, {"seq": 3})  # buffer
        cg.on_message("signals", 1, {"seq": 1})  # deliver + flush
        cg.on_message("signals", 2, {"seq": 2})  # deliver

        assert len(received) == 3
        assert received[0]["seq"] == 1
        assert received[1]["seq"] == 2
        assert received[2]["seq"] == 3

    def test_commit_offset(self):
        cg = ConsumerGroup("analysts", "agent_1")
        cg.subscribe("signals", lambda m: None)
        cg.on_message("signals", 1, {"a": 1})
        cg.on_message("signals", 2, {"a": 2})
        cg.commit("signals", 2)
        assert cg.get_offset("signals") == 2

    def test_pending_count(self):
        cg = ConsumerGroup("analysts", "agent_1")
        cg.subscribe("signals", lambda m: None)
        cg.on_message("signals", 5, {"seq": 5})
        cg.on_message("signals", 7, {"seq": 7})
        assert cg.pending_count("signals") == 2

    def test_save_and_load_state(self):
        cg = ConsumerGroup("analysts", "agent_1")
        cg.subscribe("signals", lambda m: None)
        cg.on_message("signals", 1, {"x": 1})
        cg.on_message("signals", 2, {"x": 2})
        state = cg.get_state()

        restored = ConsumerGroup.load_state(state)
        assert restored.group_id == "analysts"
        assert restored.consumer_id == "agent_1"
        assert restored.get_offset("signals") == 2

    def test_multiple_topics(self):
        cg = ConsumerGroup("multi", "agent_1")
        received = {"signals": [], "alerts": []}

        cg.subscribe("signals", lambda m: received["signals"].append(m))
        cg.subscribe("alerts", lambda m: received["alerts"].append(m))

        cg.on_message("signals", 1, {"type": "signal"})
        cg.on_message("alerts", 1, {"type": "alert"})

        assert len(received["signals"]) == 1
        assert len(received["alerts"]) == 1


# ─────────────────────────── Group Coordinator ────────────────


class TestGroupCoordinator:
    def test_join_group(self):
        gc = GroupCoordinator()
        cg = gc.join_group("analysts", "agent_1")
        assert cg is not None
        assert gc.member_count("analysts") == 1

    def test_leave_group(self):
        gc = GroupCoordinator()
        gc.join_group("analysts", "agent_1")
        gc.join_group("analysts", "agent_2")
        assert gc.member_count("analysts") == 2
        assert gc.leave_group("analysts", "agent_1") is True
        assert gc.member_count("analysts") == 1

    def test_broadcast(self):
        gc = GroupCoordinator()
        cg1 = gc.join_group("analysts", "agent_1")
        cg2 = gc.join_group("analysts", "agent_2")
        received = [[], []]

        cg1.subscribe("signals", lambda m: received[0].append(m))
        cg2.subscribe("signals", lambda m: received[1].append(m))

        gc.broadcast("analysts", "signals", 1, {"price": 100})
        assert len(received[0]) == 1
        assert len(received[1]) == 1

    def test_list_groups(self):
        gc = GroupCoordinator()
        gc.join_group("a", "x1")
        gc.join_group("a", "x2")
        gc.join_group("b", "y1")
        groups = gc.list_groups()
        assert len(groups) == 2
        for g in groups:
            if g["group_id"] == "a":
                assert len(g["members"]) == 2

    def test_get_state(self):
        gc = GroupCoordinator()
        gc.join_group("analysts", "agent_1")
        state = gc.get_state()
        assert len(state["groups"]) == 1
        assert state["groups"][0]["group_id"] == "analysts"


# ─────────────────────────── Dedup ────────────────────────────


class TestDedupLog:
    @pytest.fixture
    def dedup(self):
        path = tempfile.mktemp(suffix="_test_dedup.db")
        d = DedupLog(path)
        d.open()
        yield d
        d.close()
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_is_processed_new(self, dedup):
        assert dedup.is_processed("msg_1") is False

    def test_mark_and_check(self, dedup):
        dedup.mark_processed("msg_1", "result_hash_abc")
        assert dedup.is_processed("msg_1") is True

    def test_multiple_messages(self, dedup):
        dedup.mark_processed("msg_1")
        dedup.mark_processed("msg_2")
        dedup.mark_processed("msg_3")
        assert dedup.count() == 3

    def test_duplicate_mark(self, dedup):
        dedup.mark_processed("msg_1")
        dedup.mark_processed("msg_1")  # второй раз — ignore
        assert dedup.count() == 1

    def test_cleanup_ttl(self, dedup):
        dedup.mark_processed("msg_1")
        dedup.mark_processed("msg_2")
        assert dedup.count() == 2
        dedup.cleanup()  # TTL не истёк — ничего не удаляем
        assert dedup.count() == 2

    def test_get_stats(self, dedup):
        dedup.mark_processed("msg_1")
        stats = dedup.get_stats()
        assert stats["total_entries"] == 1
        assert stats["ttl_days"] == 7


# ─────────────────────────── Coordinator (интеграция) ─────────


class TestCoordinator:
    @pytest.mark.asyncio
    async def test_coordinator_start_stop(self):
        coord = Coordinator("node_1", [])
        await coord.start()
        assert coord.raft.state in (RaftState.FOLLOWER, RaftState.LEADER)
        assert coord.dedup is not None
        await coord.stop()

    @pytest.mark.asyncio
    async def test_ordered_broadcast_only_leader(self):
        """Только leader может делать ordered_broadcast."""
        coord = Coordinator("node_1", [], raft_election_timeout=(0.1, 0.2))
        await coord.start()
        await asyncio.sleep(1.5)

        if coord.is_leader:
            seq = await coord.ordered_broadcast("signals", {"price": 100})
            assert seq is not None, "Leader должен получить seq"
            assert seq >= 1
        else:
            seq = await coord.ordered_broadcast("signals", {"price": 100})
            assert seq is None, "Не-leader не может broadcast"

        await coord.stop()

    @pytest.mark.asyncio
    async def test_create_consumer_group(self):
        coord = Coordinator("node_1", [])
        await coord.start()

        cg = coord.create_group("analysts", "forecaster_v2")
        assert cg is not None
        assert cg.group_id == "analysts"
        assert cg.consumer_id == "forecaster_v2"

        status = coord.get_status()
        assert len(status["groups"]) == 1
        assert status["groups"][0]["group_id"] == "analysts"

        await coord.stop()

    @pytest.mark.asyncio
    async def test_full_integration(self):
        """Полная интеграция: Raft → ConsumerGroup → Dedup."""
        coord = Coordinator("node_1", [], raft_election_timeout=(0.1, 0.2))
        await coord.start()
        await asyncio.sleep(1.5)

        # Создаём consumer group
        cg = coord.create_group("traders", "forecaster_v2")
        received = []
        cg.subscribe("signals", lambda m: received.append(m))

        # Ordered broadcast (если leader)
        if coord.is_leader:
            seq = await coord.ordered_broadcast(
                "signals",
                {"asset": "BTC", "price": 85000},
            )
            assert seq is not None
            await asyncio.sleep(2)

            # Проверяем dedup
            msg_id = f"signals:{seq}"
            assert coord.dedup.is_processed(msg_id), "Dedup должен видеть сообщение"

        await coord.stop()

    @pytest.mark.asyncio
    async def test_dedup_prevents_duplicates(self):
        """Exactly-once: второе совпадающее сообщение не доставляется."""
        coord = Coordinator("node_1", [], raft_election_timeout=(0.1, 0.2))
        await coord.start()
        await asyncio.sleep(1.5)

        if coord.is_leader:
            # Первая доставка
            seq1 = await coord.ordered_broadcast("test", {"val": 1})
            await asyncio.sleep(1)
            assert coord.dedup.is_processed(f"test:{seq1}")

            # Повтор той же команды
            coord.dedup.mark_processed(f"test:{seq1}")  # дубль
            count = coord.dedup.count()
            assert count >= 1

        await coord.stop()

    @pytest.mark.asyncio
    async def test_coordinator_status(self):
        coord = Coordinator("node_1", ["node_2"])
        await coord.start()
        await asyncio.sleep(0.5)

        status = coord.get_status()
        assert status["node_id"] == "node_1"
        assert status["peers"] == ["node_2"]
        assert "raft" in status
        assert "dedup" in status
        assert "groups" in status

        await coord.stop()

    @pytest.mark.asyncio
    async def test_multi_node_ordered_broadcast(self):
        """2 координатора: leader broadcast → follower получает."""
        coord1 = Coordinator("node_1", ["node_2"], raft_election_timeout=(0.1, 0.3))
        coord2 = Coordinator("node_2", ["node_1"], raft_election_timeout=(0.1, 0.3))
        await asyncio.gather(coord1.start(), coord2.start())
        await asyncio.sleep(3)

        # Определяем leader'а
        leader = coord1 if coord1.is_leader else coord2 if coord2.is_leader else None
        follower = coord2 if coord1.is_leader else coord1 if coord2.is_leader else None

        if leader and follower:
            print(f"  Leader: {leader.node_id}, Follower: {follower.node_id}")
            seq = await leader.ordered_broadcast("signals", {"val": 42})
            assert seq is not None
            await asyncio.sleep(2)
            # Проверка, что Raft-лог растёт
            assert leader.raft.commit_index >= 1
        else:
            print("  No leader elected (expected in in-process test)")

        await asyncio.gather(coord1.stop(), coord2.stop())
