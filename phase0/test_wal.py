"""Phase 0 — WAL test: SQLite буфер для offline-first."""

import os
import tempfile
import time

import pytest

from phase0.wal import WALBuffer


@pytest.fixture
def wal():
    db = tempfile.mktemp(suffix=".db")
    w = WALBuffer(db)
    yield w
    w.close()
    os.unlink(db)


def test_append_and_replay(wal):
    """Записать 5 сообщений → replay — получить их обратно."""
    msgs = []
    for i in range(5):
        msg = {
            "id": f"msg_{i}",
            "topic": "test",
            "from": f"node_{i % 3}",
            "payload": {"seq": i},
            "ts": time.time() + i * 0.1,
        }
        msg_id = wal.append(msg)
        assert msg_id == f"msg_{i}"
        msgs.append(msg)

    replayed = wal.replay("test")
    assert len(replayed) == 5, f"Должно быть 5 сообщений, получено {len(replayed)}"
    assert replayed[0]["id"] == "msg_0"
    assert replayed[-1]["id"] == "msg_4"


def test_replay_since_id(wal):
    """Replay начиная с определённого ID."""
    for i in range(5):
        wal.append(
            {
                "id": f"msg_{i}",
                "topic": "test",
                "from": "node_0",
                "payload": {"seq": i},
                "ts": time.time() + i * 0.1,
            }
        )

    after = wal.replay("test", since_id="msg_2")
    assert len(after) == 2, f"Должно быть 2 сообщения после msg_2, получено {len(after)}"
    assert after[0]["id"] == "msg_3"


def test_prune(wal):
    """Удалить старые сообщения."""
    now = time.time()
    for i in range(10):
        wal.append(
            {
                "id": f"msg_{i}",
                "topic": "test",
                "from": "node_0",
                "payload": {"seq": i},
                "ts": now - (10 - i) * 10,  # от -100s до 0s
            }
        )

    assert wal.count("test") == 10
    removed = wal.prune(now - 50)  # удалить старше 50 секунд
    assert removed >= 5, f"Должно быть удалено >=5, удалено {removed}"
    assert wal.count("test") <= 5


def test_dedup(wal):
    """Повторная запись с тем же ID не создаёт дубликат."""
    msg = {
        "id": "unique_1",
        "topic": "test",
        "from": "n",
        "payload": {},
        "ts": time.time(),
    }
    id1 = wal.append(msg)
    id2 = wal.append(msg)
    assert id1 == id2
    assert wal.count("test") == 1


def test_empty_replay(wal):
    """Replay пустого топика возвращает пустой список."""
    assert wal.replay("nonexistent") == []


def test_auto_id(wal):
    """Если id не указан — генерируется автоматически."""
    msg = {"topic": "test", "from": "node_0", "payload": {"x": 1}, "ts": time.time()}
    msg_id = wal.append(msg)
    assert len(msg_id) == 16  # sha256 hash truncated
