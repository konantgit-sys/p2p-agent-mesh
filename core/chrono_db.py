"""
CRYTER V10.5 — CHRONO DB (Temporal Event Log)
Хранит временные ряды событий P2P Mesh для аналитики и восстановления состояния.

Схема:
  events: id, agent_id, msg_id, capability, event_type, payload (JSON), ts, created_at
  metrics: id, agent_id, metric_type, value, ts
  agent_state: agent_id, state (JSON), updated_at (upsert)
"""

import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("chrono_db")

DB_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DB_PATH = os.path.join(DB_DIR, "chrono_mesh.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id    TEXT NOT NULL,
    msg_id      TEXT,
    capability  TEXT,
    event_type  TEXT NOT NULL DEFAULT 'event',
    payload     TEXT,
    ts          REAL NOT NULL,
    created_at  REAL NOT NULL DEFAULT (julianday('now'))
);
CREATE INDEX IF NOT EXISTS idx_events_agent ON events(agent_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_cap ON events(capability, ts);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, ts);

CREATE TABLE IF NOT EXISTS metrics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id    TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    value       REAL,
    payload     TEXT,
    ts          REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_metrics_agent ON metrics(agent_id, metric_type, ts);

CREATE TABLE IF NOT EXISTS agent_state (
    agent_id    TEXT PRIMARY KEY,
    npub        TEXT,
    state       TEXT NOT NULL,
    updated_at  REAL NOT NULL
);

-- Очистка старых событий (старше 7 дней)
CREATE VIEW IF NOT EXISTS v_events_last_hour AS
SELECT * FROM events WHERE ts > (julianday('now') - 1/24.0) * 86400;

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
"""


class ChronoDB:
    """Temporal event store для P2P Mesh агентов."""

    def __init__(self, db_path: str = DB_PATH):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _init_db(self):
        """Инициализация схемы."""
        try:
            conn = self._get_conn()
            conn.executescript(SCHEMA_SQL)
            conn.commit()
        except Exception as e:
            logger.warning(f"ChronoDB init error: {e}")

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Events ────────────────────────────────────────────

    def save_event(self, agent_id: str, msg_id: str, capability: str,
                   event_type: str = "event", payload: Optional[Dict] = None) -> int:
        """Сохранить событие mesh. Возвращает ID записи."""
        conn = self._get_conn()
        now = time.time()
        conn.execute(
            "INSERT INTO events (agent_id, msg_id, capability, event_type, payload, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (agent_id, msg_id, capability, event_type,
             json.dumps(payload) if payload else None, now)
        )
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_events(self, agent_id: Optional[str] = None, capability: Optional[str] = None,
                   limit: int = 50, since_ts: Optional[float] = None) -> List[Dict]:
        """Получить события с фильтрацией."""
        conn = self._get_conn()
        parts = ["SELECT * FROM events WHERE 1=1"]
        params = []

        if agent_id:
            parts.append("AND agent_id = ?")
            params.append(agent_id)
        if capability:
            parts.append("AND capability = ?")
            params.append(capability)
        if since_ts:
            parts.append("AND ts > ?")
            params.append(since_ts)

        parts.append("ORDER BY ts DESC LIMIT ?")
        params.append(limit)

        rows = conn.execute(" ".join(parts), params).fetchall()
        return [dict(r) for r in rows]

    def count_events(self, since_ts: Optional[float] = None) -> int:
        """Количество событий за период."""
        conn = self._get_conn()
        if since_ts:
            row = conn.execute("SELECT COUNT(*) FROM events WHERE ts > ?", (since_ts,)).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) FROM events").fetchone()
        return row[0]

    # ── Metrics ────────────────────────────────────────────

    def save_metric(self, agent_id: str, metric_type: str, value: float,
                    payload: Optional[Dict] = None) -> int:
        """Сохранить метрику."""
        conn = self._get_conn()
        now = time.time()
        conn.execute(
            "INSERT INTO metrics (agent_id, metric_type, value, payload, ts) VALUES (?, ?, ?, ?, ?)",
            (agent_id, metric_type, value, json.dumps(payload) if payload else None, now)
        )
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_metrics(self, agent_id: str, metric_type: Optional[str] = None,
                    limit: int = 100) -> List[Dict]:
        """Получить метрики агента."""
        conn = self._get_conn()
        if metric_type:
            rows = conn.execute(
                "SELECT * FROM metrics WHERE agent_id = ? AND metric_type = ? "
                "ORDER BY ts DESC LIMIT ?",
                (agent_id, metric_type, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM metrics WHERE agent_id = ? ORDER BY ts DESC LIMIT ?",
                (agent_id, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_avg_metric(self, agent_id: str, metric_type: str, since_ts: float) -> float:
        """Среднее значение метрики за период."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT AVG(value) FROM metrics WHERE agent_id = ? AND metric_type = ? AND ts > ?",
            (agent_id, metric_type, since_ts)
        ).fetchone()
        return row[0] or 0.0

    # ── Agent State ────────────────────────────────────────

    def save_agent_state(self, agent_id: str, state: Dict, npub: str = "") -> bool:
        """Сохранить/обновить состояние агента (upsert)."""
        conn = self._get_conn()
        now = time.time()
        conn.execute(
            "INSERT INTO agent_state (agent_id, npub, state, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(agent_id) DO UPDATE SET state = ?, updated_at = ?",
            (agent_id, npub, json.dumps(state), now, json.dumps(state), now)
        )
        conn.commit()
        return True

    def get_agent_state(self, agent_id: str) -> Optional[Dict]:
        """Получить последнее состояние агента."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM agent_state WHERE agent_id = ?", (agent_id,)
        ).fetchone()
        if row:
            result = dict(row)
            try:
                result["state"] = json.loads(result["state"])
            except (json.JSONDecodeError, TypeError):
                pass
            return result
        return None

    def get_all_agent_states(self) -> List[Dict]:
        """Получить состояния всех агентов."""
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM agent_state ORDER BY updated_at DESC").fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["state"] = json.loads(d["state"])
            except (json.JSONDecodeError, TypeError):
                pass
            result.append(d)
        return result

    # ── Stats ──────────────────────────────────────────────

    def get_stats(self) -> Dict:
        """Статистика БД."""
        conn = self._get_conn()
        events = self.count_events()
        metrics = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        agents = conn.execute("SELECT COUNT(*) FROM agent_state").fetchone()[0]
        size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        return {
            "events": events,
            "metrics": metrics,
            "agents": agents,
            "db_size_mb": round(size / 1048576, 2),
        }

    # ── Maintenance ────────────────────────────────────────

    def cleanup_old_events(self, days: int = 7):
        """Удалить события старше N дней."""
        conn = self._get_conn()
        cutoff = time.time() - days * 86400
        deleted = conn.execute("DELETE FROM events WHERE ts < ?", (cutoff,)).rowcount
        conn.commit()
        if deleted:
            conn.execute("VACUUM")
        return deleted


# ── QUICK TEST ────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== ChronoDB Test ===\n")

    db = ChronoDB("/tmp/test_chrono.db")

    # Events
    eid = db.save_event("cryter", "msg_001", "post", payload={"text": "Hello", "len": 140})
    print(f"Saved event id={eid}")

    eid2 = db.save_event("analyst", "msg_002", "analyze", payload={"ticker": "BTC", "price": 85000})
    print(f"Saved event id={eid2}")

    evts = db.get_events(limit=10)
    print(f"Events: {len(evts)}")
    for e in evts:
        print(f"  {e['agent_id']}: {e['capability']} @ {e['ts']:.0f}")

    # Metrics
    db.save_metric("cryter", "cycle_duration_s", 128.5)
    db.save_metric("cryter", "relay_success_rate", 0.96)
    avg = db.get_avg_metric("cryter", "cycle_duration_s", time.time() - 86400)
    print(f"Avg cycle duration: {avg:.1f}s")

    # Agent state
    db.save_agent_state("cryter", {"status": "active", "cycles": 10}, npub="npub1cryter")
    s = db.get_agent_state("cryter")
    print(f"State: {s['state'] if s else 'none'}")
    print(f"Stats: {db.get_stats()}")

    # Cleanup
    db.close()
    os.remove("/tmp/test_chrono.db")
    print("\n✅ ChronoDB OK")
