# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Phase 0 — WAL: Write-Ahead Log на SQLite.



Буферизирует все отправленные и полученные сообщения.
При reconnect: replay с момента последнего коннекта.
"""

import json
import sqlite3
import threading
import time


class WALBuffer:
    """SQLite-backed WAL для offline-first сообщений."""

    def __init__(self, db_path: str = "/tmp/p2p_mesh_wal.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                sender TEXT NOT NULL,
                payload TEXT NOT NULL,
                signature TEXT,
                pubkey TEXT,
                ts REAL NOT NULL,
                received_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_topic_ts
            ON messages(topic, ts)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_id
            ON messages(id)
        """)
        conn.commit()
        conn.close()

    def append(self, msg: dict) -> str:
        """Добавить сообщение в WAL. Возвращает msg_id."""
        msg_id = msg.get("id", "")
        if not msg_id:
            import hashlib

            raw = json.dumps(msg, sort_keys=True).encode()
            msg_id = hashlib.sha256(raw).hexdigest()[:16]
            msg["id"] = msg_id

        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT OR IGNORE INTO messages
                   (id, topic, sender, payload, signature, pubkey, ts, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    msg_id,
                    msg.get("topic", ""),
                    msg.get("from", ""),
                    json.dumps(msg.get("payload", {})),
                    msg.get("signature", ""),
                    msg.get("pubkey", ""),
                    msg.get("ts", time.time()),
                    time.time(),
                ),
            )
            conn.commit()
        except Exception as e:
            print(f"[wal] append error: {e}")
        return msg_id

    def replay(self, topic: str, since_id: str | None = None, limit: int = 1000) -> list[dict]:
        """Воспроизвести сообщения по топику, опционально с ID."""
        conn = self._get_conn()
        if since_id:
            row = conn.execute("SELECT ts FROM messages WHERE id = ?", (since_id,)).fetchone()
            if row:
                cursor = conn.execute(
                    """SELECT id, topic, sender, payload, signature, pubkey, ts
                       FROM messages WHERE topic = ? AND ts > ?
                       ORDER BY ts ASC LIMIT ?""",
                    (topic, row[0], limit),
                )
            else:
                return []
        else:
            cursor = conn.execute(
                """SELECT id, topic, sender, payload, signature, pubkey, ts
                   FROM messages WHERE topic = ?
                   ORDER BY ts ASC LIMIT ?""",
                (topic, limit),
            )
        results = []
        for row in cursor.fetchall():
            results.append(
                {
                    "id": row[0],
                    "topic": row[1],
                    "from": row[2],
                    "payload": json.loads(row[3]) if row[3] else {},
                    "signature": row[4],
                    "pubkey": row[5],
                    "ts": row[6],
                }
            )
        return results

    def prune(self, before_ts: float) -> int:
        """Удалить сообщения старше timestamp. Возвращает количество."""
        conn = self._get_conn()
        cursor = conn.execute("DELETE FROM messages WHERE ts < ?", (before_ts,))
        conn.commit()
        return cursor.rowcount

    def count(self, topic: str | None = None) -> int:
        """Количество сообщений в WAL."""
        conn = self._get_conn()
        if topic:
            row = conn.execute("SELECT COUNT(*) FROM messages WHERE topic = ?", (topic,)).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) FROM messages").fetchone()
        return row[0] if row else 0

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
