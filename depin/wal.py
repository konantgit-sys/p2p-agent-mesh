# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""DePIN SDK — Локальное хранилище телеметрии (WAL).

Offline-first: устройство пишет телеметрию в WAL даже без сети.
При reconnect — Merkle-sync догоняет пропущенное.

Хранилище: SQLite с метаданными для Merkle-дерева.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path


class DePINWAL:
    """Write-Ahead Log для телеметрии устройства.

    Отличается от phase0 WAL:
    - device_id + device_type для маршрутизации
    - metrics как JSON (не бинарный blob)
    - seq_number для Merkle-дерева
    - ttl для автоматической очистки старых записей
    """

    def __init__(self, path: str, device_id: str, ttl_days: int = 30):
        self.path = path
        self.device_id = device_id
        self.ttl_days = ttl_days
        self._lock = threading.Lock()
        self._db: sqlite3.Connection | None = None

    def open(self):
        """Открыть/создать БД."""
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS depin_wal (
                seq       INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                topic     TEXT NOT NULL,
                telemetry TEXT NOT NULL,   -- JSON
                ts        REAL NOT NULL,
                hash      TEXT NOT NULL UNIQUE
            )
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_depin_wal_hash
            ON depin_wal(hash)
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_depin_wal_ts
            ON depin_wal(ts)
        """)
        self._db.commit()

    def close(self):
        if self._db:
            self._db.close()
            self._db = None

    def append(self, topic: str, telemetry: dict, ts: float | None = None) -> str:
        """Добавить запись телеметрии.

        Returns:
            hash записи (SHA256)
        """
        ts = ts or time.time()
        payload = json.dumps(telemetry, sort_keys=True)
        raw = f"{self.device_id}|{topic}|{payload}|{ts}".encode()
        hash_val = hashlib.sha256(raw).hexdigest()

        with self._lock:
            self._db.execute(
                "INSERT OR IGNORE INTO depin_wal (device_id, topic, telemetry, ts, hash) "
                "VALUES (?, ?, ?, ?, ?)",
                (self.device_id, topic, payload, ts, hash_val),
            )
            self._db.commit()

        return hash_val

    def get_all(self, limit: int = 1000) -> list[bytes]:
        """Получить все записи (для Merkle-дерева)."""
        with self._lock:
            rows = self._db.execute(
                "SELECT seq, telemetry FROM depin_wal WHERE device_id = ? ORDER BY seq ASC LIMIT ?",
                (self.device_id, limit),
            ).fetchall()
        return [row[1].encode() for row in rows]

    def get_since(self, seq: int) -> list[dict]:
        """Получить записи начиная с seq (для diff)."""
        with self._lock:
            rows = self._db.execute(
                "SELECT seq, topic, telemetry, ts FROM depin_wal "
                "WHERE device_id = ? AND seq > ? "
                "ORDER BY seq ASC",
                (self.device_id, seq),
            ).fetchall()
        return [
            {"seq": r[0], "topic": r[1], "telemetry": json.loads(r[2]), "ts": r[3]} for r in rows
        ]

    def count(self) -> int:
        """Количество записей."""
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) FROM depin_wal WHERE device_id = ?",
                (self.device_id,),
            ).fetchone()
        return row[0] if row else 0

    def last_seq(self) -> int:
        """Последний seq номер."""
        with self._lock:
            row = self._db.execute(
                "SELECT MAX(seq) FROM depin_wal WHERE device_id = ?",
                (self.device_id,),
            ).fetchone()
        return row[0] if row and row[0] else 0

    def cleanup(self):
        """Удалить записи старше ttl_days."""
        cutoff = time.time() - (self.ttl_days * 86400)
        with self._lock:
            self._db.execute("DELETE FROM depin_wal WHERE ts < ?", (cutoff,))
            self._db.commit()

    def count_by_topic(self) -> dict[str, int]:
        """Статистика по топикам."""
        with self._lock:
            rows = self._db.execute(
                "SELECT topic, COUNT(*) FROM depin_wal WHERE device_id = ? GROUP BY topic",
                (self.device_id,),
            ).fetchall()
        return dict(rows)
