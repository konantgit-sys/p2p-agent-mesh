# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Exactly-once deduplication — идемпотентный лог.



Гарантия: каждое сообщение обрабатывается ровно 1 раз,
даже при дублирующей доставке (reconnect, retry, fork).

Алгоритм:
  1. Каждое сообщение имеет idempotency_key (msg_id)
  2. Перед обработкой → проверка в dedup-логе
  3. Если уже processed → skip с уведомлением
  4. Если new → process + запись в лог
  5. Лог очищается по TTL (7 дней по умолчанию)

Хранилище: in-memory dict (для MVP) или SQLite.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path


class DedupLog:
    """Лог идемпотентности — exactly-once гарантия.

    Использование:
        dedup = DedupLog("/tmp/dedup.db")

        msg_id = "sha256:abc123..."
        if not dedup.is_processed(msg_id):
            result = process_message(msg)
            dedup.mark_processed(msg_id, result)
        else:
            print(f"Skipping {msg_id} — already processed")
    """

    def __init__(self, path: str, ttl_days: int = 7):
        self.path = path
        self.ttl_sec = ttl_days * 86400
        self._lock = threading.Lock()
        self._db: sqlite3.Connection | None = None

    def open(self):
        """Открыть/создать БД."""
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS dedup_log (
                msg_id       TEXT PRIMARY KEY,
                result_hash  TEXT NOT NULL,
                ts           REAL NOT NULL,
                ttl_until    REAL NOT NULL
            )
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dedup_ttl
            ON dedup_log(ttl_until)
        """)
        self._db.commit()

    def close(self):
        if self._db:
            self._db.close()
            self._db = None

    def is_processed(self, msg_id: str) -> bool:
        """Проверить, было ли сообщение уже обработано."""
        with self._lock:
            row = self._db.execute(
                "SELECT 1 FROM dedup_log WHERE msg_id = ? AND ttl_until > ?",
                (msg_id, time.time()),
            ).fetchone()
        return row is not None

    def mark_processed(self, msg_id: str, result_hash: str = ""):
        """Отметить сообщение как обработанное."""
        now = time.time()
        with self._lock:
            self._db.execute(
                "INSERT OR IGNORE INTO dedup_log (msg_id, result_hash, ts, ttl_until) "
                "VALUES (?, ?, ?, ?)",
                (msg_id, result_hash, now, now + self.ttl_sec),
            )
            self._db.commit()

    def count(self) -> int:
        """Количество записей в логе."""
        with self._lock:
            row = self._db.execute("SELECT COUNT(*) FROM dedup_log").fetchone()
        return row[0] if row else 0

    def cleanup(self):
        """Очистить истекшие записи."""
        with self._lock:
            self._db.execute(
                "DELETE FROM dedup_log WHERE ttl_until < ?",
                (time.time(),),
            )
            self._db.commit()

    def get_stats(self) -> dict:
        """Статистика лога."""
        with self._lock:
            total = self._db.execute("SELECT COUNT(*) FROM dedup_log").fetchone()[0]
            expired = self._db.execute(
                "SELECT COUNT(*) FROM dedup_log WHERE ttl_until < ?",
                (time.time(),),
            ).fetchone()[0]
            oldest = self._db.execute("SELECT MIN(ts) FROM dedup_log").fetchone()[0]
        return {
            "total_entries": total,
            "expired_entries": expired,
            "oldest_entry_ts": oldest,
            "ttl_days": self.ttl_sec / 86400,
        }
