# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Phase 0 — Sig Gate: верификация подписей + rate limiting + allowlist.



Фильтрует входящие сообщения:
1. Проверяет наличие подписи (signature, pubkey, from)
2. Верифицирует подпись через Identity.verify()
3. Rate limiting по отправителю (сообщений/сек)
4. Allowlist: только узлы из списка (опционально)
"""

import time
from collections import defaultdict

from phase0.identity import Identity


class SigGate:
    """Верификация подписей + rate limiting."""

    def __init__(self, rate_limit: int = 10, window: float = 1.0):
        self.rate_limit = rate_limit  # макс сообщений в окне
        self.window = window  # окно в секундах
        self._counters: dict[str, list[float]] = defaultdict(list)
        self._allowlist: set[str] | None = None  # None = все пропускаем
        self._denylist: set[str] = set()
        self._stats = {
            "passed": 0,
            "rejected_rate": 0,
            "rejected_sig": 0,
            "rejected_deny": 0,
        }

    def set_allowlist(self, dids: list[str]):
        """Установить allowlist (только эти DID пропускаем)."""
        self._allowlist = set(dids)

    def deny(self, did: str):
        """Добавить DID в денилист."""
        self._denylist.add(did)

    def _check_rate(self, sender: str) -> bool:
        """Проверить rate limit."""
        now = time.time()
        timestamps = self._counters[sender]
        # Удалить старые записи
        self._counters[sender] = [t for t in timestamps if now - t < self.window]
        if len(self._counters[sender]) >= self.rate_limit:
            return False
        self._counters[sender].append(now)
        return True

    def check(self, raw_msg: bytes) -> dict | None:
        """Проверить входящее сообщение.

        Возвращает: dict сообщения если прошло, None если rejected.
        """
        import json

        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            self._stats["rejected_sig"] += 1
            return None

        # Проверка наличия полей
        if not all(k in msg for k in ["from", "signature", "pubkey"]):
            self._stats["rejected_sig"] += 1
            return None

        # Проверка allowlist
        if self._allowlist is not None and msg["from"] not in self._allowlist:
            self._stats["rejected_deny"] += 1
            return None

        # Проверка denylist
        if msg["from"] in self._denylist:
            self._stats["rejected_deny"] += 1
            return None

        # Верификация подписи
        if not Identity.verify(msg):
            self._stats["rejected_sig"] += 1
            return None

        # Rate limit
        if not self._check_rate(msg["from"]):
            self._stats["rejected_rate"] += 1
            return None

        self._stats["passed"] += 1
        return msg

    def stats(self) -> dict:
        return {**self._stats, "allowlist": self._allowlist is not None}
