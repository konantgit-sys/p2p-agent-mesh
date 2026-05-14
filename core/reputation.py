"""
P2P Agent Mesh — Reputation Scoring (Core)

Алгоритм репутации для P2P mesh:
- Каждый агент имеет локальный вес репутации других агентов
- Факторы: uptime, количество успешных доставок, время ответа, флуд-склонность
- Репутация синхронизируется через DHT
- Порог отсечения для trust/untrust
"""
import logging, math, time
from collections import defaultdict

log = logging.getLogger('reputation')

# Пороги
TRUST_THRESHOLD = 0.7
BAN_THRESHOLD = -0.5
DECAY_RATE = 0.95
REPORT_WEIGHT = 0.3

WEIGHT_DELIVERY_SUCCESS = 0.4
WEIGHT_UPTIME = 0.25
WEIGHT_LATENCY = 0.15
WEIGHT_RATE_LIMIT = 0.2


class ReputationRecord:
    """Репутация одного агента глазами локального агента."""

    def __init__(self, pubkey: str):
        self.pubkey = pubkey
        self.score = 0.0
        self.confidence = 0.0
        self.first_seen = time.time()
        self.last_seen = time.time()

        self.msgs_sent = 0
        self.msgs_delivered = 0
        self.msgs_dropped = 0
        self.rate_limit_violations = 0
        self.total_uptime_sec = 0.0
        self.latencies: list[float] = []
        self.reports: list[tuple[str, float, float]] = []

    def record_delivery(self, success: bool, latency_ms: float = 0):
        self.msgs_sent += 1
        if success:
            self.msgs_delivered += 1
        else:
            self.msgs_dropped += 1
        if latency_ms > 0:
            self.latencies.append(latency_ms)
            if len(self.latencies) > 100:
                self.latencies = self.latencies[-100:]
        self.last_seen = time.time()
        self._recalculate()

    def record_uptime(self, sec: float):
        self.total_uptime_sec += sec
        self.last_seen = time.time()
        self._recalculate()

    def record_violation(self):
        self.rate_limit_violations += 1
        self._recalculate()

    def add_report(self, reporter_pubkey: str, score: float, weight: float = 1.0):
        self.reports = [(r, s, w) for r, s, w in self.reports if r != reporter_pubkey]
        self.reports.append((reporter_pubkey, score, weight))
        self._recalculate()

    def _recalculate(self):
        days_idle = (time.time() - self.last_seen) / 86400
        decay = DECAY_RATE ** max(0, days_idle - 1)

        del_ratio = 0.5
        if self.msgs_sent > 0:
            del_ratio = self.msgs_delivered / self.msgs_sent

        uptime_factor = min(1.0, self.total_uptime_sec / 86400)

        lat_factor = 1.0
        if self.latencies:
            avg_lat = sum(self.latencies) / len(self.latencies)
            if avg_lat > 0:
                lat_factor = max(0.0, 1.0 - math.log10(avg_lat / 10) * 0.3)

        rl_factor = 1.0
        if self.rate_limit_violations > 0:
            rl_factor = max(0.0, 1.0 - self.rate_limit_violations * 0.1)

        base_score = (
            del_ratio * WEIGHT_DELIVERY_SUCCESS +
            uptime_factor * WEIGHT_UPTIME +
            lat_factor * WEIGHT_LATENCY +
            rl_factor * WEIGHT_RATE_LIMIT
        )
        base_score = max(0.0, min(1.0, base_score))

        report_score = 0.0
        report_confidence = 0.0
        if self.reports:
            total_weight = sum(w for _, _, w in self.reports)
            if total_weight > 0:
                report_score = sum(s * w for _, s, w in self.reports) / total_weight
                report_confidence = min(1.0, total_weight / 10)

        own_weight = 1.0 - report_confidence * REPORT_WEIGHT
        combined = base_score * own_weight + report_score * report_confidence * REPORT_WEIGHT
        combined *= decay

        self.score = combined * 2 - 1
        self.confidence = min(1.0, (
            (self.msgs_sent / 50) * 0.4 +
            (self.total_uptime_sec / 86400) * 0.3 +
            report_confidence * 0.3
        ))

    @property
    def is_trusted(self) -> bool:
        return self.score >= TRUST_THRESHOLD

    @property
    def is_banned(self) -> bool:
        return self.score <= BAN_THRESHOLD


class ReputationEngine:
    """Центральный движок репутации для агента."""

    def __init__(self, own_pubkey: str):
        self.own_pubkey = own_pubkey
        self._records: dict[str, ReputationRecord] = {}

    def get_or_create(self, pubkey: str) -> ReputationRecord:
        if pubkey not in self._records:
            self._records[pubkey] = ReputationRecord(pubkey)
        return self._records[pubkey]

    def record_delivery(self, pubkey: str, success: bool, latency_ms: float = 0):
        self.get_or_create(pubkey).record_delivery(success, latency_ms)

    def record_uptime(self, pubkey: str, sec: float):
        self.get_or_create(pubkey).record_uptime(sec)

    def record_violation(self, pubkey: str):
        self.get_or_create(pubkey).record_violation()

    def add_report(self, reporter: str, target: str, score: float, weight: float = 1.0):
        self.get_or_create(target).add_report(reporter, score, weight)

    def get_score(self, pubkey: str) -> float:
        rec = self._records.get(pubkey)
        return rec.score if rec else 0.0

    def get_confidence(self, pubkey: str) -> float:
        rec = self._records.get(pubkey)
        return rec.confidence if rec else 0.0

    def is_trusted(self, pubkey: str) -> bool:
        rec = self._records.get(pubkey)
        return rec.is_trusted if rec else False

    def is_banned(self, pubkey: str) -> bool:
        rec = self._records.get(pubkey)
        return rec.is_banned if rec else False

    def trusted_list(self) -> list[tuple[str, float]]:
        return [(pk, r.score) for pk, r in self._records.items() if r.is_trusted]

    def banned_list(self) -> list[tuple[str, float]]:
        return [(pk, r.score) for pk, r in self._records.items() if r.is_banned]

    def stats(self) -> dict:
        return {
            'total_agents': len(self._records),
            'trusted': len(self.trusted_list()),
            'banned': len(self.banned_list()),
            'unknown': len(self._records) - len(self.trusted_list()) -
                       len(self.banned_list())
        }
