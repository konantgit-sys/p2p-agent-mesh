"""
P2P Agent Mesh — Reputation Scoring тесты
"""
import sys, time, unittest
sys.path.insert(0, '/home/agent/data/projects/p2p-agent-mesh')

from core.reputation import (
    ReputationRecord, ReputationEngine,
    TRUST_THRESHOLD, BAN_THRESHOLD
)


class TestReputation(unittest.TestCase):

    def setUp(self):
        self.rec = ReputationRecord("test_agent")

    def test_1_initial_score(self):
        """Новый агент: score=0, confidence=0"""
        self.assertEqual(self.rec.score, 0.0)
        self.assertEqual(self.rec.confidence, 0.0)
        self.assertFalse(self.rec.is_trusted)
        self.assertFalse(self.rec.is_banned)
        print(f"✅ TEST 1: initial score=0, confidence=0")

    def test_2_successful_delivery(self):
        """Успешная доставка повышает score"""
        for _ in range(10):
            self.rec.record_delivery(True, latency_ms=10)
        self.assertGreater(self.rec.score, 0)
        self.assertGreater(self.rec.confidence, 0)
        print(f"✅ TEST 2: 10 успешных → score={self.rec.score:.2f}, confidence={self.rec.confidence:.2f}")

    def test_3_dropped_messages(self):
        """Сброшенные сообщения понижают score"""
        for _ in range(10):
            self.rec.record_delivery(False)
        score_after_drops = self.rec.score
        for _ in range(10):
            self.rec.record_delivery(True, latency_ms=10)
        self.assertGreater(self.rec.score, score_after_drops)
        print(f"✅ TEST 3: drops→{score_after_drops:.2f}, recovery→{self.rec.score:.2f}")

    def test_4_rate_limit_violations(self):
        """Rate limit violations понижают score"""
        self.rec.record_delivery(True)
        score_before = self.rec.score
        for _ in range(5):
            self.rec.record_violation()
        self.assertLess(self.rec.score, score_before)
        print(f"✅ TEST 4: violations: {score_before:.2f} → {self.rec.score:.2f}")

    def test_5_uptime(self):
        """Uptime повышает score"""
        for _ in range(10):
            self.rec.record_delivery(True, latency_ms=10)
        score_no_uptime = self.rec.score
        self.rec.record_uptime(43200)  # 12 часов
        self.assertGreater(self.rec.score, score_no_uptime)
        print(f"✅ TEST 5: uptime +43200s: {score_no_uptime:.2f} → {self.rec.score:.2f}")

    def test_6_peer_reports(self):
        """Peer reports влияют на score"""
        for _ in range(10):
            self.rec.record_delivery(True, latency_ms=10)
        score_before = self.rec.score
        self.rec.add_report("peer_1", -0.8, weight=3.0)
        self.assertLess(self.rec.score, score_before)
        print(f"✅ TEST 6: peer report -0.8: {score_before:.2f} → {self.rec.score:.2f}")

    def test_7_trusted_threshold(self):
        """Агент с score >= 0.7 считается trusted"""
        rec = ReputationRecord("trusted_agent")
        for _ in range(50):
            rec.record_delivery(True, latency_ms=5)
        rec.record_uptime(86400)
        self.assertGreater(rec.score, 0.1)
        print(f"✅ TEST 7: score={rec.score:.2f}, confidence={rec.confidence:.2f}")

    def test_8_banned_threshold(self):
        """Агент с score <= -0.5 считается banned"""
        rec = ReputationRecord("bad_agent")
        for _ in range(20):
            rec.record_delivery(False)
        for _ in range(10):
            rec.record_violation()
        self.assertLess(rec.score, 0)
        print(f"✅ TEST 8: bad agent score={rec.score:.2f}")

    def test_9_reputation_engine(self):
        """ReputationEngine управляет множеством записей"""
        engine = ReputationEngine("self")
        engine.record_delivery("alice", True, latency_ms=10)
        engine.record_delivery("bob", False)
        engine.record_violation("bob")
        engine.record_uptime("alice", 86400)

        stats = engine.stats()
        self.assertEqual(stats['total_agents'], 2)
        self.assertGreater(engine.get_score("alice"), 0)
        self.assertLess(engine.get_score("bob"), engine.get_score("alice"))
        print(f"✅ TEST 9: engine stats: {stats}")


if __name__ == "__main__":
    print("=== Reputation Scoring — 9 тестов ===\n")
    unittest.main(verbosity=0)
