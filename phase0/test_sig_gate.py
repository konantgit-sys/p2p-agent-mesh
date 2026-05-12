"""Phase 0 — Sig Gate test: верификация + rate limiting + allowlist."""

import json

from phase0.identity import Identity
from phase0.sig_gate import SigGate


def make_signed_msg(ident: Identity, topic: str = "test", payload: dict = None):
    """Создать подписанное сообщение и сериализовать в bytes."""
    msg = ident.sign_message(
        {
            "topic": topic,
            "payload": payload or {"data": "test"},
        }
    )
    return json.dumps(msg).encode()


def test_passes_valid_message():
    """Валидное подписанное сообщение → проходит."""
    gate = SigGate()
    ident = Identity()
    raw = make_signed_msg(ident)
    result = gate.check(raw)
    assert result is not None
    assert result["from"] == ident.did


def test_rejects_no_signature():
    """Сообщение без подписи → rejected."""
    gate = SigGate()
    raw = json.dumps({"topic": "test", "payload": {}, "from": "anon"}).encode()
    assert gate.check(raw) is None


def test_rejects_tampered():
    """Сообщение с изменённым payload → rejected."""
    gate = SigGate()
    ident = Identity()
    msg = ident.sign_message({"topic": "test", "payload": {"original": "data"}})
    msg["payload"] = {"tampered": "yes"}
    raw = json.dumps(msg).encode()
    assert gate.check(raw) is None


def test_rate_limit():
    """10 msg/sec limit → 11-е rejected."""
    gate = SigGate(rate_limit=10, window=1.0)
    ident = Identity()

    # 10 сообщений — все проходят
    for i in range(10):
        raw = make_signed_msg(ident, payload={"seq": i})
        assert gate.check(raw) is not None, f"Message {i} should pass"

    # 11-е — rejected
    raw = make_signed_msg(ident, payload={"seq": 11})
    assert gate.check(raw) is None

    stats = gate.stats()
    assert stats["passed"] == 10
    assert stats["rejected_rate"] >= 1


def test_allowlist():
    """Только DID из allowlist проходят."""
    gate = SigGate()
    alice = Identity()
    bob = Identity()

    # Alice в allowlist
    gate.set_allowlist([alice.did])

    raw_alice = make_signed_msg(alice)
    assert gate.check(raw_alice) is not None

    # Bob не в allowlist
    raw_bob = make_signed_msg(bob)
    assert gate.check(raw_bob) is None


def test_denylist():
    """DID из denylist rejected."""
    gate = SigGate()
    ident = Identity()
    gate.deny(ident.did)

    raw = make_signed_msg(ident)
    assert gate.check(raw) is None


def test_stats():
    """Статистика после обработки сообщений."""
    gate = SigGate()
    alice = Identity()
    bob = Identity()

    # Alice валидное
    gate.check(make_signed_msg(alice))
    # Bob без подписи
    gate.check(json.dumps({"topic": "t", "from": "x", "payload": {}}).encode())
    # Alice валидное
    gate.check(make_signed_msg(alice, payload={"seq": 1}))
    # Bob валидное
    gate.check(make_signed_msg(bob))

    stats = gate.stats()
    assert stats["passed"] == 3
    assert stats["rejected_sig"] >= 1
