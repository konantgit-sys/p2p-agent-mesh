"""Phase 0 — Identity test: Ed25519 подпись + верификация."""

from phase0.identity import Identity


def test_generate_key():
    """Генерация ключа → DID не пустой."""
    ident = Identity()
    assert ident.did.startswith("did:snin:")
    assert len(ident.public_key_hex) == 64  # 32 bytes hex


def test_sign_and_verify():
    """Подписать и верифицировать сообщение."""
    ident = Identity()
    msg = ident.sign_message(
        {
            "topic": "test",
            "payload": {"hello": "world"},
        }
    )
    assert "signature" in msg
    assert "pubkey" in msg
    assert "from" in msg
    assert Identity.verify(msg)


def test_verify_tampered():
    """Подмена payload → верификация FAIL."""
    ident = Identity()
    msg = ident.sign_message(
        {
            "topic": "test",
            "payload": {"original": "data"},
        }
    )
    msg["payload"] = {"tampered": "yes"}
    assert not Identity.verify(msg)


def test_verify_no_signature():
    """Сообщение без подписи → FAIL."""
    msg = {"topic": "test", "payload": {}, "from": "anon"}
    assert not Identity.verify(msg)


def test_verify_wrong_pubkey():
    """Подпись от Alice, а pubkey от Bob → FAIL."""
    alice = Identity()
    bob = Identity()

    msg = alice.sign_message(
        {
            "topic": "test",
            "payload": {"secret": "data"},
        }
    )
    # Подменяем pubkey на Bob
    msg["pubkey"] = bob.public_key_hex
    assert not Identity.verify(msg)


def test_from_seed():
    """Одинаковый seed → одинаковый DID."""
    seed = b"a" * 32
    a = Identity.from_seed(seed)
    b = Identity.from_seed(seed)
    assert a.did == b.did
    assert a.private_key_hex == b.private_key_hex


def test_from_private_key_hex():
    """Сохранение и восстановление ключа."""
    ident = Identity()
    hex_key = ident.private_key_hex
    restored = Identity.from_private_key_hex(hex_key)
    assert restored.did == ident.did
    assert restored.public_key_hex == ident.public_key_hex


def test_multiple_messages():
    """Серия подписанных сообщений — все верифицируются."""
    ident = Identity()
    for i in range(10):
        msg = ident.sign_message(
            {
                "topic": f"test_{i}",
                "payload": {"seq": i},
            }
        )
        assert Identity.verify(msg), f"Message {i} failed verification"
