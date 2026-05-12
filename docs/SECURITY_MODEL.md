# P2P Agent Mesh — Security Model

## Crypto Stack

| Слой | Примитив | Назначение |
|------|----------|------------|
| **Identity** | Ed25519 | Асимметричная подпись, DID (npub/nsec) |
| **Key Exchange** | X25519 + ECDH | Эфемерный Diffie-Hellman на каждую сессию (PFS) |
| **Symmetric** | ChaCha20-Poly1305 (AEAD) | Шифрование payload, 12-byte nonce |
| **KDF** | HKDF (SHA-256) | Вывод сессионного ключа из ECDH shared secret |

## Protocol Flow

```
Client                              Server/Relay
   │                                     │
   ├── HELLO (pubkey, nonce, eph_pub) ──►│
   │◄── CHALLENGE (server_pubkey, nonce, │
   │      nonce_sig, eph_pub)            │
   ├── AUTH (nonce_sig) ────────────────►│
   │                                     │
   ╞══ E2E session (X25519+HKDF+ChaCha20)╡
   │                                     │
   ├── ENCRYPTED_MESSAGE ───────────────►│
   │   (relay cannot decrypt)             │
```

### Nonce Strategy

- **Handshake nonce:** `os.urandom(32)` — случайный, одноразовый
- **Session nonce:** sequence-based `(4 bytes seq + 8 bytes zero)` — безопасен т.к. **каждая сессия** генерирует новый ECDH ключ
- **Повтор nonce невозможен** между сессиями — ключи разные

## Threat Model

### Trust Assumptions

| Компонент | Доверие | Обоснование |
|-----------|---------|-------------|
| **Relay** | ❌ Blind | Форвардит зашифрованные сообщения, не видит контент. Видит routing metadata (from/to/size) |
| **Peer** | ✅ Mutual auth | Ed25519 подпись проверяется обоюдно. Импульс-листинг не защищает от bad actor (будущее: репутация) |
| **DHT** | ⚠️ Trust-on-first-use (TOFU) | Нет консенсуса. Peer ID хранится локально |

### Adversary Capabilities

| Уровень | Атакующий | Может | Не может |
|---------|-----------|-------|----------|
| L0 | Relay оператор | Видеть, что A↔B общаются, размер сообщений | Расшифровать payload, подменить сообщение |
| L1 | MITM при handshake | Перехватить HELLO | Сфальсифицировать подпись (Ed25519) |
| L2 | Malicious peer | Прислать подписанное сообщение | Представиться другим DID (не подделает подпись) |
| L3 | Quantum adversary | 🚫 Теоретически взломать Ed25519/X25519 | PQC не реализован |

### Forward Secrecy (PFS)

**Обеспечивается:** X25519 эфемерные ключи на каждую сессию. Даже если долговременный Ed25519 ключ скомпрометирован — прошлые сессии не расшифровать.

## Rate Limiting

- **SigGate:** 10 msg/s/peer (по умолчанию, конфигурируется)
- **Relay TCP:** переменные среды `RELAY_MAX_MSGS_PER_SEC=10`, `RELAY_MAX_CONN_PER_IP=5`
- **Payload limit:** 1 MB (конфигурируется)

## Known Security Gaps (v0.5.0)

| Проблема | Статус | План |
|----------|--------|------|
| **No certificate pinning** | ⚠️ TOFU в DHT | Будущее: Nostr NIP-05/IOT адаптация |
| **No post-quantum crypto** | ❌ Не реализовано | Гипотетически: X25519MLKEM768 (2026+) |
| **No reputation system** | ❌ Не реализовано | Фаза 4 PHASES.md — reputation oracle |
| **No audit logging** | ⚠️ WAL не зашифрован | В production: шифрование WAL |
| **DHT poisoning** | ⚠️ Любой peer может публиковать любой DID | Обсуждается: attestation chain |

## Secure Deployment Checklist

```bash
# Production relay
export RELAY_MAX_MSGS_PER_SEC=10
export RELAY_MAX_CONN_PER_IP=5
export RELAY_MAX_PAYLOAD=1048576
export RELAY_TIMEOUT_SECONDS=30

# Agent
export AGENT_RATE_LIMIT=10
export AGENT_WAL_PATH=/var/lib/agent/wal.db
```
