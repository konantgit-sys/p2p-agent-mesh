# Phase 0 — v0.4.0 Secure Mesh Specification

## Цель
Добавить сквозное шифрование (E2E) поверх TCP транспорта.
Агенты аутентифицируют друг друга через Ed25519 (уже есть),
обмениваются эфемерными X25519 ключами, шифруют все сообщения ChaCha20-Poly1305.

## Архитектура

```
TCP connect → Handshake (mutual auth + key exchange) → Encrypted pub/sub
```

## Handshake протокол (3 сообщения)

```
Client                                    Server
  │                                          │
  │  {"type":"hello",                        │
  │   "pubkey":"<ed25519_pub_hex>",          │
  │   "nonce":"<32_bytes_hex>",              │
  │   "eph_pub":"<x25519_pub_hex>"}          │
  │ ─────────────────────────────────────►   │
  │                                          │
  │          {"type":"challenge",            │
  │           "server_pubkey":"<ed25519>",   │
  │           "nonce":"<32_hex>",            │
  │           "nonce_sig":"<ed25519_sig>",   │
  │           "eph_pub":"<x25519_pub>"}      │
  │ ◄─────────────────────────────────────   │
  │                                          │
  │  Verify server signed our nonce ✓        │
  │  Derive session key (ECDH + HKDF)        │
  │                                          │
  │  {"type":"auth",                         │
  │   "nonce_sig":"<ed25519_sig>"}           │
  │ ─────────────────────────────────────►   │
  │                                          │
  │  Verify client signed our nonce ✓        │
  │  Derive session key (ECDH + HKDF)        │
```

## Encryption

После handshake все сообщения оборачиваются в ChaCha20-Poly1305:

```json
{
  "type": "enc",
  "nonce": "<12_bytes_hex>",
  "ct": "<ciphertext_hex>"
}
```

## Config (env)

| Variable | Default | Description |
|----------|---------|-------------|
| `MESH_TLS` | `0` | Включить шифрование (1/0) |
| `MESH_PORT` | `0` | Порт TCP сервера (0 = random) |

## Файлы

| Файл | Что меняем |
|------|-----------|
| `phase0/handshake.py` | **NEW** — handshake + encryption |
| `phase0/config.py` | **NEW** — настройки |
| `phase0/transport.py` | **EDIT** — интеграция handshake |
| `phase0/test_tls_transport.py` | **NEW** — 4 теста |
| `CHANGELOG.md` | **EDIT** |
| `README.md` | **EDIT** |

## Критерий готовности

- [ ] Handshake: два пира аутентифицировались за 3 сообщения
- [ ] Encrypted pub/sub: сообщения не читаются в plaintext
- [ ] Reject: wrong pubkey → соединение отклоняется
- [ ] Backward compat: MESH_TLS=0 работает как v0.3.1
- [ ] Все 40 тестов проходят
