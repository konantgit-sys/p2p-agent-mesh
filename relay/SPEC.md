# Phase 0 — v0.4.1 NAT Traversal via Relay

## Цель
Агенты за NAT/NAT могут общаться через публичный relay-узел.
Relay не видит содержимое сообщений (сквозное шифрование между агентами).

## Архитектура

```
┌──────────┐     TLS (handshake.py)     ┌──────────┐
│ Agent A  │◄══════════════════════════►│  Relay   │
│ (за NAT) │        encrypted           │  Node    │
└──────────┘         ↓                  │ *.v2.site│
                     │                  └────┬─────┘
┌──────────┐         │                       │
│ Agent B  │◄════════════════════════════════╝
│ (за NAT) │     TLS (handshake.py)
└──────────┘

Relay: форвардит encrypted blobs, не расшифровывает
E2E: X25519 ECDH через relay (совпадает с handshake.py)
```

## Протокол (6 типов сообщений)

Все сообщения между агентом и relay зашифрованы session key-ом от handshake.py.
Relay видит только `target`/`from` pubkeys (публичная информация).

### 1. REGISTER — агент → relay
```json
{"type":"register","pubkey":"<ed25519_hex>","capabilities":["cap1","cap2"]}
```
→ relay запоминает `pubkey → (session, capabilities)`

### 2. REGISTERED — relay → агент
```json
{"type":"registered","agent_id":"<pubkey_prefix>"}
```

### 3. PEERS — relay → агент (список подключённых)
```json
{"type":"peers","peers":[{"pubkey":"hex","capabilities":[...]}]}
```

### 4. E2E_INIT / E2E_REQ — установка E2E ключа
```
A → relay: {"type":"e2e_init","target":"<B_pubkey>","eph_pub":"<A_x25519_eph>"}
relay → B: {"type":"e2e_req","from":"<A_pubkey>","eph_pub":"<A_x25519_eph>"}
```

### 5. E2E_ACCEPT / E2E_READY — E2E ключ установлен
```
B → relay: {"type":"e2e_accept","target":"<A_pubkey>","eph_pub":"<B_x25519_eph>"}
relay → A: {"type":"e2e_ready","from":"<B_pubkey>","eph_pub":"<B_x25519_eph>"}
```

Оба агента независимо вычисляют: `ECDH(eph_priv, peer_eph_pub) → HKDF → session_key`

### 6. SEND / RECV — E2E зашифрованное сообщение
```
A → relay: {"type":"send","target":"<B_pubkey>","data":"<chaCha20_blob>"}
relay → B: {"type":"recv","from":"<A_pubkey>","data":"<chaCha20_blob>"}
```

## Реализация

### relay/server.py — TCP сервер
- asyncio.start_server
- handshake.py для аутентификации
- In-memory registry: `dict[pubkey_hex → AgentSession]`
- Forward: send → корректному пиру

### relay/client.py — клиентский модуль
- Подключается к relay, проходит handshake
- Регистрируется
- API: `register()`, `send(target, data)`, `on_recv(callback)`, `e2e_establish(target_pubkey)`
- Возвращает SecureSession для E2E общения

### Интеграция с P2PTransport (v0.5)
- Если `bootstrap_peers` не заданы и `relay_addr` задан → relay fallback
- В v0.4.1: relay/client.py как standalone, без изменения transport.py

## Файлы

| Файл | Описание |
|------|----------|
| `relay/SPEC.md` | Эта спецификация |
| `relay/server.py` | Relay сервер (~250 строк) |
| `relay/client.py` | Relay клиент (~200 строк) |
| `relay/test_relay.py` | Тесты (8+) |

## Критерий готовности

- [x] 2 агента через relay: A send → B recv (E2E encrypted)
- [x] Relay не может расшифровать data (проверить hex дампа)
- [x] E2E key exchange работает через relay
- [x] Relay registry: подключение → register → в списке peers
- [x] Отключение агента → relay удаляет из peers
- [x] 10 тестов
- [x] Rate limiting: max msgs/sec, max connections/IP, max payload (env vars)
- [x] IP connection limit: `RELAY_MAX_CONN_PER_IP` (default 5)

---

## Rate Limiting (v0.4.1-alpha)

Rate limiting опционален в этом релизе. Для публичного relay разверни с переменными окружения:

```bash
RELAY_MAX_MSGS_PER_SEC=10     # макс сообщений в секунду с одного агента
RELAY_MAX_CONN_PER_IP=5       # макс соединений с одного IP
RELAY_MAX_PAYLOAD=1048576     # макс размер payload (1 MB)
```

Без rate limit: агент может отправлять безлимитно → DoS relay.
Защита: sliding window (deque timestamp-ов), сброс каждую секунду.

> **v0.4.2**: adaptive throttling (exponential backoff), per-topic rate limits,
>  Prometheus метрики для мониторинга.

---

## Security Properties

| Свойство | Статус |
|----------|--------|
| E2E encryption (relay не видит контент) | ✅ ChaCha20-Poly1305 AEAD |
| Mutual authentication | ✅ Ed25519 challenge-response |
| Perfect Forward Secrecy | ✅ X25519 ephemeral keys per session |
| Replay protection | ✅ Sequence-numbered nonces |
| Rate limiting | ✅ Sliding window (env config) |
| IP connection limit | ✅ Per IP tracking |
| Metadata leakage | ⚠️ Relay видит `from→to` паттерны. Mixing/delay jitter — v0.5 |

## Known Limits (v0.4.1)

- 📦 Base64 payload encoding: ~33% overhead (оптимизация в v0.5 — msgpack)
- 🔁 Deduplication: client-side (msg_id cache)
- 🌐 No DHT discovery: агенты должны знать relay endpoint
- 👀 Metadata: relay видит кто с кем общается (from→to)
- 📊 No metrics: Prometheus/grafana — v0.4.2
