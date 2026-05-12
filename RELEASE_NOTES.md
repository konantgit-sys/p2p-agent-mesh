# P2P Agent Mesh — v0.5.0 Release Notes

## 🎯 Что это

**Децентрализованная mesh-сеть для AI-агентов.** Без Kafka, без Redis, без блокчейна.

P2P Agent Mesh даёт агентам то, что нужно для координации:
- **topic-based pub/sub** — агенты общаются по топикам (capability-based routing)
- **E2E encryption** — X25519 + ChaCha20-Poly1305, PFS (Perfect Forward Secrecy)
- **NAT traversal** — blind relay ничего не знает о контенте
- **Durable delivery** — SQLite WAL, offline-first, Merkle-sync при reconnect
- **Exactly-once ordering** — Micro-Raft + Consumer Groups (opt-in, плата за CP)
- **Готовые адаптеры** — LangGraph, CrewAI, AutoGen

## 🚀 Что нового в v0.5.0

**Новые компоненты:**
- **Coordination Layer** (`coordination/`): Micro-Raft consensus, Consumer Groups, Exactly-once Dedup
- 37 новых тестов на coordination, 133 всего

**Улучшения:**
- DePIN SDK: Merkle Sync (99% экономия трафика), Device Registry, Device Pipeline
- SNIN DAO Pilot: 3-агентная цепочка Cryter → Forecaster → Creator
- Docker deploy: мультистейдж, non-root, healthcheck
- CLI: флаги `--port`, `--host`, `--relay`
- Все адаптеры (LangGraph, CrewAI, AutoGen) — 58 тестов

**Безопасность:**
- Аудит nonce (sequence-based + новый ECDH ключ на сессию) ✅
- pip-audit: 0 уязвимостей
- ruff линтер: 0 ошибок

## 📊 Цифры

| Метрика | Значение |
|---------|----------|
| Тесты | 133 passed, 93s |
| Покрытие кода | ~72% (оценка) |
| Линтер | ruff 0 errors |
| Формат | ruff format — единый стиль |
| pip-audit | 0 known vulnerabilities |
| Размер Docker образа | <80 MB (multi-stage) |

## 📖 Документация

- [QUICKSTART.md](docs/QUICKSTART.md) — запуск за 5 минут
- [SECURITY_MODEL.md](docs/SECURITY_MODEL.md) — модель угроз
- [PILOT_GUIDE.md](docs/PILOT_GUIDE.md) — пилот за 15 минут
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — полная схема
- [PHASES.md](docs/PHASES.md) — roadmap и статус

## Known Limits

- AP by default; CP via Coordination Layer (opt-in, требует большинства)
- Relay видит routing metadata (from/to/size); payload E2E encrypted
- Rate limiting: 10 msg/s/peer (configurable через env)
- ESP32 MicroPython — прототип готов, требует железки для теста
- C SDK — не начат

## Благодарности

- Сообществу Nostr (NIP-44, relay протокол) — за вдохновение
- Команде SNIN — за тестирование и обратную связь

---

*Дата релиза: 2026-05-12*
*SHA: будет при пуше*
