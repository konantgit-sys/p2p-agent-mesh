# Phase 1 — Agent SDK Specification
# P2P Agent Mesh — высокоуровневый API для AI-агентов

## Цель
Агент (Cryter, Forecaster, Creator) может `emit()` событие, `listen()` на события других агентов по capability, `query()` по DHT, `request()` с ответом. Сообщения подписаны, WAL буферизирует, reconnect replay.

## Компоненты

| Файл | API |
|------|-----|
| `sdk/agent.py` | AgentMesh — основной класс |
| `phase1/SPEC.md` | Эта спецификация |
| `phase1/test_agent.py` | Интеграционные тесты (реальный IPFS) |

## API Surface

```python
class AgentMesh:
    async def start(self) -> str
    async def emit(self, capability: str, payload: dict) -> str         # msg_id
    async def listen(self, filter: dict, callback: Callable) -> Subscription
    async def query(self, capability: str, min_rep: float = 0.0) -> list[dict]
    async def request(self, target: str, payload: dict, timeout=30) -> dict|None
    async def sync_on_reconnect(self) -> int                             # кол-во догнанных
    async def stop(self)
    def status(self) -> dict
```

### Subscription
```python
class Subscription:
    filter: dict           # {"capability": "forecast", "min_reputation": 0.7}
    callback: Callable
    async def cancel(self) # отписаться
```

## Протокол

```json
// emit("crypto_analysis", {"signal": "BUY", "coin": "BTC", "confidence": 0.85})
{
  "id": "a1b2c3d4e5f6g7h8",
  "type": "event",
  "topic": "agent:crypto_analysis",
  "capability": "crypto_analysis",
  "from": "did:snin:forecaster_v2",
  "ts": 1715293200.123,
  "payload": {"signal": "BUY", "coin": "BTC", "confidence": 0.85},
  "signature": "<ed25519_hex>",
  "pubkey": "<ed25519_pub_hex>"
}
```

## Capability routing

- Agent публикует в топик `agent:{capability}`
- Подписка может быть:
  - `{"capability": "crypto_analysis"}` — точное совпадение
  - `{"capability": ["crypto_analysis", "forecast"]}` — любой из списка
  - `{"capabilities": ["crypto_analysis"]}` — альтернативный ключ (Legacy)
- При получении сообщения: проверка подписи → sig_gate → match по filter → callback

## DHT registration

При `start()` агент публикует свой профиль в DHT:
```
key: "agent:{did}"
value: {"agent_id": "...", "capabilities": [...], "reputation": 0.9, "peer_id": "..."}
```

`query(capability)` ищет в локальном DHT-кэше по capability.

## Критерий готовности

- [ ] 2 агента в разных процессах: A emit("ping") → B listen("ping") получает
- [ ] Сообщение подписано и верифицировано
- [ ] WAL сохраняет emit, sync_on_reconnect догоняет пропущенные
- [ ] listen с filter по capability не получает сообщения других capability
- [ ] DHT: agent A start() → agent B query("ping") видит A
