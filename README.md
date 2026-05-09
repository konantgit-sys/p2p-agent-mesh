# P2P Agent Mesh

**Децентрализованный pub/sub транспорт для AI-агентов.**

IPFS PubSub (gossipsub) + Ed25519 подписи + SQLite WAL + DHT discovery.
AP модель (availability + partition tolerance). Eventual consistency.
Никакого центрального брокера.

```
┌──────────┐         IPFS PubSub (gossipsub + DHT + lazy-relay)      ┌──────────┐
│ Agent A  │ ◄─────────────────────────────────────────────────────► │ Agent B  │
│ [emit]   │                                                         │ [listen] │
└──────────┘                                                         └──────────┘
     ▲                                                                     ▲
     │                                                                     │
     └──── P2P Mesh ────┬──────────┐                                      │
                        │ Agent C  │ ◄────────────────────────────────────┘
                        │ [query]  │
                        └──────────┘
```

> ⚠️ **AP mesh, eventual consistency.** Не для строгого ordering или финансовых расчётов.
> Для сценариев: A2A сигналы между AI-агентами, IoT телеметрия, event-driven координация.
> Для ordered streaming — Kafka/RabbitMQ.

## Status — v0.2.0

| Компонент | Статус | Тесты |
|-----------|--------|-------|
| **Phase 0 — Core Transport** (IPFS PubSub, WAL, Ed25519, SigGate, DHT) | ✅ v0.2 | 24/24 |
| **Phase 1 — Agent SDK** (emit/listen/query/request, DHT discovery) | ✅ v0.2 | 5/5 |
| **LangGraph adapter** (MeshTopic, MeshStateSync, MeshRPC) | ✅ v0.2 | 3/3 |
| **ИТОГО** | **32/32 тестов** | |

## Quickstart

```bash
git clone https://github.com/snin-ai/p2p-agent-mesh
cd p2p-agent-mesh

# Требуется: запущенный IPFS daemon с --enable-pubsub-experiment
# См. QUICKSTART.md для Docker Compose

pip install cryptography
python examples/3_agent_langgraph.py
```

Полная инструкция: [QUICKSTART.md](QUICKSTART.md) (Docker Compose, bare metal, troubleshooting).

## Live Demo

```bash
# Терминал 1 — слушатель
python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('listener', ['echo'])
    await a.start()
    print(f'DID: {a.did}')
    await a.listen({'capability': 'echo'}, lambda m: print(f'GOT: {m[\"payload\"]}'))
    await asyncio.sleep(3600)
asyncio.run(run())
"

# Терминал 2 — публикатор
python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('publisher', ['echo'])
    await a.start()
    await a.emit('echo', {'msg': 'hello mesh'})
    await asyncio.sleep(1)
asyncio.run(run())
"
```

## Architecture

| Слой | Файл | Описание |
|------|------|---------|
| **Transport** | `phase0/transport.py` | IPFS PubSub через asyncio subprocess |
| **WAL** | `phase0/wal.py` | SQLite Write-Ahead Log (буфер, replay, prune) |
| **Identity** | `phase0/identity.py` | Ed25519 подпись, DID (`did:snin:*`) |
| **SigGate** | `phase0/sig_gate.py` | Верификация подписей, rate limit 10/msg/s |
| **DHT** | `phase0/dht.py` | Key-value кэш через pubsub топик `_dht` |
| **Agent SDK** | `sdk/agent.py` | `emit()` / `listen()` / `query()` / `request()` |
| **LangGraph** | `adapters/langgraph_channel.py` | MeshTopic, MeshStateSync, MeshRPC |

## Протокол

```json
{
  "type": "event",
  "topic": "agent:crypto_analysis",
  "capability": "crypto_analysis",
  "from": "did:snin:forecaster_v2",
  "payload": {"signal": "BUY", "coin": "BTC", "confidence": 0.85},
  "ts": 1715293200.123,
  "signature": "<ed25519_hex>",
  "pubkey": "<ed25519_pub_hex>"
}
```

## Metrics (localhost)

| Метрика | Значение |
|---------|----------|
| Latency p50 | 35ms |
| Latency p99 | 96ms |
| Throughput | ~20 msg/s (cli subprocess) |
| WAL replay | <100ms for 1000 msgs |
| Peers in network | 22 (в IPFS DHT) |

## Тесты

```bash
# Все тесты
python -m pytest phase0/ phase1/ -v

# Agent SDK
python -m pytest phase1/test_agent.py -v

# LangGraph адаптер
python -m pytest phase1/test_langgraph.py -v
```

## GTM

**v0.2.0** — 32/32 тестов, живая демка за 5 минут.

Для пилотов:
1. `QUICKSTART.md` — Docker Compose, запуск 3 агентов
2. `examples/3_agent_langgraph.py` — готовый к запуску
3. `BENCHMARKS.md` — таблица с результатами

## License

MIT
