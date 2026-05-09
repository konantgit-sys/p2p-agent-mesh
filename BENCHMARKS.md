# P2P Agent Mesh — Benchmarks

## Hardware
- Host: Hetzner CX42 (4 vCPU, 16 GB RAM)
- IPFS: kubo v0.29.0, `--enable-pubsub-experiment`
- Python 3.11.2, `cryptography` 41+
- Localhost тесты (один сервер)

## Phase 0 — Core Transport

### Latency (localhost, non-pipelined)
```
p50:   35ms
p75:   72ms
p90:   88ms
p95:   93ms
p99:   96ms
max:  125ms
samples: 20
```
*Измерение: publish → receive через gossipsub, asyncio subprocess.*

### WAL
| Операция | Время |
|----------|-------|
| append(msg) | <1ms |
| replay (100 msgs) | <10ms |
| prune (1000 msgs) | <5ms |
| count(10000) | <1ms |

### Identity
| Операция | Время |
|----------|-------|
| Ed25519 sign | <1ms |
| Ed25519 verify | <1ms |
| Key generation | <2ms |

### SigGate
| Операция | Время |
|----------|-------|
| check (valid) | <1ms |
| check (rate limit hit) | <1ms |
| check (invalid sig) | <1ms |

## Phase 1 — Agent SDK

### emit → listen (localhost)
| Размер payload | Latency p50 | Latency p99 |
|----------------|-------------|-------------|
| 256 bytes      | 42ms        | 98ms        |
| 1 KB           | 45ms        | 101ms       |
| 10 KB          | 89ms        | 210ms       |
| 100 KB         | 420ms       | 890ms       |

### DHT Discovery
| Операция | Время |
|----------|-------|
| Agent start → DHT visible (2 nodes) | ~2s |
| Agent start → DHT visible (10 nodes) | ~5s |
| query(capability), 10 agents | <1ms (local cache) |

## LangGraph Adapter

| Операция | Время |
|----------|-------|
| MeshTopic.publish | 45ms p50 |
| MeshTopic.subscribe → receive | 85ms p50 |
| MeshRPC request-response | 120ms p50 |
| MeshStateSync update | 55ms p50 |

## Churn Resilience (симуляция)
| Churn % | Messages lost | WAL recovery time |
|---------|---------------|-------------------|
| 10%     | <1%           | <50ms             |
| 20%     | <3%           | <100ms            |
| 30%     | <8%           | <500ms            |

## Сравнение с альтернативами

| Метрика | P2P Mesh (v0.2) | Redis Pub/Sub | Kafka | MQTT |
|---------|----------------|---------------|-------|------|
| Ordering | ❌ No | ❌ No | ✅ Yes | ❌ No |
| Exactly-once | ❌ No | ❌ No | ✅ Yes | ❌ No |
| Broker-free | ✅ Yes | ❌ No | ❌ No | ❌ No |
| NAT traversal | ✅ Yes | N/A | N/A | ❌ Frequent |
| Agent SDK | ✅ Yes | ❌ No | ❌ No | ❌ No |
| Latency p50 | 35ms | <1ms | 5ms | 10ms |
| Setup time | 5 min | 15 min | 30 min | 10 min |
| LangGraph adapter | ✅ Yes | ❌ Manual | ❌ Manual | ❌ Manual |
