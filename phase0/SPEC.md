# Phase 0 — Core Transport Specification
# P2P Agent Mesh — реальный IPFS PubSub бэкенд

## Цель
2 узла обмениваются сообщениями через IPFS PubSub без сервера.
Сообщения подписаны Ed25519, WAL буферизирует при offline,
sig gate режектит неподписанные, DHT хранит K=3 реплики.

## Компоненты

| Файл | Что делает | API |
|------|-----------|-----|
| `transport.py` | Обёртка над `ipfs pubsub pub/sub` через asyncio subprocess | publish(topic, data), subscribe(topic, cb), peers() |
| `wal.py` | SQLite WAL — буфер всех отправленных/полученных сообщений | append(msg), replay(topic, since_id), prune(before_ts) |
| `dht.py` | Key-value хранилище поверх IPFS PubSub (DHT топики) | put(key, val, ttl), get(key), replicate(k=3) |
| `identity.py` | Ed25519 ключ + подпись + DID | generate(), sign(data), verify(sig, data, pubkey) |
| `sig_gate.py` | Верификация подписей + rate limit + allowlist | check(msg), reject(reason) |

## Протокол сообщения

```json
{
  "id": "<hash(sender+ts+payload)[:16]>",
  "type": "event | request | response | ack",
  "topic": "agent:crypto_analysis",
  "from": "did:snin:cryter_v10",
  "ts": 1715293200.123,
  "payload": { ... },
  "signature": "<ed25519_hex>",
  "pubkey": "<ed25519_pub_hex>"
}
```

## Критерий готовности

- [ ] 2 процесса на одной машине обмениваются сообщениями через IPFS PubSub
- [ ] WAL сохраняет все сообщения, replay восстанавливает после offline
- [ ] Подпись + верификация Ed25519
- [ ] Sig gate режектит сообщения без подписи
- [ ] DHT: put('agent:cryter', peer_id) → get('agent:cryter') → peer_id
- [ ] latency publish→receive < 200ms (localhost)
