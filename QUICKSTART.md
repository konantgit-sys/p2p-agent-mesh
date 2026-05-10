# P2P Agent Mesh — Quickstart

Запуск 3-узловой mesh-сети за минуту. Zero внешних зависимостей.

## Быстрый старт (все в одном процессе)

```bash
pip install cryptography
python -c "
import asyncio
from sdk.agent import AgentMesh

async def run():
    a = AgentMesh('agent_a', ['ping'])
    await a.start()
    print(f'Готов. DID: {a.did}')
    await a.stop()

asyncio.run(run())
"
```

Работает без IPFS, без Docker, без внешних сервисов.

## 2 агента (один процесс)

```bash
python -c "
import asyncio
from sdk.agent import AgentMesh

async def run():
    a = AgentMesh('publisher', ['ping'])
    b = AgentMesh('listener', ['listen'])
    await a.start()
    await b.start()

    def callback(msg):
        print(f'Получено: {msg[\"payload\"]}')

    await b.listen({'capability': 'ping'}, callback)
    await asyncio.sleep(0.1)
    await a.emit('ping', {'msg': 'hello from publisher!'})
    await asyncio.sleep(0.3)
    await a.stop()
    await b.stop()

asyncio.run(run())
"
```

## 🌐 Multi-container / Multi-machine (TCP transport)

Zero dependencies. Works across Docker containers, VMs, or LAN.

**Terminal 1 (Listener)**
```bash
python -c "
import asyncio
from sdk.agent import AgentMesh

async def run():
    a = AgentMesh('listener', ['ping'])
    await a.start()
    await a.listen({'capability': 'ping'}, lambda m: print(f'GOT: {m[\"payload\"]}'))
    await asyncio.sleep(3600)

asyncio.run(run())
"
```

**Terminal 2 (Publisher)**
```bash
python -c "
import asyncio
from sdk.agent import AgentMesh
# замени 127.0.0.1 на IP listener'а и реальный port (из лога listener: TCP: 127.0.0.1:XXXXX)
a = AgentMesh('publisher', ['ping'],
              bootstrap_peers=['listener@127.0.0.1:XXXXX'])
async def run():
    await a.start()
    await a.emit('ping', {'msg': 'hello via tcp'})
    await asyncio.sleep(2)

asyncio.run(run())
"
```

💡 **Tip:** Replace `127.0.0.1` with actual LAN/VPC IP for cross-machine.

## Демо: 3-агентная mesh

```python
# examples/3_agent_signal_mesh.py
import asyncio
from sdk.agent import AgentMesh

async def main():
    # Создаём 3 агента в одном процессе
    agent_a = AgentMesh("agent_a", ["signal"])
    agent_b = AgentMesh("agent_b", ["listen", "analysis"])
    agent_c = AgentMesh("agent_c", ["listen", "content"])

    await agent_a.start()
    await agent_b.start()
    await agent_c.start()

    # Подписка
    await agent_b.listen({"capability": "signal"}, lambda m: print(f"[B] {m['payload']}"))
    await agent_c.listen({"capability": "analysis"}, lambda m: print(f"[C] {m['payload']}"))

    await asyncio.sleep(0.1)

    # A отправляет signal → B получает
    await agent_a.emit("signal", {"action": "BUY", "conf": 0.85})
    await asyncio.sleep(0.3)

    await agent_a.stop()
    await agent_b.stop()
    await agent_c.stop()

asyncio.run(main())
```

## Под капотом

```
P2PTransport
├── in-memory _bus          ← same-process (Python dict + asyncio.Lock)
└── TCP слой                ← cross-machine (asyncio.start_server)
    ├── JSON lines протокол
    ├── base64 payload
    ├── exponential backoff reconnect (1→30s)
    └── zero external deps
```

### Известные ограничения v0.3
- 🔓 Нет шифрования трафика (trusted networks / VPC)
- 🌐 Нет NAT traversal (требуется прямой IP:port)
- 📦 Base64 payload ~33% overhead (msgpack/protobuf в v0.4)
- 🔁 Дедупликация на стороне клиента (msg_id cache)

## Проверка

```python
# test_connection.py
import asyncio
from sdk.agent import AgentMesh

async def test():
    a = AgentMesh("test_node", ["ping"])
    await a.start()
    print(f"✅ Started, DID: {a.did[:24]}...")
    peers = await a.transport.peers()
    print(f"✅ Peers: {len(peers)}")
    status = a.status()
    print(f"✅ WAL messages: {status['wal_count']}")
    await a.stop()

asyncio.run(test())
```

## Troubleshooting

| Проблема | Причина | Решение |
|----------|---------|---------|
| TCP пир не виден | bootstrap_peers не указан в конструкторе | Добавить `bootstrap_peers=['node_id@host:port']` |
| Сообщение не получено | Разные топики / разные capability | Проверить что `emit()` и `listen()` используют одно capability |
| DHT пустой | Late-joiner не получил метаданные | Реализован auto-republish при получении DHT-сообщения |
| Connection refused | Пир ещё не запущен | Exponential backoff retry (1→30 сек) |
