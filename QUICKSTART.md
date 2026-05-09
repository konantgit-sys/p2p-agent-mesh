# P2P Agent Mesh — Quickstart

Запуск 3-узловой mesh-сети на одном сервере за 5 минут.

> ⚠️ Docker Compose tested on: Linux (Ubuntu 22.04), macOS (Docker Desktop 4.25+).
> Если возникают проблемы — используй bare-metal вариант ниже и сообщи окружение в issues.

## Требования
- Docker + docker-compose
- Или Python 3.11+ и запущенный IPFS daemon

## Вариант A: Docker Compose (рекомендуется)

```yaml
# docker-compose.yml
version: "3.9"
services:
  ipfs:
    image: ipfs/kubo:latest
    command: daemon --enable-pubsub-experiment --routing=dht
    environment:
      - IPFS_PROFILE=server
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
      - "5001:5001"
      - "8080:8080"
    volumes:
      - ipfs_data:/data/ipfs

  agent-forecaster:
    build: .
    depends_on: [ipfs]
    command: python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('forecaster', ['forecast', 'signal'])
    await a.start()
    print(f'forecaster ready: {a.did}')
    await asyncio.sleep(3600)
asyncio.run(run())
"
    environment:
      - IPFS_PATH=/data/ipfs
      - P2P_MESH_DB=/data/p2p_mesh.db
    volumes:
      - ./:/app

  agent-cryter:
    build: .
    depends_on: [ipfs]
    command: python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('cryter', ['listen', 'analysis'])
    await a.start()
    await a.listen({'capability': 'signal'}, lambda m: print(f'cryter got: {m[\"payload\"]}'))
    await asyncio.sleep(3600)
asyncio.run(run())
"
    environment:
      - IPFS_PATH=/data/ipfs
      - P2P_MESH_DB=/data/p2p_cryter.db
    volumes:
      - ./:/app

  agent-creator:
    build: .
    depends_on: [ipfs]
    command: python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('creator', ['content'])
    await a.start()
    await a.listen({'capability': 'analysis'}, lambda m: print(f'creator got: {m[\"payload\"]}'))
    await asyncio.sleep(3600)
asyncio.run(run())
"
    environment:
      - IPFS_PATH=/data/ipfs
      - P2P_MESH_DB=/data/p2p_creator.db
    volumes:
      - ./:/app

volumes:
  ipfs_data:
```

```dockerfile
# Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
```

```bash
# requirements.txt
# (Библиотек не требуется — только стандартная + cryptography)
cryptography>=41.0.0
```

**Запуск:**
```bash
docker compose up -d
docker compose logs -f agent-cryter
# В другом терминале:
docker compose exec agent-forecaster python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('cli', ['test'])
    await a.start()
    await a.emit('signal', {'coin': 'BTC', 'action': 'BUY', 'conf': 0.85})
    await asyncio.sleep(1)
asyncio.run(run())
"
# В логах cryter: 'cryter got: {"coin": "BTC", "action": "BUY", "conf": 0.85}'
```

## Вариант B: Bare metal (3 команды)

```bash
# 1. Установить IPFS (kubo v0.29+)
wget -qO- https://dist.ipfs.tech/kubo/v0.29.0/kubo_v0.29.0_linux-amd64.tar.gz | tar xz
cd kubo && sudo bash install.sh && cd .. && rm -rf kubo
ipfs init
ipfs config --json Experimental.Libp2pStreamMounting true
ipfs config --json Addresses.Gateway '""'  # если порт 8080 занят

# 2. Запустить демон
ipfs daemon --enable-pubsub-experiment &

# 3. Установить зависимости
pip install cryptography

# 4. Запустить 2 агента
# Терминал 1:
python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('listener', ['echo'])
    await a.start()
    print(f'Слушаю, DID: {a.did}')
    await a.listen({'capability': 'echo'}, lambda m: print(f'Получено: {m[\"payload\"]}'))
    await asyncio.sleep(3600)
asyncio.run(run())
"

# Терминал 2:
python -c "
import asyncio
from sdk.agent import AgentMesh
async def run():
    a = AgentMesh('publisher', ['echo'])
    await a.start()
    await a.emit('echo', {'msg': 'hello from phase1!'})
    print('Отправлено')
    await asyncio.sleep(1)
asyncio.run(run())
"
```

## Проверка

```python
# test_connection.py
import asyncio
from sdk.agent import AgentMesh

async def test():
    a = AgentMesh("test_node", ["ping"])
    await a.start()
    print(f"✅ IPFS connected, peer_id: {a.transport.peer_id[:20]}...")
    print(f"✅ DID: {a.did}")
    peers = await a.transport.peers()
    print(f"✅ Peers in network: {len(peers)}")
    await a.stop()

asyncio.run(test())
```
```bash
python test_connection.py
# Output:
# ✅ IPFS connected, peer_id: 12D3KooWFzSEd...
# ✅ DID: did:snin:...
# ✅ Peers in network: 22
```

## Troubleshooting

| Проблема | Причина | Решение |
|----------|---------|---------|
| `IPFS daemon not running` | IPFS не запущен | `ipfs daemon --enable-pubsub-experiment &` |
| Сообщение не получено | Gossipsub не успел разослать | Увеличь sleep до 3-5 сек после publish |
| DHT пустой | Late-joiner не получил метаданные | Реализован auto-republish при получении любого DHT-сообщения |
| WAL не replay | Нет newline в publish | transport.py добавляет `\n` автоматически |
| `Event loop is closed` (warning) | cleanup pytest-asyncio | Безвредно, не влияет на работу |
