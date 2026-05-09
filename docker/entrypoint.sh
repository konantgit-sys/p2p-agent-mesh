#!/bin/bash
# Entrypoint для agent-контейнеров в docker-compose
# Ждёт IPFS relay, подключается, запускает агент
set -e

RELAY_HOST="${RELAY_HOST:-ipfs-bootstrap}"
RELAY_PORT="${RELAY_PORT:-4001}"

echo "=== Agent: $AGENT_ID ==="
echo "Relay: $RELAY_HOST:$RELAY_PORT"

# Ждём relay
for i in $(seq 1 15); do
  if curl -s --max-time 2 "http://${RELAY_HOST}:5001/api/v0/id" -X POST > /dev/null 2>&1; then
    echo "Relay ready (attempt $i)"
    break
  fi
  echo "Waiting for relay... ($i)"
  sleep 2
done

# Получаем peer ID relay-ноды
PEER_ID=$(curl -s --max-time 3 "http://${RELAY_HOST}:5001/api/v0/id" -X POST | python3 -c "import sys,json; print(json.load(sys.stdin)['ID'])" 2>/dev/null)
if [ -z "$PEER_ID" ]; then
  echo "ERROR: Cannot get relay peer ID"
  exit 1
fi
echo "Relay PeerID: $PEER_ID"

# Инициализируем свой IPFS
BOOTSTRAP="/dns4/${RELAY_HOST}/tcp/${RELAY_PORT}/p2p/${PEER_ID}"
ipfs init --profile test 2>/dev/null
ipfs config --json Bootstrap "[\"${BOOTSTRAP}\"]"
ipfs config --json Experimental.Libp2pStreamMounting true
ipfs config --json Addresses.Gateway '""'

# Стартуем IPFS daemon
ipfs daemon --enable-pubsub-experiment &
sleep 5

echo "Peers connected:"
ipfs swarm peers 2>/dev/null | wc -l

# Запускаем Python-агента
exec python3 -c "
import asyncio, os, sys
sys.path.insert(0, '/app')
from sdk.agent import AgentMesh

AGENT_ID = os.environ.get('AGENT_ID', 'agent')
CAP = os.environ.get('AGENT_CAP', 'ping')

async def run():
    a = AgentMesh(AGENT_ID, [CAP])
    await a.start()
    print(f'Agent {AGENT_ID} started: {a.did}')
    
    if os.environ.get('MODE') == 'publisher':
        await a.emit(CAP, {'msg': f'hello from {AGENT_ID}', 'ts': asyncio.get_event_loop().time()})
        print('Published!')
    else:
        await a.listen({'capability': CAP}, lambda m: print(f'GOT: {m[\"payload\"]}'))
        print('Listening...')
    
    await asyncio.sleep(3600)

asyncio.run(run())
"
