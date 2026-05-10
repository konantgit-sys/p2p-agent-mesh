# Pilot Onboarding — Public Relay Test

## Quick Start (5 minutes)

### Requirements
- Python 3.10+
- `pip install -e .` в корне p2p-agent-mesh
- Две машины/контейнера за разными NAT

### Machine A
```bash
export MESH_RELAY=https://relay-mesh.v2.site
python -c "
from phase0.identity import Identity
from relay.http_client import HTTPRelayClient
import asyncio

async def run():
    ident = Identity()
    client = HTTPRelayClient(ident, '$MESH_RELAY')
    await client.register()
    print(f'✓ Registered: {ident.public_key_hex[:16]}...')
    
    for _ in range(10):
        peers = await client.peers()
        if peers:
            print(f'✓ Peers found: {len(peers)}')
            break
        await asyncio.sleep(1)
    
    if peers:
        target = peers[0]['pubkey']
        await client.send(target, b'hello across NAT 🔐')
        print('✓ Sent E2E message')

asyncio.run(run())
"
```

### Machine B
Тот же скрипт. Если оба запустить в течение ~30 секунд:
1. Увидят друг друга в `peers()`
2. Обменяются E2E зашифрованными сообщениями
3. Relay логи — только `timestamp, peer_id, msg_type, size`

⚠️ **Security note**: Public relay rate-limited (10 msg/s, 1 MB payload).
Для продакшена — свой relay (см. `RELAY_DEPLOY.md`).

---

## 72-Hour Monitoring

```bash
# relay-monitor.sh
watch -n 5 '
echo "=== $(date) ==="
echo "Active connections: $(ss -tn state established | grep :8443 | wc -l)"
echo "Registered peers (last hour): $(grep REGISTERED /var/log/relay.log | tail -100 | wc -l)"
echo "Messages forwarded: $(grep "type\":\"SEND" /var/log/relay.log | tail -1000 | wc -l)"
echo "Rate limit triggers: $(grep "rate_limited" /var/log/relay.log | tail -100 | wc -l)"
echo "Memory: $(ps -o rss= -p $(pgrep -f relay.server) | awk "{print \$1/1024 \" MB\"}")"
'
```

### Target Metrics (first 72h)

| Метрика | Цель | Тревога если |
|---------|------|-------------|
| `pip install` установок | >20 | QUICKSTART.md битый |
| Уникальных pubkey на relay | >5 | Нет аутрича |
| Первое E2E сообщение снаружи | 1 за 24ч | Написать лично в 3 репо |
| Rate limit triggers | <5% трафика | Нормально, если тест нагрузки |
| Memory growth | Flat <50 MB | Утечка — чистка deque |
