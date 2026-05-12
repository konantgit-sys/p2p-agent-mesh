# 🔌 Relay Status — v0.5.0

| Endpoint | Status | Заметки |
|----------|--------|---------|
| `mesh-relay.v2.site` | ✅ **Активен** | HTTP relay, Flask, port 9905 |
| `/api/stats` | ✅ `{"agents":0,"pending":0,"uptime":N}` | |
| `/api/peers` | ✅ Пусто (нет агентов) | |
| `/api/messages` | ✅ Пусто | |
| `/api/register` | ✅ POST для агентов | |
| `/api/send` | ✅ POST для сообщений | |

## Статус

```
mesh-relay.v2.site → full:9905 → Flask HTTP Relay
  └── Поддомен: ✅ mesh-relay.v2.site
  └── Backend: ✅ RelayServer (Flask) port 9905
  └── Agents: 0
  └── PID: активен
```
