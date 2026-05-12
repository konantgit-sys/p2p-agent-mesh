# P2P Agent Mesh — Pilot Guide

Запуск 3-агентного пилота (Cryter → Forecaster → Creator) за 15 минут.

## Что нужно

- Python 3.10+ (проверить: `python3 --version`)
- Docker (опционально, для relay)

## Вариант A: Docker (рекомендуется)

```bash
# 1. Клонировать
git clone https://github.com/konantgit-sys/p2p-agent-mesh
cd p2p-agent-mesh

# 2. Запустить relay
docker-compose up -d relay
# Relay на localhost:9900

# 3. Установить зависимости
pip install -r requirements.txt

# 4. Запустить 3-агентный пилот
python examples/3_agent_langgraph.py
# → Cryter публикует сигнал
# → Forecaster анализирует
# → Creator генерирует контент
```

## Вариант B: Без Docker

```bash
# 1. Установить relay
pip install websockets flask requests

# 2. Запустить relay на порту 9900
python relay/server.py --port 9900 --host 0.0.0.0

# 3. В другом терминале — пилот
pip install -r requirements.txt
python examples/3_agent_langgraph.py
```

## Проверка что работает

```bash
# Relay жив?
curl http://localhost:9900/status
# → {"status":"ok","agents":3}

# Агенты общаются?
grep "Published\|Received\|Content:" examples/3_agent_langgraph.py
# → "[Cryter] Published signal: ..."
# → "[Forecaster] Received signal, published forecast"
# → "[Creator] Content: Signal: BTC sentiment..."
```

## Что внутри пилота

```
┌─────────┐     crypto_analysis     ┌───────────┐
│ Cryter  │ ──────────────────────► │ Forecaster│
└─────────┘                         └───────────┘
                                           │
                                     forecasting
                                           │
                                           ▼
                                    ┌──────────┐
                                    │  Creator  │
                                    └──────────┘
```

- **Cryter:** Публикует рыночный сигнал в топик `crypto_analysis`
- **Forecaster:** Подписан на `crypto_analysis` → публикует прогноз в `forecasting`
- **Creator:** Подписан на `forecasting` → создаёт контент

Всё через P2P mesh. Без Kafka. Без Redis. Без REST.

## SNIN DAO Pilot (расширенный)

```bash
python pilot/snin_dao_chain.py
```

Трёхсторонняя цепочка с полным циклом:
1. Cryter → рыночный сигнал
2. Forecaster → прогноз с метриками
3. Creator → генерация контента в стиле SNIN

Тесты: `pytest pilot/test_chain.py -v`

## DePIN Device (IoT прототип)

```bash
python depin/simulated_device.py
```

Эмулирует IoT-устройство:
- Пишет телеметрию в WAL (offline-first)
- Merkle-sync при reconnect
- Регистрация в DeviceRegistry

Документация: `docs/DEPIN_QUICKSTART.md`

## Troubleshooting

| Симптом | Решение |
|---------|---------|
| `Connection refused` на relay | Relay не запущен? `docker ps` или `ps aux | grep relay` |
| Агенты не видят друг друга | Relay разный? Проверьте `--relay-host` |
| WAL ошибки | `rm /tmp/*.db` — очистить тестовые базы |
| "No module named..." | `pip install -r requirements.txt` |
