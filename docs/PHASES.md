# P2P Agent Mesh — Phases & File Map

Каждый этап = spec.md + impl.py + test.py + пример.
Никакой абстрактной архитектуры — только файлы которые реально пишутся.

═══════════════════════════════════════════════════════════════
PHASE 0 — CORE TRANSPORT (ядро)
═══════════════════════════════════════════════════════════════

  Цель: 2 узла в mesh обмениваются сообщениями без сервера

  Файлы:

  00_core/
  ├── SPEC.md          # CAP-модель, протокол, интерфейсы
  ├── transport.py     # IPFS PubSub / gossipsub бэкенд
  ├── wal.py           # Write-Ahead Log (SQLite буфер)
  ├── dht.py           # Kademlia DHT (put/get, K=3 реплика)
  ├── identity.py      # DID + ECDSA подпись сообщений
  ├── sig_gate.py      # Верификация каждой подписи, rate limit
  ├── __init__.py
  ├── test_transport.py  # 2 узла → publish → receive
  ├── test_wal.py        # offline → WAL → reconnect → sync
  └── test_sig_gate.py   # подпись + верификация + reject spam

  Критерий готовности: 2 узла обмениваются сообщениями через mesh,
  WAL буферизирует при offline, DHT хранит K=3 реплики,
  sig gate режектит неподписанные сообщения.

═══════════════════════════════════════════════════════════════
PHASE 1 — AGENT SDK
═══════════════════════════════════════════════════════════════

  Цель: агент может emit() событие, listen() по capability,
  query_agents() по навыку

  Файлы:

  01_agent_sdk/
  ├── SPEC.md          # Agent API, capability routing, reputation
  ├── agent_sdk.py     # AgentMesh — emit / listen / query / request
  ├── capability_registry.py  # DHT-хранение тегов + репутации
  ├── listener.py      # Подписка с фильтрацией по capability
  ├── __init__.py
  ├── test_emit_listen.py    # Agent A emit → Agent B receive
  ├── test_query_agents.py   # Поиск по capability через DHT
  └── test_reputation.py     # Вес ответов trusted vs unknown

  Критерий готовности: 2 агента обмениваются событиями по
  capability-тегам, discovery находит агента по навыку.

═══════════════════════════════════════════════════════════════
PHASE 2 — FRAMEWORK ADAPTERS
═══════════════════════════════════════════════════════════════

  Цель: LangGraph, CrewAI, AutoGen работают через mesh

  Файлы:

  02_adapters/
  ├── SPEC.md          # Каждый adapter: API, ограничения
  ├── langgraph_channel.py  # MeshChannel для StateGraph
  ├── crewai_tool.py        # MeshTool для CrewAI
  ├── autogen_agent.py      # MeshAgent для AutoGen
  ├── test_langgraph.py     # 2 агента в StateGraph через mesh
  ├── test_crewai.py        # Crew с mesh-тулом
  ├── test_autogen.py       # Agent chat через mesh

  Критерий готовности: каждый framework заменяет Redis/Kafka
  на mesh, пример работает без изменений в коде фреймворка.

═══════════════════════════════════════════════════════════════
PHASE 3 — DEPIN SDK
═══════════════════════════════════════════════════════════════

  Цель: IoT/DePIN устройство публикует телеметрию через mesh

  Файлы:

  03_depin/
  ├── SPEC.md          # Protobuf схема, Merkle-sync, device registry
  ├── depin_sdk.py     # DeviceSDK — publish/subscribe telemetry
  ├── device_identity.py  # DID для устройств
  ├── merkle_sync.py   # Diff-tree при reconnect
  ├── __init__.py
  ├── test_telemetry.py    # Дэвайс публикует → подписчик получает
  ├── test_merkle_sync.py  # 1h offline → 99% меньше трафика
  └── test_device_registry.py  # DHT put/get device info

  Критерий готовности: ESP32 (симуляция) публикует телеметрию
  через mesh, offline sync через Merkle-дерево.

═══════════════════════════════════════════════════════════════
PHASE 4 — DAO INTEGRATION (опционально)
═══════════════════════════════════════════════════════════════

  Цель: DAO управляет mesh (голосование за relay, репутация)

  Файлы:

  04_dao/
  ├── SPEC.md          # DAO-контракты, voting, treasury
  ├── dao_contract.py  # Snapshot/токен-гейтинг relay
  ├── reputation_oracle.py  # Репутация агентов on-chain
  ├── treasury_bridge.py    # Доход от mesh → DAO казна
  ├── test_dao_vote.py      # Голосование за добавление relay
  ├── test_rewards.py       # Распределение дохода агентам

  Критерий готовности: mesh relay управляется через DAO-голосование,
  доход от подписок распределяется агентам.

═══════════════════════════════════════════════════════════════
DEPLOYED
═══════════════════════════════════════════════════════════════

  v0.2.0-alpha — IPFS PubSub + Ed25519 + WAL + DHT
  32/32 тестов, 3 агента в Docker Compose, LangGraph адаптер.
  Live demo: https://p2p-dash.v2.site

═══════════════════════════════════════════════════════════════
LESSONS LEARNED (2026-05-10)
═══════════════════════════════════════════════════════════════

  v0.2.0 доказала: схема (WAL + подписи + DHT + emit/listen) работает.
  Но IPFS CLI как транспорт — тупик для production:
    - 200-500 MB RAM на daemon
    - ~20 msg/s пропускная способность
    - Зависимость от внешнего бинарника
    - Daemon crash = агент молчит

  v0.3: переписать transport.py с нуля. Свой лёгкий P2P транспорт.
  API AgentMesh не меняется. Меняется только transport.py под капотом.
  Полный roadmap: docs/ROADMAP.md

═══════════════════════════════════════════════════════════════
СТРУКТУРА (v0.2.0)
═══════════════════════════════════════════════════════════════

  p2p-agent-mesh/
  ├── phase0/         # Core Transport (IPFS PubSub + WAL + DHT + identity)
  ├── phase1/         # Agent SDK (emit/listen/query/request)
  ├── adapters/       # LangGraph + CrewAI
  ├── sdk/            # AgentMesh публичный API
  ├── examples/       # рабочие примеры
  ├── tests/          # нагрузочные тесты
  ├── docs/           # документация + roadmap
  ├── docker/         # Docker entrypoint для compose
  ├── README.md       # описалово
  ├── LICENSE
  ├── .gitignore
  └── requirements.txt
