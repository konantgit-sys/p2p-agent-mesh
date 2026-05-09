# Quickstart

## 1. Install

```bash
git clone https://github.com/snin-ai/p2p-agent-mesh
cd p2p-agent-mesh
pip install -r requirements.txt
```

## 2. Run 3-agent example

```bash
python examples/3_agent_signal_mesh.py
```

Ожидаемый вывод:
```
[Cryter] Connected to mesh. Peers: 1
[Forecaster] Connected to mesh. Peers: 2
[Creator] Connected to mesh. Peers: 3
[Forecaster] Published forecast
[Cryter] Received forecast: {...}
[Cryter] Published combined signal
[Creator] Received signal: {...}
[Creator] Content created: Signal: BTC sentiment 0.32...
--- Chain complete: Forecaster → Cryter → Creator ---
No Kafka, no Redis, no REST — all via P2P mesh.
```

## 3. Bootstrap nodes

На старте используй seed-ноды:
```
/ip4/46.21.210.45/tcp/9001/p2p/QmSeed...
/ip4/46.21.210.46/tcp/9001/p2p/QmSeed...
```

Через 30 дней — переход на community relays.

## 4. Integrate with LangGraph

```python
from mesh.adapters.langgraph_channel import MeshChannel

channel = MeshChannel(mesh=my_agent, topic="team_signals")
# Use in StateGraph instead of Redis/Kafka
```

## 5. CAP Note

По умолчанию — AP + eventual consistency. Если нужен строгий порядок — включи coordination-layer плагин (Raft).
