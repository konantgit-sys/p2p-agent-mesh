# P2P Agent Mesh — Architecture Specification v0.1

## 1. Core Model
- **Network**: libp2p + gossipsub (P2P relay mesh)
- **Discovery/Registry**: Kademlia DHT (capability tags, reputation, node metadata)
- **Storage**: Local WAL (SQLite/WAL) per peer, flush-on-connect
- **Routing**: Content-based filtering via Bloom + regex on relay nodes
- **CAP Model**: AP by default (Availability + Partition Tolerance). Eventual consistency. No global ordering guarantees.

## 2. Plugin Architecture
| Plugin | Purpose | Consistency | When to use |
|--------|---------|-------------|-------------|
| `agent-sdk` | AI agent pub/sub, capability routing, framework adapters | AP | LangGraph, CrewAI, AutoGen |
| `depin-telemetry` | IoT/DePIN device sync, protobuf schemas, Merkle diff | AP | ESP32/RPi, sensors, fleet telemetry |
| `coordination-layer` | Micro-Raft shard, consumer groups, exactly-once commands | CP (optional) | Critical state sync, firmware updates, audit logs |

## 3. Security & Identity
- Peer identity: Ed25519 keypair (libp2p native)
- Message auth: Ed25519 signature + optional DID envelope
- Spam control: Rate limits + reputation score + allowlist mode
- Data locality: Explicit geo-tags + client-side encryption (optional)

## 4. Offline & Sync
- Local buffer: WAL queue per topic
- Reconnect: Merkle-tree diff → request missing chunks
- Conflict resolution: LWW / client-defined CRDT
- Backpressure: In-memory ring buffer + drop-oldest policy

## 5. Testing & SLA Targets
- Churn tolerance: 20% node drop/rejoin, <5% message loss
- Latency: <80ms p50, <250ms p99 (LAN), <1.2s p99 (WAN)
- Throughput: 3k msg/s/node (1KB protobuf)
- Duplication rate: <3% (gossipsub overlap), mitigated by client-side idempotency
