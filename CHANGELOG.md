# Changelog

## [v0.3.1-alpha] - 2026-05-10
### Added
- TCP transport layer: `asyncio.start_server` + outbound connections with exponential backoff (1→30s)
- JSON-lines protocol over TCP: `pub`, `sub`, `unsub`, `ping/pong`
- Automatic cross-process delivery: in-memory bus → TCP broadcast → local delivery
- 4 new TCP integration tests (`test_tcp_transport.py`)

### Changed
- `P2PTransport` expanded to 373 lines (was 97), zero new dependencies
- Test suite: 32 → 36 tests, total runtime ~16.7s
- `subscribe()`/`unsubscribe()` now notify connected TCP peers

### Known Limits (v0.3)
- 🔓 No encryption: trusted networks / VPC only
- 🌐 No NAT traversal: direct IP:port required
- 📦 Base64 payload encoding: ~33% overhead (planned: msgpack/protobuf in v0.4)
- 🔁 Deduplication handled client-side (`msg_id` cache)

## [v0.3.0-alpha] - 2026-05-10
### Added
- `P2PTransport` — zero-dependency in-memory transport (replaces IPFS CLI)
- Global `_bus` relay with `asyncio.Lock` for same-process pub/sub

### Changed
- Test suite speedup: 58s → 12.5s (5x faster)
- Removed all IPFS daemon dependencies

### Removed
- `IPFSTransport` — replaced by `P2PTransport`
- `stdbuf`, `ipfs` subprocess calls

## [v0.2.0-alpha] - 2026-05-09
### Added
- Initial prototype: IPFS PubSub CLI wrapper
- Ed25519 identity + signing (phase0/identity.py)
- WAL buffer on SQLite (phase0/wal.py)
- SigGate rate limiter (phase0/sig_gate.py)
- DHT store for agent discovery (phase0/dht.py)
- AgentMesh SDK (sdk/agent.py) — emit/listen/query/request
- LangGraph + CrewAI adapters
- 32 tests, live demo at https://p2p-dash.v2.site
