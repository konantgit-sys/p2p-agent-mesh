# Changelog

## [v0.4.1-alpha] - 2026-05-10
### Added
- **NAT Traversal via Relay**: публичный relay-узел для агентов за NAT
- `relay/server.py` — relay TCP сервер (handshake + registry + forward)
- `relay/client.py` — relay клиент (E2E key exchange + encrypted send/recv)
- `relay/SPEC.md` — спецификация протокола (6 типов сообщений)
- **E2E через relay**: X25519 ECDH + HKDF (совпадает с handshake.py)
- 10 новых тестов relay

### Security
- Relay не видит контент: сквозное шифрование между агентами
- E2E ключи: эфемерные X25519 (Perfect Forward Secrecy)
- Relay видит только pubkey агента (публичная информация)

### Known Limits (v0.4)
- 📦 Base64 payload encoding: ~33% overhead
- 🔁 Deduplication handled client-side (`msg_id` cache)

## [v0.4.0-alpha] - 2026-05-10
### Added
- **Secure Mesh**: сквозное шифрование ChaCha20-Poly1305 поверх TCP (issue #4)
- **Mutual auth**: Ed25519 challenge-response рукопожатие (3 сообщения)
- **Perfect Forward Secrecy**: X25519 ECDH + HKDF для сессионных ключей
- `phase0/handshake.py` — handshake протокол + SecureSession API
- `phase0/SPEC_v0.4.md` — спецификация Secure Mesh
- 8 новых тестов: SecureSession unit (3), handshake (2), TLS e2e (2), backward compat (1)

### Changed
- `P2PTransport.__init__()`: новые параметры `identity`, `use_tls`
- `P2PTransport` расширен до ~444 строк (+71), zero new runtime deps
- Test suite: 28 → 36 тестов
- `MESH_TLS=1` env — включает шифрование

### Security
- Ed25519 mutual authentication (identity.py)
- ChaCha20-Poly1305 AEAD per-message encryption
- Replay protection: sequence-numbered nonces
- Forward secrecy: ephemeral X25519 keys per session

### Known Limits (v0.4)
- 🌐 No NAT traversal: direct IP:port required
- 📦 Base64 payload encoding: ~33% overhead
- 🔁 Deduplication handled client-side (`msg_id` cache)

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
