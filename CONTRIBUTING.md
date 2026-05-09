# Contributing to P2P Agent Mesh

## Welcome

This is an early-stage research project (v0.2.0). We're looking for:
- Bug reports & edge cases
- Performance benchmarks on different infra
- Multi-node deployment experiences
- LangGraph / CrewAI integration feedback

## Quick Start

```bash
# Setup
git clone https://github.com/snin-ai/p2p-agent-mesh
cd p2p-agent-mesh
pip install -r requirements.txt

# Run tests
python -m pytest phase0/ phase1/ -v
```

## How to Contribute

### Bugs
1. Check existing issues
2. Include: Python version, IPFS version, OS, full traceback
3. Attach test case if possible

### Features
1. Open an issue first
2. Describe: what problem it solves, how it fits the AP/eventual consistency model
3. Fork → branch → PR

### Tests
We run 32 tests across Phase 0 (transport, WAL, identity, sig_gate, DHT) and Phase 1 (Agent SDK, LangGraph adapter). New features need tests.

## Code Style

- Python 3.11+
- Type hints for all public functions
- Async-first (asyncio)
- No external deps except `cryptography` (Ed25519)
- No central broker, no REST, no Kafka — P2P only

## Architecture Decisions

| Decision | Rationale |
|----------|-----------|
| IPFS CLI subprocess (not py-libp2p) | Avoids C dependency hell, keeps setup to 5 min |
| Ed25519 (not RSA/ECDSA) | Fast keygen + sign, small signatures |
| SQLite WAL | Zero deps, crash-safe, fast replay |
| PubSub topic per capability | Simple routing, no central registry |

## License

MIT — do what you want, just don't blame us if your agents start a DAO.
