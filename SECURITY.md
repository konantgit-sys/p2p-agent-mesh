# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| v0.2.x | ✅ Active development |
| < v0.2 | ❌ |

## Reporting a Vulnerability

This is an open-source research project in active development.
If you find a security vulnerability:

1. **Do NOT** open a public GitHub issue
2. Email: `snin@duck.com`
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Affected version
   - Any suggested fix (optional)

We aim to respond within 48 hours and patch within 7 days.

## Known Security Properties

### Crypto
- **Ed25519** for message signing (keygen <2ms, sign <1ms, verify <1ms)
- DID format: `did:snin:<sha256(pubkey)[:16]>`
- Repudiation: signatures provide authenticity, not non-repudiation

### Transport
- IPFS PubSub (gossipsub) — unencrypted by default
- **No transport encryption** in v0.2 — use VPN/tailscale for untrusted networks
- Messages are signed but payloads are visible to all gossipsub peers

### Rate Limiting
- SigGate: 10 messages/sec per pubkey
- No global rate limit yet (v0.2+)

## Disclosure Policy

- Critical: 7 day embargo before public disclosure
- Normal: publish fix alongside disclosure
- Low: addressed in next release
