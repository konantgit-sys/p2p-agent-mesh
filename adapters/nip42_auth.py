"""NIP-42 AUTH module — WebSocket authentication for SNIN Mesh.

Implements:
  - WebSocket endpoint /ws with NIP-42 handshake
  - Challenge-response AUTH (kind:22242) via Schnorr signatures (BIP-340)
  - Session tokens for authenticated clients
  - Rate limiting (anonymous vs authenticated)
"""
import json, time, secrets
from typing import Optional

try:
    import secp256k1
    HAS_SECP = True
except ImportError:
    HAS_SECP = False

# ─── Config ───
RATE_LIMIT_ANON = 10       # msg/s per IP (anonymous)
RATE_LIMIT_AUTH = 100      # msg/s per pubkey (authenticated)
CHALLENGE_TTL = 60         # seconds
SESSION_TTL = 3600         # 1 hour

# ─── In-memory stores ───
challenges: dict[str, float] = {}   # challenge → timestamp
sessions: dict[str, dict] = {}      # session_token → {pubkey, created_at}
rate_limit: dict[str, list] = {}    # key → [timestamp, ...]

# ─── NIP-01 event serialization ───

def serialize_event(event: dict) -> bytes:
    """NIP-01 canonical serialization: [0, pubkey, created_at, kind, tags, content]"""
    return json.dumps([
        0,
        event.get("pubkey", ""),
        event.get("created_at", 0),
        event.get("kind", 0),
        event.get("tags", []),
        event.get("content", ""),
    ], separators=(",", ":"), ensure_ascii=False).encode()

def verify_event_signature(event: dict) -> bool:
    """Verify Schnorr signature (BIP-340) of a Nostr event (NIP-01, NIP-42)."""
    if not HAS_SECP:
        return False
    try:
        serialized = serialize_event(event)
        sig = bytes.fromhex(event.get("sig", ""))
        pubkey_hex = event.get("pubkey", "")
        if len(sig) != 64 or len(pubkey_hex) != 64:
            return False
        pubkey_bytes = bytes.fromhex(pubkey_hex)
        # Try both compressed prefixes (02 = even y, 03 = odd y)
        for prefix in (b'\x02', b'\x03'):
            try:
                pk = secp256k1.PublicKey(pubkey=prefix + pubkey_bytes, raw=True)
                if pk.schnorr_verify(serialized, sig, 'BIPSchnorr'):
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False

def is_valid_auth_event(event: dict, challenge: str) -> bool:
    """Check if event is a valid NIP-42 AUTH response."""
    if event.get("kind") != 22242:
        return False
    if event.get("content", "") != challenge:
        return False
    # Tags should include relay URL
    tags = event.get("tags", [])
    has_relay = any(t[0] == "relay" for t in tags if len(t) > 1)
    if not has_relay:
        return False
    return verify_event_signature(event)

# ─── Challenge management ───

def generate_challenge() -> str:
    """Generate a unique challenge string."""
    challenge = secrets.token_hex(16)
    challenges[challenge] = time.time()
    return challenge

def validate_challenge(challenge: str) -> bool:
    """Check if challenge is valid and not expired."""
    ts = challenges.get(challenge)
    if ts is None:
        return False
    if time.time() - ts > CHALLENGE_TTL:
        challenges.pop(challenge, None)
        return False
    challenges.pop(challenge, None)  # one-time use
    return True

# ─── Session management ───

def create_session(pubkey: str) -> str:
    """Create a session token for an authenticated pubkey."""
    token = secrets.token_hex(24)
    sessions[token] = {"pubkey": pubkey, "created_at": time.time()}
    return token

def validate_session(token: str) -> Optional[str]:
    """Validate session token and return pubkey if valid."""
    session = sessions.get(token)
    if session is None:
        return None
    if time.time() - session["created_at"] > SESSION_TTL:
        sessions.pop(token, None)
        return None
    # Refresh TTL on use
    session["created_at"] = time.time()
    return session["pubkey"]

def cleanup_sessions():
    """Remove expired sessions and challenges."""
    now = time.time()
    for token, session in list(sessions.items()):
        if now - session["created_at"] > SESSION_TTL:
            sessions.pop(token, None)
    for challenge, ts in list(challenges.items()):
        if now - ts > CHALLENGE_TTL:
            challenges.pop(challenge, None)

# ─── Rate limiting ───

def check_rate_limit(key: str, max_per_sec: int) -> bool:
    """Check if key is within rate limit. Returns True if allowed."""
    now = time.time()
    timestamps = rate_limit.setdefault(key, [])
    # Remove timestamps older than 1 second
    rate_limit[key] = [t for t in timestamps if now - t < 1.0]
    if len(rate_limit[key]) >= max_per_sec:
        return False
    rate_limit[key].append(now)
    return True

# ─── WebSocket handler helpers ───

def format_auth_message(challenge: str) -> str:
    """NIP-42 AUTH challenge message."""
    return json.dumps(["AUTH", challenge])

def format_auth_ok() -> str:
    """NIP-42 AUTH OK response."""
    return json.dumps(["AUTH", "OK"])

def format_notice(msg: str) -> str:
    """Nostr NOTICE message."""
    return json.dumps(["NOTICE", msg])

def parse_message(data: str) -> list:
    """Parse a Nostr protocol message."""
    try:
        msg = json.loads(data)
        if not isinstance(msg, list) or len(msg) < 1:
            return None
        return msg
    except (json.JSONDecodeError, TypeError):
        return None

# ─── HTTP Auth middleware ───

def get_auth_pubkey(request_headers) -> Optional[str]:
    """Extract authenticated pubkey from HTTP request headers."""
    auth_header = request_headers.get("Authorization", "")
    if auth_header.startswith("NIP-42 "):
        token = auth_header[7:]
        return validate_session(token)
    return None
