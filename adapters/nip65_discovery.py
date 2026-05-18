"""NIP-65 Relay Discovery — publish and discover mesh nodes.

NIP-65: Relay publishes kind:10002 with read/write URLs,
allowing other mesh nodes to discover each other and their channels.
"""
import json, time, os, hashlib
from pathlib import Path

try:
    import secp256k1
    HAS_SECP = True
except ImportError:
    HAS_SECP = False

BASE = Path.home() / "data" / "sites" / "relay-mesh"
RELAY_META_FILE = str(BASE / "relay_meta.json")

# ─── Relay metadata ───
relay_meta: dict = {
    "name": "SNIN Relay Mesh",
    "description": "P2P mesh for Nostr agent networks",
    "pubkey": "",
    "relays": {
        "read": [
            "wss://relay.example.com",
            "wss://mesh-relay.example.com",
        ],
        "write": [
            "wss://relay.example.com",
            "wss://mesh-relay.example.com",
        ],
    },
    "channels": ["direct", "mesh", "gossip", "nostr"],
    "ports": {
        "api": 9907,
        "sr": 9932,
        "eg": 9931,
        "gossip": [9100, 9101, 9102, 9103, 9104],
    },
    "version": "v3",
    "throughput": "36,873 msg/s",
    "burst": "194,388 msg/s",
    "relays_101": 101,
    "last_updated": time.time(),
}

def _load_relay_meta():
    global relay_meta
    try:
        with open(RELAY_META_FILE) as f:
            stored = json.load(f)
            relay_meta.update(stored)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

def _save_relay_meta():
    relay_meta["last_updated"] = time.time()
    with open(RELAY_META_FILE, "w") as f:
        json.dump(relay_meta, f, indent=2, ensure_ascii=False)

_load_relay_meta()

# ─── kind:10002 event builder ───

def build_relay_list_event() -> dict:
    """Build NIP-65 kind:10002 event for relay metadata."""
    relay_meta["last_updated"] = int(time.time())
    
    tags = []
    for url in relay_meta.get("relays", {}).get("read", []):
        tags.append(["r", url, "read"])
    for url in relay_meta.get("relays", {}).get("write", []):
        tags.append(["r", url, "write"])
    
    event = {
        "pubkey": relay_meta.get("pubkey", ""),
        "created_at": int(time.time()),
        "kind": 10002,
        "tags": tags,
        "content": json.dumps({
            "name": relay_meta.get("name", ""),
            "description": relay_meta.get("description", ""),
            "channels": relay_meta.get("channels", []),
            "throughput": relay_meta.get("throughput", ""),
            "burst": relay_meta.get("burst", ""),
            "relays_101": relay_meta.get("relays_101", 0),
        }),
    }
    return event

def sign_relay_event(event: dict, privkey_hex: str = "") -> dict:
    """Schnorr-sign the NIP-65 event."""
    if not HAS_SECP or not privkey_hex:
        return event
    try:
        priv = secp256k1.PrivateKey(bytes.fromhex(privkey_hex))
        event["pubkey"] = priv.pubkey.serialize()[1:].hex()
        
        serialized = json.dumps([
            0, event["pubkey"], event["created_at"],
            event["kind"], event["tags"], event["content"],
        ], separators=(",", ":"), ensure_ascii=False).encode()
        
        event["sig"] = priv.schnorr_sign(serialized, 'BIPSchnorr').hex()
        _save_relay_meta()
    except Exception as e:
        print(f"[NIP-65] Sign error: {e}")
    return event

# ─── Store & Publish ───

def store_in_redis(redis_client=None, event: dict = None):
    """Store relay metadata in Redis under nip65:relay_list."""
    if event is None:
        event = build_relay_list_event()
    
    if redis_client:
        try:
            redis_client.set("nip65:relay_list", json.dumps(event))
            redis_client.expire("nip65:relay_list", 86400)  # 24h TTL
            return True
        except Exception:
            pass
    
    # Fallback: file
    with open(str(BASE / "nip65_relay_list.json"), "w") as f:
        json.dump(event, f, indent=2)
    return True

def discover_relays(redis_client=None) -> list:
    """Discover other mesh-nodes via NIP-65."""
    relays = []
    
    if redis_client:
        try:
            data = redis_client.get("nip65:relay_list")
            if data:
                event = json.loads(data)
                for tag in event.get("tags", []):
                    if len(tag) >= 2 and tag[0] == "r":
                        relays.append({
                            "url": tag[1],
                            "purpose": tag[2] if len(tag) > 2 else "read",
                            "pubkey": event.get("pubkey", "?"),
                        })
        except Exception:
            pass
    
    return relays

# ─── API handler ───

def get_relay_info() -> dict:
    """Return relay info for /nip65 endpoint."""
    event = build_relay_list_event()
    return {
        "nip65": event,
        "relay_info": relay_meta,
        "discovered_relays": [],
    }
