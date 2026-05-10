"""Relay over HTTP API — для деплоя на *.v2.site.

POST /api/message — отправить сообщение
GET /api/messages    — получить сообщения
"""

import os, json, time, sys
from collections import defaultdict, deque
from flask import Flask, request, jsonify

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from phase0.identity import Identity

app = Flask(__name__)

# ─── In-memory store ─────────────────────────────────────────────
agents: dict[str, dict] = {}       # pubkey → {capabilities, registered_at}
pending: dict[str, list[dict]] = defaultdict(list)  # target_pubkey → [msg]
rate_limits: dict[str, deque] = defaultdict(deque)
MAX_MSGS_PER_SEC = int(os.getenv("RELAY_MAX_MSGS_PER_SEC", "10"))
MAX_PAYLOAD = int(os.getenv("RELAY_MAX_PAYLOAD", str(2**20)))  # 1 MB

relay_identity = Identity()


def check_rate_limit(peer: str) -> bool:
    now = time.time()
    q = rate_limits[peer]
    while q and q[0] < now - 1:
        q.popleft()
    if len(q) >= MAX_MSGS_PER_SEC:
        return False
    q.append(now)
    return True


# ─── Auth middleware ────────────────────────────────────────────
def verify_auth(pubkey: str) -> bool:
    # В v0.4.2 — подпись запроса. Сейчас просто проверяем формат
    return len(pubkey) == 64 and all(c in "0123456789abcdef" for c in pubkey)


# ─── API Routes ─────────────────────────────────────────────────
@app.route("/api/register", methods=["POST"])
def register():
    data = request.get_json(force=True, silent=True) or {}
    pubkey = data.get("pubkey", "")
    caps = data.get("capabilities", [])
    if not verify_auth(pubkey):
        return jsonify({"error": "invalid_pubkey"}), 400
    agents[pubkey] = {"capabilities": caps, "registered_at": time.time(),
                      "agent_id": pubkey[:16]}
    return jsonify({
        "type": "registered",
        "agent_id": pubkey[:16],
        "relay": relay_identity.public_key_hex[:16],
    })


@app.route("/api/peers", methods=["GET"])
def peers():
    pubkey = request.args.get("pubkey", "")
    if not verify_auth(pubkey):
        return jsonify({"error": "invalid_pubkey"}), 400
    peer_list = [
        {"pubkey": k, "capabilities": v["capabilities"]}
        for k, v in agents.items() if k != pubkey
    ]
    return jsonify({"type": "peers", "peers": peer_list})


@app.route("/api/send", methods=["POST"])
def send():
    data = request.get_json(force=True, silent=True) or {}
    from_pubkey = data.get("from", "")
    target = data.get("target", "")
    payload = data.get("data", "")
    msg_type = data.get("type", "send")  # send, e2e_init, e2e_accept

    if not verify_auth(from_pubkey) or not verify_auth(target):
        return jsonify({"error": "invalid_pubkey"}), 400

    if not check_rate_limit(from_pubkey):
        return jsonify({"error": "rate_limited"}), 429

    if len(payload) > MAX_PAYLOAD:
        return jsonify({"error": "payload_too_large"}), 413

    if target not in agents:
        return jsonify({"error": "target_not_found"}), 404

    # Сохраняем сообщение
    if msg_type == "send":
        mtype = "recv"
    elif msg_type == "e2e_init":
        mtype = "e2e_req"
    elif msg_type == "e2e_accept":
        mtype = "e2e_ready"
    else:
        return jsonify({"error": "unknown_type"}), 400

    msg = {
        "type": mtype,
        "from": from_pubkey,
        "data": payload,
        "ts": time.time(),
    }
    if msg_type in ("e2e_init", "e2e_accept") and data.get("eph_pub"):
        msg["eph_pub"] = data["eph_pub"]

    pending[target].append(msg)
    return jsonify({"ok": True, "queued": len(pending[target])})


@app.route("/api/messages", methods=["GET"])
def get_messages():
    pubkey = request.args.get("pubkey", "")
    if not verify_auth(pubkey):
        return jsonify({"error": "invalid_pubkey"}), 400

    msgs = list(pending.pop(pubkey, []))
    return jsonify({"type": "messages", "messages": msgs})


@app.route("/api/stats", methods=["GET"])
def stats():
    return jsonify({
        "agents": len(agents),
        "pending": sum(len(v) for v in pending.values()),
        "relay_pubkey": relay_identity.public_key_hex[:16],
        "uptime": time.time() - start_time,
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "agents": len(agents)})


# ─── Main ──────────────────────────────────────────────────────
start_time = time.time()

if __name__ == "__main__":
    port = int(os.environ.get("RELAY_PORT", "9907"))
    print(f"[relay] HTTP relay starting on 0.0.0.0:{port}")
    print(f"[relay] Relay identity: {relay_identity.public_key_hex[:16]}...")
    # Flask dev server — для production перейти на gunicorn/uvicorn
    app.run(host="0.0.0.0", port=port, debug=False)
