"""HTTP relay client — для подключения через relay-mesh.v2.site.

Использование:
    from relay.http_client import HTTPRelayClient
    client = HTTPRelayClient(identity, "https://relay-mesh.v2.site")
    await client.register()
    peers = await client.peers()
    await client.send(target_pubkey, b"encrypted data")
    msgs = await client.recv()
"""

import json
import time
import urllib.request
import urllib.error
from typing import Optional


class HTTPRelayClient:
    """Relay клиент поверх HTTP API (long-polling)."""

    def __init__(self, identity, relay_url: str = "https://relay-mesh.v2.site"):
        self._identity = identity
        self._relay_url = relay_url.rstrip("/")
        self._pubkey = identity.public_key_hex
        self._agent_id = identity.public_key_hex[:16]
        self._relay_info = None

    @property
    def public_key(self) -> str:
        return self._pubkey

    @property
    def agent_id(self) -> str:
        return self._agent_id

    def _request(self, method: str, path: str, data: Optional[dict] = None,
                 timeout: int = 10) -> Optional[dict]:
        url = f"{self._relay_url}{path}"
        if method == "GET":
            req = urllib.request.Request(url)
        else:
            body = json.dumps(data).encode() if data else b""
            req = urllib.request.Request(url, data=body,
                                         headers={"Content-Type": "application/json"})
            req.method = method

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            print(f"[relay] Request failed: {e}")
            return None

    async def register(self) -> bool:
        """Зарегистрироваться на relay."""
        resp = self._request("POST", "/api/register", {
            "pubkey": self._pubkey,
            "capabilities": [],
        })
        if resp and resp.get("type") == "registered":
            self._relay_info = resp
            return True
        return False

    async def peers(self) -> list[dict]:
        """Получить список подключённых агентов."""
        resp = self._request("GET", f"/api/peers?pubkey={self._pubkey}")
        if resp:
            return resp.get("peers", [])
        return []

    async def send(self, target_pubkey: str, data: bytes, msg_type: str = "send",
                   eph_pub: str = "") -> bool:
        """Отправить E2E зашифрованное сообщение через relay."""
        payload = {
            "from": self._pubkey,
            "target": target_pubkey,
            "data": data.hex() if isinstance(data, bytes) else data,
            "type": msg_type,
        }
        if eph_pub:
            payload["eph_pub"] = eph_pub
        resp = self._request("POST", "/api/send", payload)
        return resp is not None and resp.get("ok") is True

    async def recv(self) -> list[dict]:
        """Получить все ожидающие сообщения."""
        resp = self._request("GET", f"/api/messages?pubkey={self._pubkey}")
        if resp:
            return resp.get("messages", [])
        return []

    async def e2e_establish(self, target_pubkey: str) -> Optional[bytes]:
        """Полный цикл E2E установки через relay.
        Возвращает общий сессионный ключ или None при ошибке.
        """
        from phase0.handshake import X25519Keypair, ecdh_hkdf

        # Создаём эфемерный ключ
        eph = X25519Keypair()
        eph_pub_hex = eph.public_key_hex

        # Отправляем e2e_init
        ok = await self.send(target_pubkey, b"", msg_type="e2e_init",
                             eph_pub=eph_pub_hex)
        if not ok:
            return None

        # Ждём ответ (long-poll)
        deadline = time.time() + 15
        while time.time() < deadline:
            msgs = await self.recv()
            for msg in msgs:
                if msg.get("type") == "e2e_req" and "eph_pub" in msg:
                    # B отвечает на запрос A
                    b_eph_pub = msg["eph_pub"]
                    shared = ecdh_hkdf(eph.private_key, b_eph_pub)
                    return shared
            time.sleep(0.5)
        return None
