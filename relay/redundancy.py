"""
P2P Agent Mesh — Relay Redundancy v0.6

Multi-relay architecture:
1. Relay-to-relay peering (relay_link)
2. Client auto-failover (multi-relay client)
3. Message forwarding between relays

```
Agent A ──── relay-1 ──── relay-2 ──── Agent B
                │             │
           local agents   local agents
```
"""
import asyncio, json, logging, random, time
from typing import Optional

log = logging.getLogger('relay_redundancy')


class RelayLinkClient:
    """
    Клиент для соединения relay → relay.
    
    Подключается к другому relay, регистрируется как relay-пир.
    Relay-peer отправляет сообщения для агентов, которых нет локально.
    """
    
    def __init__(self, local_pubkey: str, on_remote_message: callable):
        self.local_pubkey = local_pubkey
        self._on_remote_message = on_remote_message
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._running = False
    
    async def connect(self, host: str, port: int) -> bool:
        """Подключиться к другому relay как peer."""
        try:
            self._reader, self._writer = await asyncio.open_connection(host, port)
        except (OSError, ConnectionRefusedError) as e:
            log.warning(f"RelayLink: connect to {host}:{port} failed: {e}")
            return False
        
        # Отправляем relay_handshake
        await self._send({
            "type": "relay_handshake",
            "pubkey": self.local_pubkey,
            "version": "0.6"
        })
        
        self._running = True
        asyncio.create_task(self._read_loop())
        log.info(f"RelayLink: connected to {host}:{port}")
        return True
    
    async def forward(self, msg: dict, target_pubkey: str) -> bool:
        """Отправить сообщение агенту на удалённом relay."""
        try:
            await self._send({
                "type": "relay_forward",
                "target": target_pubkey,
                "payload": msg
            })
            return True
        except Exception:
            return False
    
    async def _send(self, msg: dict):
        if self._writer:
            self._writer.write((json.dumps(msg) + "\n").encode())
            await self._writer.drain()
    
    async def _read_loop(self):
        while self._running and self._reader:
            try:
                line = await asyncio.wait_for(self._reader.readline(), timeout=30.0)
                if not line:
                    break
                msg = json.loads(line.decode().strip())
                msg_type = msg.get("type", "")
                
                if msg_type == "relay_forward":
                    target = msg.get("target", "")
                    payload = msg.get("payload", {})
                    if self._on_remote_message:
                        await self._on_remote_message(target, payload)
                elif msg_type == "relay_handshake":
                    log.info(f"RelayLink: peer {msg.get('pubkey', '?')[:12]} handshake OK")
                    await self._send({
                        "type": "relay_handshake_ack",
                        "pubkey": self.local_pubkey,
                        "version": "0.6"
                    })
                elif msg_type == "relay_handshake_ack":
                    log.info(f"RelayLink: handshake confirmed by {msg.get('pubkey', '?')[:12]}")
                    
            except asyncio.TimeoutError:
                continue
            except (json.JSONDecodeError, ConnectionError):
                break
        
        self._running = False
        log.warning("RelayLink: disconnected")
    
    async def stop(self):
        self._running = False
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass


class MultiRelayClient:
    """
    RelayClient с поддержкой failover.
    
    Подключается к первичному relay. Если он недоступен —
    переключается на вторичный. При восстановлении — возвращается на первичный.
    """
    
    def __init__(self, relay_list: list[tuple[str, int]]):
        """
        relay_list: [("relay-1.v2.site", 8899), ("relay-2.v2.site", 8899)]
        """
        self.relay_list = relay_list
        self._current_idx = 0
        self._current_relay: Optional[tuple[str, int]] = None
        self._inner_client = None  # будет RelayClient после connect
        self._running = False
        self._on_connect = None
        self._on_disconnect = None
    
    @property
    def current_relay(self) -> Optional[tuple[str, int]]:
        return self._current_relay
    
    async def connect(self, inner_client_factory: callable) -> bool:
        """
        Подключиться: перебирает relay из списка.
        inner_client_factory(pubkey) -> RelayClient
        """
        for attempt in range(len(self.relay_list) * 2):
            idx = (self._current_idx + attempt) % len(self.relay_list)
            host, port = self.relay_list[idx]
            
            try:
                client = await inner_client_factory(host, port)
                if client:
                    self._inner_client = client
                    self._current_idx = idx
                    self._current_relay = (host, port)
                    self._running = True
                    log.info(f"MultiRelay: connected to {host}:{port}")
                    if self._on_connect:
                        self._on_connect(host, port)
                    return True
            except Exception as e:
                log.debug(f"MultiRelay: {host}:{port} fail: {e}")
                continue
        
        log.error(f"MultiRelay: все relay недоступны ({len(self.relay_list)} шт)")
        return False
    
    async def reconnect(self) -> bool:
        """Переподключение на следующий relay."""
        self._current_idx = (self._current_idx + 1) % len(self.relay_list)
        self._inner_client = None
        return await self.connect(lambda h, p: self._inner_client)
    
    async def get_peers(self) -> list:
        """Список агентов со всех relay."""
        if self._inner_client and hasattr(self._inner_client, 'peers'):
            return self._inner_client.peers()
        return []
    
    async def stop(self):
        self._running = False
        if self._inner_client and hasattr(self._inner_client, 'stop'):
            await self._inner_client.stop()
    
    def on_connect(self, callback):
        self._on_connect = callback
    
    def on_disconnect(self, callback):
        self._on_disconnect = callback
