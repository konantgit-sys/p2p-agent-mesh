"""SNIN Mesh Agent Client — library for connecting AI agents to a SmartRouter network.

Usage:
    from mesh_client import MeshAgent
    
    agent = MeshAgent(
        pubkey="npub1forecaster...",
        name="forecaster_ai",
        mesh_host="127.0.0.1",
        mesh_port=9932,
        api_url="http://127.0.0.1:9907"
    )
    
    # Send a message
    result = await agent.send(
        to="npub1archivist...",
        payload={"text": "hello"},
        channel="mesh"
    )
    
    # Get network status
    status = await agent.ping()
"""

import asyncio
import json
import time
import uuid
from typing import Optional

class MeshAgent:
    """Client for connecting an AI agent to a SNIN Mesh."""
    
    def __init__(self, pubkey: str, name: str = "", 
                 mesh_host: str = "127.0.0.1", mesh_port: int = 9932,
                 api_url: str = "http://127.0.0.1:9907"):
        self.pubkey = pubkey
        self.name = name or pubkey[:16]
        self.mesh_host = mesh_host
        self.mesh_port = mesh_port
        self.api_url = api_url.rstrip("/")
        self._reader = None
        self._writer = None
        self._connected = False
        self._msg_id = 0
    
    async def connect(self) -> bool:
        """Connect to SmartRouter."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.mesh_host, self.mesh_port), 
                timeout=5
            )
            self._connected = True
            return True
        except Exception as e:
            print(f"[{self.name}] ❌ Connect failed: {e}")
            return False
    
    async def disconnect(self):
        """Close connection."""
        if self._writer:
            try:
                self._writer.close()
            except:
                pass
        self._connected = False
    
    async def send(self, to: str = "broadcast", payload: dict = None,
                   kind: int = 39002, channel: str = "auto", 
                   priority: str = "normal") -> dict:
        """Send a message through SmartRouter."""
        if not self._connected:
            if not await self.connect():
                return {"ok": False, "error": "not connected"}
        
        msg = {
            "kind": kind,
            "pubkey": self.pubkey,
            "from": self.pubkey,
            "to": to,
            "id": f"{self.pubkey[:16]}_{self._msg_id}_{int(time.time()*1000)}",
            "meta": {
                "channel": channel,
                "priority": priority,
                "timestamp": time.time(),
                "agent": self.name
            },
            "payload": payload or {}
        }
        self._msg_id += 1
        
        try:
            self._writer.write(json.dumps(msg).encode() + b"\n")
            await asyncio.wait_for(self._writer.drain(), timeout=5)
            return {"ok": True, "id": msg["id"]}
        except Exception as e:
            self._connected = False
            return {"ok": False, "error": str(e)}
    
    async def broadcast(self, payload: dict, kind: int = 39002) -> dict:
        """Broadcast to all agents in the network."""
        return await self.send(to="broadcast", payload=payload, kind=kind)
    
    async def register(self) -> dict:
        """Register agent in Mesh API."""
        import urllib.request
        data = json.dumps({
            "pubkey": self.pubkey,
            "name": self.name,
            "meta": {
                "type": "ai-agent",
                "role": self.name.split("_")[0] if "_" in self.name else "agent",
                "version": "1.0"
            }
        }).encode()
        req = urllib.request.Request(
            f"{self.api_url}/agents/register",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            return json.loads(resp.read())
        except Exception as e:
            return {"ok": False, "error": str(e)}
    
    async def ping(self) -> dict:
        """Check agent status in the network."""
        import urllib.request
        try:
            req = urllib.request.Request(f"{self.api_url}/agents/{self.pubkey}/ping", method="POST")
            resp = urllib.request.urlopen(req, timeout=3)
            return json.loads(resp.read())
        except Exception as e:
            return {"ok": False, "error": str(e)}
    
    @staticmethod
    async def get_network_status(api_url: str = "http://127.0.0.1:9907") -> dict:
        """Get overall network status."""
        import urllib.request
        try:
            resp = urllib.request.urlopen(f"{api_url}/health", timeout=3)
            return json.loads(resp.read())
        except Exception as e:
            return {"ok": False, "error": str(e)}


# ─── Console test ───
if __name__ == "__main__":
    async def test():
        agent = MeshAgent(
            pubkey="npub1testagent...",
            name="test_agent",
        )
        # Register
        r = await agent.register()
        print(f"Register: {r}")
        # Connect
        c = await agent.connect()
        print(f"Connect: {c}")
        # Send
        s = await agent.send(to="broadcast", payload={"text": "hello mesh!"})
        print(f"Send: {s}")
        # Status
        status = await MeshAgent.get_network_status()
        print(f"Network: {status.get('pools',{}).get('workers_alive',0)} workers, {status.get('agents',0)} agents")
        await agent.disconnect()
    
    asyncio.run(test())
