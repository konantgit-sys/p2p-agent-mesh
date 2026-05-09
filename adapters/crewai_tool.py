"""CrewAI adapter — MeshTool для P2P коммуникации агентов."""

from typing import Any
from sdk.agent import AgentMesh


class MeshTool:
    """CrewAI-compatible tool wrapped around P2P mesh."""

    def __init__(self, mesh: AgentMesh, capability: str, description: str):
        self.mesh = mesh
        self.capability = capability
        self.name = f"mesh_{capability}"
        self.description = description

    def run(self, **kwargs) -> dict:
        """Execute mesh request."""
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(
                self.mesh.request(self.capability, kwargs)
            )
        except RuntimeError:
            result = {"error": "no event loop"}
        return result or {}
