#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""P2P Agent Mesh — CLI.

Запуск mesh-агента с указанием порта.

Пример:
    python cli.py --port 9090 --agent listener --capability echo
    python cli.py --port 0  # random port
"""

import argparse
import asyncio
import sys

from sdk.agent import AgentMesh


def main():
    parser = argparse.ArgumentParser(description="P2P Agent Mesh — запуск mesh-агента")
    parser.add_argument(
        "--port", type=int, default=0, help="TCP port для mesh (0 = random, default)"
    )
    parser.add_argument(
        "--agent", type=str, default="cli_agent", help="Имя агента (default: cli_agent)"
    )
    parser.add_argument(
        "--capability",
        type=str,
        default="echo",
        help="Capability (default: echo). Можно указать несколько через запятую",
    )
    parser.add_argument("--relay-host", type=str, default=None, help="Relay host для NAT traversal")
    parser.add_argument("--relay-port", type=int, default=0, help="Relay port")
    parser.add_argument(
        "--listen", action="store_true", help="Режим слушателя (подписка на capability)"
    )

    args = parser.parse_args()

    capabilities = [c.strip() for c in args.capability.split(",")]

    agent = AgentMesh(
        agent_id=args.agent,
        capabilities=capabilities,
        port=args.port,
        relay_host=args.relay_host,
        relay_port=args.relay_port,
    )

    async def run():
        print(f"[cli] Starting agent '{args.agent}' on port {args.port or 'random'}")
        print(f"[cli] Capabilities: {capabilities}")

        ok = await agent.start()
        if not ok:
            print("[cli] Failed to start agent")
            sys.exit(1)

        print(f"[cli] Agent started. DID: {agent.did}")
        port_str = str(agent.transport._tcp_port) if hasattr(agent, "transport") else "N/A"
        print(f"[cli] Listening on port: {port_str}")

        if args.listen:

            caps = {"capability": capabilities} if len(capabilities) == 1 else {"capabilities": capabilities}

            async def on_event(event):
                print(f"[{args.agent}] Received: {event}")

            sub = await agent.listen(caps, on_event)
            print(f"[cli] Listening for capabilities: {capabilities} (sub={sub._id})")
        else:
            # В режиме публикатора — просто шлём раз в 10 сек
            async def publish_loop():
                import time

                while True:
                    msg = {"msg": f"heartbeat from {args.agent}", "ts": time.time()}
                    await agent.emit(capabilities[0], msg)
                    print(f"[{args.agent}] Published: {msg}")
                    await asyncio.sleep(10)

            asyncio.create_task(publish_loop())
            print(f"[cli] Publisher mode. Emitting to capability: {capabilities[0]} every 10s")

        # Держим процесс живым
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await agent.stop()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[cli] Shutting down...")


if __name__ == "__main__":
    main()
