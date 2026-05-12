"""Пример: 3 агента в P2P mesh через LangGraph адаптер.

Запуск:
    python examples/3_agent_langgraph.py

Демонстрирует:
    - 3 агента обмениваются сообщениями через IPFS PubSub
    - LangGraph адаптер (MeshTopic, MeshStateSync, MeshRPC)
    - Подписанные Ed25519 сообщения
    - WAL буферизация
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sdk.agent import AgentMesh


async def main():
    print("=" * 60)
    print("P2P Agent Mesh — 3 Agent LangGraph Demo")
    print("=" * 60)

    # Создаём 3 агента с разными capability
    forecaster = AgentMesh("forecaster_v2", ["forecasting", "crypto_analysis"])
    cryter = AgentMesh("cryter_v10", ["crypto_analysis", "sentiment"])
    creator = AgentMesh("creator_v3", ["content_creation"])

    print("\n[init] Starting 3 agents...")
    await asyncio.gather(forecaster.start(), cryter.start(), creator.start())
    print(f"[init] Forecaster: {forecaster.did}")
    print(f"[init] Cryter:     {cryter.did}")
    print(f"[init] Creator:    {creator.did}")

    # Переменные для результатов
    results = {
        "forecast_received": None,
        "signal_received": None,
        "content_received": None,
    }

    # Подписки
    def on_forecast(msg):
        results["forecast_received"] = msg
        print(f"\n[Cryter] ← Received forecast: {msg['payload']}")

    def on_signal(msg):
        results["signal_received"] = msg
        print(f"\n[Creator] ← Received signal: {msg['payload']}")

    await cryter.listen({"capability": "forecasting"}, on_forecast)
    await creator.listen({"capability": "crypto_analysis"}, on_signal)

    # Ждём распространения подписок через DHT
    await asyncio.sleep(2)

    # Forecaster публикует прогноз
    print("\n[Forecaster] → Publishing forecast...")
    await forecaster.emit(
        "forecasting",
        {
            "asset": "BTC",
            "prediction": "bullish",
            "confidence": 0.78,
            "timestamp": time.time(),
        },
    )

    # Ждём обработки
    await asyncio.sleep(3)

    # Cryter получил прогноз — публикует комбинированный сигнал
    if results["forecast_received"]:
        print("\n[Cryter] → Publishing combined signal...")
        await cryter.emit(
            "crypto_analysis",
            {
                "asset": "BTC",
                "sentiment": 0.32,
                "forecast_confidence": results["forecast_received"]["payload"].get("confidence", 0),
                "source": "cryter+forecaster",
                "timestamp": time.time(),
            },
        )

    # Ждём
    await asyncio.sleep(3)

    # Creator получил сигнал
    if results["signal_received"]:
        signal = results["signal_received"]["payload"]
        print("\n[Creator] → Generating content...")
        sentiment = signal.get("sentiment", "N/A")
        confidence = signal.get("forecast_confidence", "N/A")
        post = f"Signal: BTC sentiment {sentiment}, forecast confidence {confidence}"
        print(f"[Creator] Content: {post}")

    # Итог
    print("\n" + "=" * 60)
    print("Chain complete: Forecaster → Cryter → Creator")
    print("No Kafka. No Redis. No REST. All via P2P mesh.")
    print("WAL state:")
    print(f"  Forecaster: {forecaster.status()['wal_count']} msgs")
    print(f"  Cryter:     {cryter.status()['wal_count']} msgs")
    print(f"  Creator:    {creator.status()['wal_count']} msgs")
    print("=" * 60)

    # Остановка
    await asyncio.gather(forecaster.stop(), cryter.stop(), creator.stop())


if __name__ == "__main__":
    asyncio.run(main())
