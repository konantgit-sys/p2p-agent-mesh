"""Пример: 3 агента в P2P mesh — Cryter, Forecaster, Creator."""

import asyncio

from sdk.agent import AgentMesh

BOOTSTRAP = ["/ip4/127.0.0.1/tcp/9001/p2p/seed"]


async def signal_flow():
    # Инициализация 3 агентов
    cryter = AgentMesh("cryter_v10", ["crypto_analysis", "sentiment"], BOOTSTRAP)
    forecaster = AgentMesh("forecaster_v2", ["forecasting", "crypto_analysis"], BOOTSTRAP)
    creator = AgentMesh("creator_v3", ["content_creation"], BOOTSTRAP)

    await asyncio.gather(cryter.start(), forecaster.start(), creator.start())

    # Forecaster публикует прогноз
    @forecaster.listen(["forecasting"], lambda e: None)
    async def publish_forecast():
        await forecaster.emit(
            "forecasting",
            {
                "asset": "BTC",
                "prediction": "bullish",
                "confidence": 0.78,
                "model": "forecaster_v2",
            },
        )
        print("[Forecaster] Published forecast")

    # Cryter подписан на forecasting
    @cryter.listen(["forecasting"])
    async def on_forecast(event):
        print(f"[Cryter] Received forecast: {event}")
        # Комбинируем с собственным анализом
        combined = {
            "asset": "BTC",
            "sentiment": 0.32,
            "forecast_confidence": event.get("confidence", 0),
            "source": "cryter+forecaster",
        }
        await cryter.emit("crypto_analysis", combined)
        print("[Cryter] Published combined signal")

    # Creator подписан на crypto_analysis
    @creator.listen(["crypto_analysis"])
    async def on_signal(event):
        print(f"[Creator] Received signal: {event}")
        post = f"Signal: BTC sentiment {event.get('sentiment', 'N/A')}"
        await creator.emit("content_creation", {"text": post})
        print(f"[Creator] Content created: {post[:60]}...")

    # Даём время на обмен
    await asyncio.sleep(2)
    print("\n--- Chain complete: Forecaster → Cryter → Creator ---")
    print("No Kafka, no Redis, no REST — all via P2P mesh.")

    await asyncio.gather(cryter.stop(), forecaster.stop(), creator.stop())


if __name__ == "__main__":
    asyncio.run(signal_flow())
