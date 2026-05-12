# SPDX-License-Identifier: MIT
# Copyright 2026 SNIN Network <snin@v2.site>

"""SNIN DAO Pilot — 3-агентная цепочка через P2P mesh.

Cryter публикует сигналы → Forecaster подписан → публикует прогнозы
→ Creator подписан → создаёт контент.

Запуск:
    python3 -m pilot.snin_dao_chain

Требования:
    - Никакого Kafka, Redis, REST — только mesh
    - Все 3 агента в одном процессе (для демо)
    - Для распределённого режима — relay + Docker (docker-compose.yml)
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

from sdk.agent import AgentMesh

# ─────────────────────────────── Конфигурация ───────────────────────────────

ASSET = "BTC"


# ───────────────────── Агент 1: Cryter (источник сигналов) ──────────────────


class CryterAgent:
    """Публикует торговые сигналы в mesh."""

    def __init__(self, db_path: str):
        self.mesh = AgentMesh(
            agent_id="cryter_v10",
            capabilities=["crypto_analysis", "sentiment"],
            db_path=db_path,
        )

    async def start(self):
        await self.mesh.start()
        print(f"[Cryter] Started. DID: {self.mesh.identity.did[:16]}...")
        return self

    async def publish_signal(self, asset: str = ASSET, sentiment: float = 0.32):
        """Публикует сигнал в capability crypto_analysis."""
        signal = {
            "asset": asset,
            "type": "signal",
            "sentiment": sentiment,
            "source": "cryter_v10",
            "ts": time.time(),
        }
        msg_id = await self.mesh.emit("crypto_analysis", signal)
        print(f"[Cryter] Published signal: {asset} sentiment={sentiment} msg={msg_id[:12]}...")
        return msg_id

    async def stop(self):
        await self.mesh.stop()


# ─────────────────── Агент 2: Forecaster (анализ сигналов) ──────────────────


class ForecasterAgent:
    """Подписан на crypto_analysis, публикует прогнозы в forecasting."""

    def __init__(self, db_path: str):
        self.mesh = AgentMesh(
            agent_id="forecaster_v2",
            capabilities=["forecasting", "crypto_analysis"],
            db_path=db_path,
        )
        self.received_signals: list[dict] = []
        self.published_forecasts: list[dict] = []

    async def start(self):
        await self.mesh.start()
        # Подписка на сигналы от Cryter
        await self.mesh.listen(
            {"capability": "crypto_analysis"},
            self._on_signal,
        )
        print(f"[Forecaster] Started. DID: {self.mesh.identity.did[:16]}...")
        return self

    def _on_signal(self, event: dict):
        """Обработчик входящего сигнала."""
        payload = event.get("payload", event)
        self.received_signals.append(payload)

        asset = payload.get("asset", "UNKNOWN")
        sentiment = payload.get("sentiment", 0.5)
        print(f"  [Forecaster] Received signal: {asset} sentiment={sentiment}")

        # Анализируем и публикуем прогноз
        forecast = {
            "asset": asset,
            "type": "forecast",
            "prediction": "bullish" if sentiment > 0.5 else "bearish",
            "confidence": abs(sentiment - 0.5) * 2,  # 0..1 от силы сигнала
            "source": "forecaster_v2",
            "input_sentiment": sentiment,
            "ts": time.time(),
        }
        # Создаём задачу для emit (не блокируем callback)
        asyncio.create_task(self._publish_forecast(forecast))

    async def _publish_forecast(self, forecast: dict):
        """Публикует прогноз в capability forecasting."""
        msg_id = await self.mesh.emit("forecasting", forecast)
        self.published_forecasts.append(forecast)
        print(
            f"  [Forecaster] Published forecast: {forecast['asset']} → {forecast['prediction']} "
            f"(conf={forecast['confidence']:.2f}) msg={msg_id[:12]}..."
        )

    async def stop(self):
        await self.mesh.stop()


# ───────────────────── Агент 3: Creator (создание контента) ──────────────────


class CreatorAgent:
    """Подписан на forecasting, создаёт контент."""

    def __init__(self, db_path: str):
        self.mesh = AgentMesh(
            agent_id="creator_v3",
            capabilities=["content_creation", "forecasting"],
            db_path=db_path,
        )
        self.received_forecasts: list[dict] = []
        self.created_content: list[str] = []

    async def start(self):
        await self.mesh.start()
        # Подписка на прогнозы от Forecaster
        await self.mesh.listen(
            {"capability": "forecasting"},
            self._on_forecast,
        )
        print(f"[Creator] Started. DID: {self.mesh.identity.did[:16]}...")
        return self

    def _on_forecast(self, event: dict):
        """Обработчик прогноза — создаёт контент."""
        payload = event.get("payload", event)
        self.received_forecasts.append(payload)

        asset = payload.get("asset", "UNKNOWN")
        prediction = payload.get("prediction", "neutral")
        confidence = payload.get("confidence", 0.5)

        # Генерируем контент
        post = (
            f"📊 SNIN Signal: {asset} is {prediction.upper()} "
            f"(confidence: {confidence:.0%})\n"
            f"Source: Cryter → Forecaster → Creator via P2P Mesh\n"
            f"No Kafka. No Redis. No REST."
        )
        self.created_content.append(post)
        print(f"  [Creator] Content created: {post[:80]}...")

        # Публикуем результат
        asyncio.create_task(self._publish_content(post, asset, prediction))

    async def _publish_content(self, post: str, asset: str, prediction: str):
        """Публикует созданный контент."""
        content_msg = {
            "asset": asset,
            "type": "content",
            "prediction": prediction,
            "text": post,
            "source": "creator_v3",
            "ts": time.time(),
        }
        msg_id = await self.mesh.emit("content_creation", content_msg)
        print(f"  [Creator] Published content msg={msg_id[:12]}...")

    async def stop(self):
        await self.mesh.stop()


# ─────────────────────────── Запуск цепочки ─────────────────────────


async def run_chain(
    signal_sentiment: float = 0.32,
    timeout: float = 10.0,
    db_prefix: str = "pilot",
) -> tuple[CryterAgent, ForecasterAgent, CreatorAgent]:
    """Запускает 3-агентную цепочку и публикует 1 сигнал."""
    db1 = tempfile.mktemp(suffix=f"_{db_prefix}_cryter.db")
    db2 = tempfile.mktemp(suffix=f"_{db_prefix}_forecaster.db")
    db3 = tempfile.mktemp(suffix=f"_{db_prefix}_creator.db")

    cryter = CryterAgent(db1)
    forecaster = ForecasterAgent(db2)
    creator = CreatorAgent(db3)

    # Стартуем всех
    await asyncio.gather(
        cryter.start(),
        forecaster.start(),
        creator.start(),
    )

    # Даём время на подписки
    await asyncio.sleep(1)

    print(f"\n{'=' * 60}")
    print("SNIN DAO Pilot — Chain Start")
    print("  Cryter (signal) → Forecaster (forecast) → Creator (content)")
    print(f"  Signal: BTC sentiment={signal_sentiment}")
    print(f"{'=' * 60}\n")

    # Cryter публикует сигнал
    await cryter.publish_signal(asset=ASSET, sentiment=signal_sentiment)

    # Ждём прохождения по цепочке
    await asyncio.sleep(timeout)

    print(f"\n{'=' * 60}")
    print("Chain Results:")
    print(f"  Forecaster received {len(forecaster.received_signals)} signals")
    print(f"  Forecaster published {len(forecaster.published_forecasts)} forecasts")
    print(f"  Creator received {len(creator.received_forecasts)} forecasts")
    print(f"  Creator created {len(creator.created_content)} posts")
    print(f"{'=' * 60}\n")

    # Стопаем
    await asyncio.gather(
        cryter.stop(),
        forecaster.stop(),
        creator.stop(),
    )

    # Чистим временные файлы
    for p in [db1, db2, db3]:
        try:
            os.unlink(p)
        except OSError:
            pass

    return cryter, forecaster, creator


def main():
    """Точка входа: `python3 -m pilot.snin_dao_chain`"""
    asyncio.run(run_chain(signal_sentiment=0.78, timeout=8.0))


if __name__ == "__main__":
    main()
