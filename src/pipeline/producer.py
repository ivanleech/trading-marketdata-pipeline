import asyncio

import ujson
from aiokafka import AIOKafkaProducer
from src.common.config import settings
from src.common.logging import get_logger
from src.exchanges.binance import BinanceStream
from src.exchanges.coinbase import CoinbaseStream
from src.pipeline.normalizer import normalize_binance, normalize_coinbase

logger = get_logger("producer")


async def start_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP, value_serializer=lambda v: ujson.dumps(v).encode())
    await producer.start()
    logger.info("Kafka producer started")

    async def run_binance():
        stream = BinanceStream("BTCUSDT")
        async for msg in stream.listen():
            normalized = normalize_binance(msg, "BTCUSDT").model_dump()
            await producer.send_and_wait(settings.TOPIC_NORMALIZED, normalized)

    async def run_coinbase():
        stream = CoinbaseStream("BTC-USD")
        async for msg in stream.listen():
            if msg.get("type") in ("snapshot", "l2update"):
                normalized = normalize_coinbase(msg, "BTC-USD").model_dump()
                await producer.send_and_wait(settings.TOPIC_NORMALIZED, normalized)

    await asyncio.gather(run_binance(), run_coinbase())


if __name__ == "__main__":
    asyncio.run(start_producer())
