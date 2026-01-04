import asyncio

import ujson
from aiokafka import AIOKafkaConsumer
from src.common.config import settings
from src.common.logging import get_logger

logger = get_logger("topofbook")


async def run_consumer():
    consumer = AIOKafkaConsumer(
        settings.TOPIC_NORMALIZED, bootstrap_servers=settings.KAFKA_BOOTSTRAP, value_deserializer=lambda v: ujson.loads(v), group_id="topofbook-consumer"
    )

    await consumer.start()
    logger.info("Top-of-book consumer started")

    books = {}

    try:
        async for msg in consumer:
            data = msg.value
            symbol = data["symbol"]
            bid = data["b"][0] if data["b"] else None
            bid_volume = data["bv"][0] if data["bv"] else None
            ask = data["b"][0] if data["a"] else None
            ask_volume = data["av"][0] if data["av"] else None
            books[symbol] = (bid, ask)

            logger.info(f"{symbol} | Best Bid(Volume): {bid}({bid_volume}) Best Ask(Volume): {ask}({ask_volume})")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run_consumer())
