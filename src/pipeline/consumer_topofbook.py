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
            bid = data["bids"][0]["price"] if data["bids"] else None
            ask = data["asks"][0]["price"] if data["asks"] else None
            books[symbol] = (bid, ask)

            logger.info(f"{symbol} | bid {bid} | ask {ask}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run_consumer())
