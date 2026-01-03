import asyncio

import aiohttp
import ujson
from src.common.logging import get_logger

WS_URL = "wss://stream.binance.com:9443/ws"
logger = get_logger("binance")


class BinanceStream:
    def __init__(self, symbol: str):
        self.symbol = symbol.lower()  # e.g. btcusdt

    async def listen(self):
        stream = f"{self.symbol}@depth20@100ms"
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"{WS_URL}/{stream}") as ws:
                logger.info(f"Subscribed to Binance {self.symbol}")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        yield ujson.loads(msg.data)
                    else:
                        logger.warning("Non-text frame received")
