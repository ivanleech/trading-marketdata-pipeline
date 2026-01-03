import aiohttp
import ujson
from src.common.logging import get_logger

WS_URL = "wss://ws-feed.exchange.coinbase.com"
logger = get_logger("coinbase")


class CoinbaseStream:
    """
    Note: level2, level3, and full channels now require authentication.
    https://docs.cloud.coinbase.com/exchange/docs/websocket-auth
    """

    def __init__(self, product_id: str):
        self.product_id = product_id  # e.g. BTC-USD

    async def listen(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL) as ws:
                await ws.send_json({"type": "subscribe", "channels": [{"name": "level2", "product_ids": [self.product_id]}]})
                logger.info(f"Subscribed to Coinbase {self.product_id}")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        yield ujson.loads(msg.data)
