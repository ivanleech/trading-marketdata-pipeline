from typing import List, Literal

from pydantic import BaseModel


class PriceLevel(BaseModel):
    price: float
    size: float


class NormalizedBook(BaseModel):
    exchange: str
    symbol: str  # canonical e.g. BTC-USDT
    event_time: int  # epoch ms
    sequence: int
    bids: List[PriceLevel]
    asks: List[PriceLevel]
    update_type: Literal["snapshot", "delta"]
