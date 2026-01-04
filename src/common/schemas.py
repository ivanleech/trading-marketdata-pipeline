from typing import List, Literal

from pydantic import BaseModel


class NormalizedBook(BaseModel):
    exchange: str
    symbol: str  # canonical e.g. BTC-USDT
    sequence: int
    b: List[float]
    bv: List[float]
    a: List[float]
    av: List[float]
    timestamp: int  # epoch ms
    timestamp_human: str
    update_type: Literal["snapshot", "delta"]
