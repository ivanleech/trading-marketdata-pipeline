import time

from src.common.schemas import NormalizedBook, PriceLevel
from src.common.symbols import normalize_symbol


def normalize_binance(msg, symbol_raw: str):
    print("Normalizing Binance message:", msg)
    bids = [PriceLevel(price=float(p), size=float(s)) for p, s in msg["bids"]]
    asks = [PriceLevel(price=float(p), size=float(s)) for p, s in msg["asks"]]
    print("Best Bid:", bids[0] if bids else "N/A", "Best Ask:", asks[0] if asks else "N/A")

    # Handle both snapshot and update formats
    sequence = msg.get("u") or msg.get("lastUpdateId", 0)
    event_time = msg.get("E") or msg.get("lastUpdateId", int(time.time() * 1000))

    return NormalizedBook(
        exchange="BINANCE",
        symbol=normalize_symbol("BINANCE", symbol_raw.upper()),
        event_time=event_time,
        sequence=sequence,
        bids=bids,
        asks=asks,
        update_type="snapshot" if "lastUpdateId" in msg else "delta",
    )


def normalize_coinbase(msg, product_id: str):
    print("Normalizing Coinbase message:", msg)
    bids = [PriceLevel(price=float(p), size=float(s)) for p, s, *_ in msg.get("bids", [])]
    asks = [PriceLevel(price=float(p), size=float(s)) for p, s, *_ in msg.get("asks", [])]

    return NormalizedBook(
        exchange="COINBASE",
        symbol=normalize_symbol("COINBASE", product_id),
        event_time=int(time.time() * 1000),
        sequence=msg.get("sequence", 0),
        bids=bids,
        asks=asks,
        update_type=msg.get("type", "delta"),
    )
