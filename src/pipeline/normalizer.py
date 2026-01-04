import time
from datetime import datetime, timezone

from src.common.schemas import NormalizedBook
from src.common.symbols import normalize_symbol


def normalize_binance(msg, symbol_raw: str):
    print("\n Normalizing Binance message:", msg)
    # bids = [PriceLevel(price=float(p), size=float(s)) for p, s in msg["bids"]]
    # asks = [PriceLevel(price=float(p), size=float(s)) for p, s in msg["asks"]]
    prices, sizes = zip(*msg["bids"]) if msg["bids"] else ([], [])
    b = [float(p) for p in prices]
    bv = [float(s) for s in sizes]

    # Asks
    prices, sizes = zip(*msg["asks"]) if msg["asks"] else ([], [])
    a = [float(p) for p in prices]
    av = [float(s) for s in sizes]

    # Handle both snapshot and update formats
    sequence = msg.get("u") or msg.get("lastUpdateId", 0)
    arrive_time = int(time.time() * 1_000)
    timestamp_human = datetime.fromtimestamp(arrive_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"msg: {msg}")
    print(f"Arrive Type: {arrive_time}, Sequence: {sequence}")
    print(f"Best Bid(Volume): {b[0]}({bv[0]}) Best Ask(Volume): {a[0]}({av[0]}), Time: {timestamp_human}, timestamp: {arrive_time}")

    return NormalizedBook(
        exchange="BINANCE",
        symbol=normalize_symbol("BINANCE", symbol_raw.upper()),
        sequence=sequence,
        b=b,
        bv=bv,
        a=a,
        av=av,
        timestamp=arrive_time,
        timestamp_human=timestamp_human,
        update_type="snapshot" if "lastUpdateId" in msg else "delta",
    )


def normalize_coinbase(msg, product_id: str):
    print("Normalizing Coinbase message:", msg)
    prices, sizes, *_ = zip(*msg.get("bids", [])) if msg.get("bids") else ([], [])
    b = [float(p) for p in prices]
    bv = [float(s) for s in sizes]
    prices, sizes, *_ = zip(*msg.get("asks", [])) if msg.get("asks") else ([], [])
    a = [float(p) for p in prices]
    av = [float(s) for s in sizes]
    arrive_time = int(time.time() * 1_000)
    timestamp_human = datetime.fromtimestamp(arrive_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    return NormalizedBook(
        exchange="COINBASE",
        symbol=normalize_symbol("COINBASE", product_id),
        event_time=int(time.time() * 1000),
        sequence=msg.get("sequence", 0),
        b=b,
        bv=bv,
        a=a,
        av=av,
        timestamp=arrive_time,
        timestamp_human=timestamp_human,
        update_type=msg.get("type", "delta"),
    )
