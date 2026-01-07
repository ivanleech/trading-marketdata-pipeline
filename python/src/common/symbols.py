SYMBOL_MAP = {
    "BINANCE": {
        "BTCUSDT": "BTC-USDT",
        "ETHUSDT": "ETH-USDT",
    },
    "COINBASE": {
        "BTC-USD": "BTC-USDT",
        "ETH-USD": "ETH-USDT",
    },
}


def normalize_symbol(exchange: str, raw: str) -> str:
    return SYMBOL_MAP[exchange].get(raw, raw)
