use crate::schema::NormalizedBook;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn normalize_binance(msg: &Value, symbol: &str) -> Option<NormalizedBook> {
    let bids = msg.get("bids")?.as_array()?;
    let asks = msg.get("asks")?.as_array()?;

    let mut b = Vec::with_capacity(bids.len());
    let mut bv = Vec::with_capacity(bids.len());
    
    for bid in bids {
        let arr = bid.as_array()?;
        b.push(arr.get(0)?.as_str()?.parse().ok()?);
        bv.push(arr.get(1)?.as_str()?.parse().ok()?);
    }

    let mut a = Vec::with_capacity(asks.len());
    let mut av = Vec::with_capacity(asks.len());
    
    for ask in asks {
        let arr = ask.as_array()?;
        a.push(arr.get(0)?.as_str()?.parse().ok()?);
        av.push(arr.get(1)?.as_str()?.parse().ok()?);
    }

    let timestamp = msg
        .get("E")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        });

    Some(NormalizedBook {
        symbol: normalize_symbol("BINANCE", symbol),
        b,
        bv,
        a,
        av,
        timestamp,
    })
}

pub fn normalize_coinbase(msg: &Value, product_id: &str) -> Option<NormalizedBook> {
    // print out the message for debugging
    println!("Coinbase message: {}", msg);

    let changes = msg.get("changes")?.as_array()?;

    let mut b = Vec::new();
    let mut bv = Vec::new();
    let mut a = Vec::new();
    let mut av = Vec::new();

    for change in changes {
        let arr = change.as_array()?;
        let side = arr.get(0)?.as_str()?;
        let price: f64 = arr.get(1)?.as_str()?.parse().ok()?;
        let size: f64 = arr.get(2)?.as_str()?.parse().ok()?;

        match side {
            "buy" => {
                b.push(price);
                bv.push(size);
            }
            "sell" => {
                a.push(price);
                av.push(size);
            }
            _ => {}
        }
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    Some(NormalizedBook {
        symbol: normalize_symbol("COINBASE", product_id),
        b,
        bv,
        a,
        av,
        timestamp,
    })
}

fn normalize_symbol(exchange: &str, raw: &str) -> String {
    match exchange {
        "BINANCE" => {
            if raw.to_uppercase().ends_with("USDT") {
                let base = &raw[..raw.len() - 4];
                format!("{}-USDT", base.to_uppercase())
            } else {
                raw.to_uppercase()
            }
        }
        "COINBASE" => raw.to_uppercase(),
        _ => raw.to_uppercase(),
    }
}