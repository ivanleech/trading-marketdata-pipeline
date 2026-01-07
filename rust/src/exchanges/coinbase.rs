use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

pub async fn stream_orderbook(
    product_id: &str,
    tx: tokio::sync::mpsc::Sender<Value>,
) -> Result<()> {
    let url = "wss://ws-feed.exchange.coinbase.com";

    loop {
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Coinbase: {}", product_id);
                let (mut write, mut read) = ws_stream.split();

                let subscribe = json!({
                    "type": "subscribe",
                    "product_ids": [product_id],
                    "channels": ["level2_batch"]
                });

                if let Err(e) = write.send(Message::Text(subscribe.to_string())).await {
                    error!("Failed to subscribe: {}", e);
                    continue;
                }

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                if let Some(msg_type) = data.get("type").and_then(|t| t.as_str()) {
                                    if msg_type == "snapshot" || msg_type == "l2update" {
                                        if tx.send(data).await.is_err() {
                                            error!("Channel closed");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            warn!("Coinbase connection closed");
                            break;
                        }
                        Err(e) => {
                            error!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to Coinbase: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }

        info!("Reconnecting to Coinbase...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}