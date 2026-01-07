use anyhow::Result;
use futures_util::{StreamExt};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

pub async fn stream_orderbook(
    symbol: &str,
    tx: tokio::sync::mpsc::Sender<Value>,
) -> Result<()> {
    let url = format!(
        "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
        symbol.to_lowercase()
    );

    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Binance: {}", symbol);
                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                if tx.send(data).await.is_err() {
                                    error!("Channel closed");
                                    return Ok(());
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            warn!("Binance connection closed");
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
                error!("Failed to connect to Binance: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }

        info!("Reconnecting to Binance...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}