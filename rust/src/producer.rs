use crate::{config::Config, normalizer, schema::NormalizedBook};
use anyhow::Result;
use kafka::producer::{Producer as KafkaProducer, Record, RequiredAcks};
use serde_json::Value;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::time::Duration;
use tokio::time;
use tracing::{error, info};

pub struct Producer {
    producer: Arc<Mutex<KafkaProducer>>,
    topic: String,
    sent_count: Arc<AtomicU64>,
}

impl Producer {
    pub fn new(config: &Config) -> Result<Self> {
        let producer = KafkaProducer::from_hosts(vec![config.kafka_brokers.clone()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()?;

        Ok(Self {
            producer: Arc::new(Mutex::new(producer)),
            topic: config.kafka_topic.clone(),
            sent_count: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn send(&self, book: &NormalizedBook) -> Result<()> {
        let payload = serde_json::to_string(book)?;
        let mut producer = self.producer.lock().unwrap();
        producer.send(&Record::from_value(&self.topic, payload))?;
        self.sent_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn get_count(&self) -> u64 {
        self.sent_count.load(Ordering::Relaxed)
    }
}

pub async fn run(config: Config) -> Result<()> {
    let producer = Arc::new(Producer::new(&config)?);
    info!("Kafka producer started");

    // Metrics reporter (messages/sec)
    let producer_clone = producer.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(10));
        let mut last_count = 0u64;
        loop {
            interval.tick().await;
            let current_count = producer_clone.get_count();
            let rate = (current_count - last_count) / 10;
            info!("Messages sent: {} total, {} msg/sec", current_count, rate);
            last_count = current_count;
        }
    });

    let (binance_tx, mut binance_rx) = tokio::sync::mpsc::channel::<Value>(1000);
    let (coinbase_tx, mut coinbase_rx) = tokio::sync::mpsc::channel::<Value>(1000);

    // Spawn exchange streams
    // let binance_handle = tokio::spawn(async move {
    //     if let Err(e) = crate::exchanges::binance::stream_orderbook("btcusdt", binance_tx).await {
    //         error!("Binance stream error: {}", e);
    //     }
    // });

    let coinbase_handle = tokio::spawn(async move {
        if let Err(e) = crate::exchanges::coinbase::stream_orderbook("BTC-USD", coinbase_tx).await {
            error!("Coinbase stream error: {}", e);
        }
    });

    // Process messages from exchanges
    loop {
        tokio::select! {
            // Some(msg) = binance_rx.recv() => {
            //     if let Some(normalized) = normalizer::normalize_binance(&msg, "BTCUSDT") {
            //         let producer = producer.clone();
            //         tokio::spawn(async move {
            //             if let Err(e) = producer.send(&normalized) {
            //                 error!("Failed to send Binance message: {}", e);
            //             }
            //         });
            //     }
            // }
            Some(msg) = coinbase_rx.recv() => {
                if let Some(normalized) = normalizer::normalize_coinbase(&msg, "BTC-USD") {
                    let producer = producer.clone();
                    tokio::spawn(async move {
                        if let Err(e) = producer.send(&normalized) {
                            error!("Failed to send Coinbase message: {}", e);
                        }
                    });
                }
            }
            // print out the message if both channels are closed
            
            else => break,
        }
    }

    // binance_handle.await?;
    coinbase_handle.await?;

    Ok(())
}
