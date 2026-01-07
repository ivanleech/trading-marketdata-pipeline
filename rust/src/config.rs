use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            kafka_brokers: env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:19092".into()),
            kafka_topic: env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "md.orderbook.normalized".into()),
            log_level: env::var("LOG_LEVEL")
                .unwrap_or_else(|_| "info".into()),
        })
    }
}