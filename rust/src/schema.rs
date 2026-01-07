use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedBook {
    pub symbol: String,
    pub b: Vec<f64>,   // bid prices
    pub bv: Vec<f64>,  // bid volumes
    pub a: Vec<f64>,   // ask prices
    pub av: Vec<f64>,  // ask volumes
    pub timestamp: u64, // epoch milliseconds
}