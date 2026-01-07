# Trading Market Data Pipeline - Rust Implementation

## Setup

```bash
# Build release version (optimized)
cargo build --release

# Run
KAFKA_BROKERS=localhost:19092 cargo run --release
```

## Performance Optimizations

This Rust implementation includes:
- Zero-copy message processing where possible
- Optimized Kafka producer settings (batching, compression)
- Async/await with Tokio for maximum concurrency
- Lock-free atomic counters for metrics
- Release build with LTO and single codegen unit

## Expected Performance

- 50,000-100,000 msg/sec (vs Python's 10,000)
- ~1-2ms latency (vs Python's 5-10ms)
- 10-50x lower memory usage
- CPU usage: 10-20% (vs Python's 50-70%)