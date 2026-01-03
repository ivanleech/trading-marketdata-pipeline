# Trading Market Data Pipeline

> Real-time cryptocurrency market data ingestion and normalization pipeline

Stream order book updates from **Binance** and **Coinbase**, normalize them into a unified format, and publish to a Kafka-compatible broker (Redpanda). Includes a consumer for real-time top-of-book analytics.

---

## 📁 Project Structure

```
trading-market-data-pipeline/
├── src/
│   ├── common/           # Shared configuration, logging, schemas, symbol mapping
│   ├── exchanges/        # Exchange-specific WebSocket clients (Binance, Coinbase)
│   └── pipeline/         # Data normalization, producer, and consumer scripts
├── docker-compose.yml    # Local Redpanda + Console setup
├── requirements.txt      # Project dependencies
├── requirements.lock     # Pinned dependencies (uv managed)
└── .venv/               # Virtual environment
```

---

## 🚀 Quick Start

### 1. Set up Python environment

```bash
# Create virtual environment with Python 3.12
uv venv --python 3.12

# Activate virtual environment
source .venv/bin/activate  # Linux/Mac
# OR
.venv\Scripts\activate     # Windows

# Install dependencies
uv pip install -r requirements.txt
```

### 2. Start Redpanda

```bash
docker compose up -d
```

**Services:**
- **Kafka Broker:** `localhost:19092` (external access)
- **Redpanda Console:** http://localhost:8080

### 3. Run the pipeline

**Start the producer** (streams data to Redpanda):
```bash
export KAFKA_BROKERS="localhost:19092"
python -m src.pipeline.producer
```

**Start the consumer** (displays top-of-book analytics):
```bash
export KAFKA_BROKERS="localhost:19092"
python -m src.pipeline.consumer_topofbook
```

---

## 🔧 How It Works

### 1. **WebSocket Ingestion**
- `src/exchanges/binance.py` and `src/exchanges/coinbase.py` connect to exchange WebSocket APIs
- Subscribe to Level 2 order book streams for real-time updates

### 2. **Data Normalization**
- `src/pipeline/normalizer.py` converts exchange-specific messages into a unified schema
- **NormalizedBook** schema includes:
  - `symbol` - Canonical symbol (e.g., `BTC-USD`)
  - `bids` & `asks` - Top 20 price levels
  - `event_time` - Event timestamp
  - `sequence` - Message sequence number
  - `exchange` - Source exchange identifier

### 3. **Kafka Streaming**
- `src/pipeline/producer.py` publishes normalized messages to Redpanda/Kafka topics
- Ensures reliable, ordered delivery of market data updates

### 4. **Real-time Analytics**
- `src/pipeline/consumer_topofbook.py` maintains per-symbol best bid/ask
- Calculates and displays:
  - **Mid-price:** `(best_bid + best_ask) / 2`
  - **Spread:** `best_ask - best_bid`
  - **Spread %:** `(spread / mid_price) × 100`

---

## 📊 Architecture

```
┌─────────────┐       ┌─────────────┐
│   Binance   │       │  Coinbase   │
│  WebSocket  │       │  WebSocket  │
└──────┬──────┘       └──────┬──────┘
       │                     │
       │    Exchange Data    │
       └──────────┬──────────┘
                  │
         ┌────────▼────────┐
         │   Normalizer    │
         │  (Unified Schema)│
         └────────┬────────┘
                  │
         ┌────────▼────────┐
         │   Producer      │
         │  (Kafka Client) │
         └────────┬────────┘
                  │
         ┌────────▼────────┐
         │    Redpanda     │
         │  (Kafka Broker) │
         └────────┬────────┘
                  │
         ┌────────▼────────┐
         │   Consumer      │
         │ (Top-of-Book)   │
         └─────────────────┘
```

---

## 🛠️ Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Kafka Configuration
KAFKA_BROKERS=localhost:19092

# Logging
LOG_LEVEL=INFO
```

### Docker Compose Ports

| Service | Internal Port | External Port | Purpose |
|---------|--------------|---------------|---------|
| Redpanda | `9092` | `19092` | Kafka protocol (external) |
| Redpanda | `9092` | `9092` | Kafka protocol (internal) |
| Redpanda | `9644` | `9644` | Admin API |
| Console | `8080` | `8080` | Web UI |

---

## 📦 Dependencies

- **aiokafka** - Async Kafka client
- **aiohttp** - Async HTTP client for WebSocket connections
- **pydantic** - Data validation and settings management
- **pydantic-settings** - Configuration management
- **ujson** - Fast JSON serialization

---

## 🧪 Development

### Stop services
```bash
docker compose down
```

### View Redpanda logs
```bash
docker compose logs -f redpanda
```

### Access Redpanda Console
Visit http://localhost:8080 to:
- View topics and messages
- Monitor consumer groups
- Inspect message schemas


### Example screenshots
![Redpanda Console Topics](assets/redpanda_console_topics.JPG)
![Redpanda Console Messages](assets/producer_consumer_logs.JPG)

---

## 📝 License

MIT

---

## 🤝 Contributing

Contributions welcome! Please open an issue or submit a pull request.