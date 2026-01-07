-- ClickHouse Market Data Pipeline Initialization
-- This script sets up the complete data flow:
-- Kafka Stream -> Orderbook Depth -> Top of Book
-- Create database
CREATE DATABASE IF NOT EXISTS marketdata;
USE marketdata;
-- ============================================================================
-- 1. Kafka Stream Table (Consumer)
-- ============================================================================
-- Continuously reads from Redpanda/Kafka topic
CREATE TABLE IF NOT EXISTS md_orderbook_stream_kafka (
    symbol String,
    b Array(Float64),
    -- bid prices
    bv Array(Float64),
    -- bid volumes
    a Array(Float64),
    -- ask prices
    av Array(Float64),
    -- ask volumes
    timestamp UInt64 -- epoch milliseconds
) ENGINE = Kafka SETTINGS kafka_broker_list = 'redpanda:9092',
kafka_topic_list = 'md.orderbook.normalized',
kafka_group_name = 'clickhouse-depth-sink_v1',
kafka_format = 'JSONEachRow',
kafka_num_consumers = 2;
-- ============================================================================
-- 2. Orderbook Depth Storage Table
-- ============================================================================
-- Stores full order book depth with all price levels
CREATE TABLE IF NOT EXISTS md_orderbook_depth (
    symbol LowCardinality(String),
    b Array(Float64),
    bv Array(Float64),
    a Array(Float64),
    av Array(Float64),
    timestamp UInt64,
    ingest_time DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (symbol, timestamp);
-- ============================================================================
-- 3. Materialized View: Kafka -> Depth Table
-- ============================================================================
-- Automatically populates md_orderbook_depth from Kafka stream
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_md_orderbook_depth TO md_orderbook_depth AS
SELECT symbol,
    b,
    bv,
    a,
    av,
    timestamp
FROM md_orderbook_stream_kafka;
-- ============================================================================
-- 4. Top of Book Storage Table
-- ============================================================================
-- Stores only the best bid and ask (level 1)
CREATE TABLE IF NOT EXISTS md_top_of_book (
    symbol LowCardinality(String),
    bid_price Float64,
    bid_volume Float64,
    ask_price Float64,
    ask_volume Float64,
    timestamp UInt64
) ENGINE = MergeTree
ORDER BY (symbol, timestamp);
-- ============================================================================
-- 5. Materialized View: Depth -> Top of Book
-- ============================================================================
-- Automatically extracts best bid/ask from depth table
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_md_top_of_book TO md_top_of_book AS
SELECT symbol,
    b [1] AS bid_price,
    bv [1] AS bid_volume,
    a [1] AS ask_price,
    av [1] AS ask_volume,
    timestamp
FROM md_orderbook_depth
WHERE length(b) > 0
    AND length(a) > 0;
-- ============================================================================
-- Verification Queries
-- ============================================================================
-- Uncomment these to verify the setup:
-- Show all tables
-- SHOW TABLES;
-- Check Kafka stream is consuming
-- SELECT * FROM md_orderbook_stream_kafka LIMIT 5;
-- Check depth table has data
-- SELECT symbol, timestamp, length(b) as bid_levels, length(a) as ask_levels 
-- FROM md_orderbook_depth 
-- ORDER BY timestamp DESC LIMIT 10;
-- Check top of book
-- SELECT * FROM md_top_of_book ORDER BY timestamp DESC LIMIT 10;