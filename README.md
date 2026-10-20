# Real-Time Crypto Analytics Platform

A real-time data engineering pipeline that polls cryptocurrency prices (Bitcoin and Ethereum in USD), publishes them to Apache Kafka, and processes them with Spark Structured Streaming into Delta Lake tables following a medallion architecture. The Bronze and Silver layers are implemented; the Gold layer is planned. The project runs locally (Kafka on `localhost:9092`, Delta tables on the local filesystem).

## Architecture

```text
Crypto price API (API_URL)
        ↓  producer/crypto_producer.py (every 10 s, backoff on errors)
Kafka topic (crypto_prices)
        ↓  spark/bronze_stream.py
Delta Bronze  (delta/bronze_crypto)
        ↓  spark/silver_stream.py
Delta Silver  (delta/silver_crypto, partitioned by event_date)
        ↓
Gold (planned: windowed aggregations)
```

## Tech stack

- Python with `requests`, `kafka-python` (`KafkaProducer`) and `python-dotenv`
- Apache Kafka
- Apache Spark Structured Streaming (PySpark) with the Kafka source
- Delta Lake (`DeltaSparkSessionExtension`, `DeltaCatalog`)

## Repository structure

```text
.
├── producer/
│   └── crypto_producer.py   # Polls the price API and publishes events to Kafka
├── spark/
│   ├── bronze_stream.py     # Kafka → Delta Bronze
│   └── silver_stream.py     # Bronze → cleaned, enriched Delta Silver
├── config/                  # Placeholder (empty)
├── docs/                    # Placeholder (empty)
├── tests/                   # Placeholder (empty)
└── .gitignore               # Ignores .env, delta/, checkpoints/, kafka-logs/, local tool folders
```

## What each component does

**Producer (`producer/crypto_producer.py`)**
- Reads `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` and `API_URL` from a `.env` file.
- Calls the API with `ids=bitcoin,ethereum` and `vs_currencies=usd` and builds an event with `event_time` (UTC ISO timestamp), `bitcoin_usd` and `ethereum_usd`.
- Sends the event as JSON to Kafka every 10 seconds. On HTTP errors (including 429 rate limits) and other failures it doubles the wait time, up to 60 seconds.

**Bronze (`spark/bronze_stream.py`)**
- Subscribes to the `crypto_prices` topic on `localhost:9092` (`startingOffsets=latest`).
- Parses the JSON value with an explicit schema and appends the rows to the Bronze Delta table, using a checkpoint directory for fault-tolerant progress tracking.

**Silver (`spark/silver_stream.py`)**
- Streams from the Bronze Delta table and converts `event_time` to a timestamp.
- Drops rows with null or non-positive prices.
- Applies a 10-minute watermark and removes duplicate events.
- Adds `event_date`, `event_hour`, the `btc_eth_ratio` and a simple `btc_volatility_flag` (`HIGH` when the BTC price is above an example threshold of 100,000, otherwise `NORMAL`).
- Appends to the Silver Delta table partitioned by `event_date`, with its own checkpoint.

## How to run

Prerequisites: Python 3, a running Kafka broker, and Spark with the Delta Lake and Spark-Kafka connector packages available.

1. Create a `.env` with `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` (use `crypto_prices` to match the Spark job) and `API_URL`.
2. Start Kafka and create the topic.
3. Run the producer: `python producer/crypto_producer.py`.
4. Update `BASE_PATH` in both Spark scripts (currently `/home/naman/crypto-streaming-project`), then start the jobs, for example with `spark-submit` and the Delta and Kafka packages:
   - `spark/bronze_stream.py`
   - `spark/silver_stream.py`
5. Query the Delta tables under `delta/` with Spark.

## Planned enhancements

These are from the project roadmap and are not implemented yet:

- Gold layer: 1-minute windowed price aggregations, moving averages, volatility and ranking metrics
- Config files in `config/` and unit tests in `tests/`
- Dockerised Kafka + Spark stack and CI/CD
- Dashboard integration (Superset / Power BI), data quality checks, Prometheus monitoring
- Cloud deployment (Azure / AWS)

## Possible improvements

- The Spark jobs hard-code the Kafka server, topic and base path; reading them from the same environment variables as the producer would keep the components consistent.

## Author

Naman Agarwal
