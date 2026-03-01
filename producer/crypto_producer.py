import json
import time
import logging
import os
from datetime import datetime

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
API_URL = os.getenv("API_URL")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

def fetch_crypto_price():
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd"
    }

    response = requests.get(API_URL, params=params)
    response.raise_for_status()

    return response.json()

def build_event(payload):
    return {
        "event_time": datetime.utcnow().isoformat(),
        "bitcoin_usd": payload["bitcoin"]["usd"],
        "ethereum_usd": payload["ethereum"]["usd"]
    }

def main():
    producer = create_producer()
    logging.info("Producer started...")

    backoff = 10  # normal polling interval (seconds)

    while True:
        try:
            raw_data = fetch_crypto_price()
            event = build_event(raw_data)

            producer.send(KAFKA_TOPIC, value=event)
            producer.flush()

            logging.info(f"Sent event: {event}")

            # reset backoff after success
            backoff = 10
            time.sleep(backoff)

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 429:
                logging.warning("Rate limited by API. Applying exponential backoff...")
                backoff = min(backoff * 2, 60)
            else:
                logging.error(f"HTTP error occurred: {http_err}")
                backoff = min(backoff * 2, 60)

            time.sleep(backoff)

        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
            backoff = min(backoff * 2, 60)
            time.sleep(backoff)

if __name__ == "__main__":
    main()
