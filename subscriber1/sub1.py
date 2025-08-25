"""
consumer_interesting_service.py
-------------------------------
FastAPI service that consumes messages from Kafka topic "interesting" and stores them
in MongoDB collection "interesting_messages". Also provides an API to fetch stored messages.

Behavior:
    - A background thread starts on app startup and continuously consumes messages.
    - Each consumed message is stored with: partition, kafka_offset, topic, timestamp (Kafka event time), value.

Endpoints:
    - GET /messages : Return all stored messages (most recent first).
    - GET /health   : Health check endpoint.

Environment Variables:
    - KAFKA_BOOTSTRAP: Comma-separated Kafka bootstrap servers (default: "localhost:9092")
    - KAFKA_TOPIC_INTERESTING: Topic name (default: "interesting")
    - GROUP_ID: Kafka consumer group id (default: "interesting-subscriber-group")
    - MONGO_URI: MongoDB connection string (default: "mongodb://localhost:27017")
    - MONGO_DB: MongoDB database name (default: "newsdb")

Run:
    uvicorn consumer_interesting_service:app --host 0.0.0.0 --port 8101
"""

import os
import json
import time
import threading
from datetime import datetime, timezone
from typing import List, Dict, Any

from fastapi import FastAPI
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, errors


app = FastAPI(title="Interesting Subscriber Service", version="1.0.0")

TOPIC_KEY = "KAFKA_TOPIC_INTERESTING"
DEFAULT_TOPIC = "interesting"
COLLECTION_NAME = "interesting_messages"

_running = False
_worker: threading.Thread | None = None


def _iso_utc_from_kafka_ts(ts_ms: int | None) -> str:
    """
    Convert Kafka millisecond timestamp to ISO UTC string.

    Args:
        ts_ms: Milliseconds since epoch.

    Returns:
        ISO 8601 UTC string.
    """
    if ts_ms is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def get_mongo_collection():
    """
    Build MongoDB client and return the target collection.

    Returns:
        A PyMongo collection instance.
    """
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB", "newsdb")
    client = MongoClient(uri, tz_aware=True, uuidRepresentation="standard")
    coll = client[db_name][COLLECTION_NAME]
    coll.create_index([("partition", ASCENDING), ("kafka_offset", ASCENDING)], unique=True)
    coll.create_index([("timestamp", ASCENDING)])
    return coll


def build_consumer() -> KafkaConsumer:
    """
    Create a Kafka consumer configured for the 'interesting' topic.

    Returns:
        A configured KafkaConsumer instance.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
    group_id = os.getenv("GROUP_ID", "interesting-subscriber-group")
    topic = os.getenv(TOPIC_KEY, DEFAULT_TOPIC)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest"),
        group_id=group_id,
        max_poll_interval_ms=int(os.getenv("MAX_POLL_INTERVAL_MS", "300000")),
        session_timeout_ms=int(os.getenv("SESSION_TIMEOUT_MS", "10000")),
    )
    return consumer


def _consume_loop():
    """
    Background worker loop that consumes messages and stores them in MongoDB.
    Commits offsets only after successful insertion to achieve at-least-once semantics.
    """
    global _running
    coll = get_mongo_collection()
    consumer = build_consumer()
    try:
        while _running:
            for msg in consumer:
                if not _running:
                    break
                try:
                    doc = {
                        "partition": msg.partition,
                        "kafka_offset": msg.offset,
                        "topic": msg.topic,
                        "timestamp": _iso_utc_from_kafka_ts(msg.timestamp),
                        "value": msg.value,  # already JSON-decoded
                    }
                    coll.insert_one(doc)
                    consumer.commit()
                except errors.DuplicateKeyError:
                    consumer.commit()
                except Exception:
                    # Do not commit—message will be retried
                    pass
            time.sleep(0.1)
    finally:
        try:
            consumer.close()
        except Exception:
            pass


@app.on_event("startup")
def _on_startup():
    """
    Start the background consumer loop on application startup.
    """
    global _running, _worker
    _running = True
    _worker = threading.Thread(target=_consume_loop, daemon=True)
    _worker.start()


@app.on_event("shutdown")
def _on_shutdown():
    """
    Signal the background worker to stop on application shutdown.
    """
    global _running, _worker
    _running = False
    if _worker and _worker.is_alive():
        _worker.join(timeout=5.0)


@app.get("/messages")
def get_messages() -> List[Dict[str, Any]]:
    """
    Retrieve all stored messages for the 'interesting' subscriber.

    Returns:
        A list of stored message documents (most recent first).
    """
    coll = get_mongo_collection()
    docs = list(coll.find().sort("timestamp", -1))
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs


@app.get("/health")
def health() -> Dict[str, str]:
    """
    Health check endpoint.

    Returns:
        A small JSON indicating the service status.
    """
    return {"status": "ok"}
