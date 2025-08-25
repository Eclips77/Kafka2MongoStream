"""
publisher_api.py
----------------
FastAPI publisher service that fetches messages from the 20 Newsgroups dataset
and publishes them to Kafka topics "interesting" and "not_interesting".

Endpoints:
    - GET /publish : Fetch 20 messages (10 per group, one per category) and publish to Kafka.

Environment Variables:
    - KAFKA_BOOTSTRAP: Comma-separated Kafka bootstrap servers (default: "localhost:9092")
    - KAFKA_TOPIC_INTERESTING: Topic name for interesting messages (default: "interesting")
    - KAFKA_TOPIC_NOT_INTERESTING: Topic name for not interesting messages (default: "not_interesting")

Run:
    uvicorn publisher_api:app --host 0.0.0.0 --port 8000
"""

import os
import json
from typing import Dict, Any

from fastapi import FastAPI
from kafka import KafkaProducer

from publisher.groupsempler import DataAnalyzer

app = FastAPI(title="Publisher Service", version="1.0.0")


def get_producer() -> KafkaProducer:
    """
    Create a Kafka producer instance.

    Returns:
        A configured KafkaProducer.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        acks="all",
    )
    return producer


@app.get("/publish")
def publish_messages() -> Dict[str, Any]:
    """
    Fetch and publish 20 messages (10 per group) to Kafka.

    Returns:
        JSON with number of messages published per topic.
    """
    topic_interesting = os.getenv("KAFKA_TOPIC_INTERESTING", "interesting")
    topic_not_interesting = os.getenv("KAFKA_TOPIC_NOT_INTERESTING", "not_interesting")

    analyzer = DataAnalyzer(subset="train")
    data = analyzer.sample_messages()

    prod = get_producer()
    counts = {"interesting": 0, "not_interesting": 0}

    for item in data["interesting"]:
        prod.send(topic_interesting, value=item)
        counts["interesting"] += 1
    for item in data["not_interesting"]:
        prod.send(topic_not_interesting, value=item)
        counts["not_interesting"] += 1

    prod.flush()
    return {"published": counts}
