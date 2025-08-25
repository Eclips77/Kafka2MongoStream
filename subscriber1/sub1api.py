"""
Subscriber Service
------------------
FastAPI service that consumes messages from a Kafka topic,
adds a timestamp, stores them in MongoDB, and exposes an API
to retrieve stored messages.
"""

from fastapi import FastAPI, BackgroundTasks
import os
import json
import threading
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime, timezone


app = FastAPI()


def get_db():
	uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
	client = MongoClient(uri)
	return client.kafka_streams


def consumer_loop(topic: str, stop_event: threading.Event):
	bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
	consumer = KafkaConsumer(topic,
							 bootstrap_servers=bootstrap.split(","),
							 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
							 auto_offset_reset="earliest",
							 enable_auto_commit=True,
							 group_id="subscriber1-group")
	db = get_db()
	coll = db.interesting_messages
	while not stop_event.is_set():
		for msg in consumer:
			data = msg.value
			data["timestamp"] = datetime.now(timezone.utc).isoformat()
			coll.insert_one({"kafka_offset": msg.offset, "value": data})
			if stop_event.is_set():
				break
		time.sleep(1)


@app.on_event("startup")
def startup_event():
	topic = os.getenv("KAFKA_TOPIC", "test-topic")
	stop_event = threading.Event()
	thread = threading.Thread(target=consumer_loop, args=(topic, stop_event), daemon=True)
	thread.start()
	app.state.consumer_stop = stop_event


@app.on_event("shutdown")
def shutdown_event():
	stop = getattr(app.state, "consumer_stop", None)
	if stop:
		stop.set()


@app.get("/messages")
def get_messages():
	db = get_db()
	coll = db.interesting_messages
	docs = list(coll.find({}, {"_id": 0}))
	return docs

