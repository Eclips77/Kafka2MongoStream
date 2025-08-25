import os
import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient


def get_mongo():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    db = client.kafka_streams
    return db


def run_consumer():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "test-topic")
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=bootstrap.split(","),
                             value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                             auto_offset_reset="earliest",
                             enable_auto_commit=True,
                             group_id="subscriber2-group")

    db = get_mongo()
    coll = db.messages_sub2
    print("Subscriber2 listening on topic", topic)
    for msg in consumer:
        print("Subscriber2 Received:", msg.value)
        payload = {"kafka_offset": msg.offset, "partition": msg.partition, "value": msg.value, "timestamp": msg.timestamp}
        coll.insert_one(payload)


if __name__ == "__main__":
    while True:
        try:
            run_consumer()
        except Exception as e:
            print("Subscriber2 error:", e)
            time.sleep(5)
