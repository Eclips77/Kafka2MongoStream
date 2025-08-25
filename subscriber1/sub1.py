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


def run_consumer(topic:str):
	bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
	consumer = KafkaConsumer(topic,
							 bootstrap_servers=bootstrap.split(","),
							 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
							 auto_offset_reset="earliest",
							 enable_auto_commit=True,
							 group_id="sub1-group")

	db = get_mongo()
	coll = db.messages
	print("Subscriber1 listening on topic", topic)
	for msg in consumer:
		print("Received:", msg.value)
		payload = {"kafka_offset": msg.offset, "partition": msg.partition, "value": msg.value, "timestamp": msg.timestamp}
		coll.insert_one(payload)


def run_consumer(topic: str):
	bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
	consumer = KafkaConsumer(topic,
							 bootstrap_servers=bootstrap.split(","),
							 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
							 auto_offset_reset="earliest",
							 enable_auto_commit=True,
							 group_id="subscriber1-group")
	db = get_mongo()
	coll = db.interesting_messages
	for msg in consumer:
		data = msg.value
		data["timestamp"] = datetime.now(timezone.utc).isoformat()
		coll.insert_one({"kafka_offset": msg.offset, "value": data})









# if __name__ == "__main__":
# 	while True:
# 		try:
# 			run_consumer()
# 		except Exception as e:
# 			print("Subscriber1 error:", e)
# 			time.sleep(5)