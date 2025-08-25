import os
import json
from kafka import KafkaProducer


class Publisher:
	def __init__(self):
		bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
		self.topic = os.getenv("KAFKA_TOPIC", "intresting")
		self.producer = KafkaProducer(bootstrap_servers=bootstrap.split(","),
									  value_serializer=lambda v: json.dumps(v).encode("utf-8"))

	def publish(self, message: dict):
		future = self.producer.send(self.topic, message)
		result = future.get(timeout=30)
		return result


publisher = Publisher()    