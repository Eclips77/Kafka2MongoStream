import os
import json
from kafka import KafkaProducer
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

class Publisher:
    def __init__(self):
        bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.default_topic = os.getenv("KAFKA_TOPIC", "intresting")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def publish(self, message: dict, topic: str):
        topic_to_use = topic if topic else self.default_topic
        future = self.producer.send(topic_to_use, message)
        result = future.get(timeout=30)
        return result

publisher = Publisher()
# publisher.publish({"msg": "hello"})
publisher.publish({"msg": "world"}, topic="another_topic")