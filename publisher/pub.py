import os
import json
from kafka import KafkaProducer
from publisher.groupsempler import NewsgroupSampler
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

class Publisher:
    def __init__(self):
        bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.sampler = NewsgroupSampler()


    def publish(self, message: dict, topic: str):
        """Publish a single message dict to the given Kafka topic."""
        future = self.producer.send(topic, message)
        result = future.get(timeout=30)
        return result

    def send_messages_to_topics(self):
        """Fetch messages from the sampler, classify and send to topics.

        Simple classifier: category names containing 'comp', 'sci', or 'rec' are treated as interesting
        (this is a placeholder; you can replace with a real model).
        """
        samples = self.sampler.get_sample()
        interesting = samples.get("interesting", [])
        not_interesting = samples.get("not_interesting", [])

        sent = {"interesting": 0, "not_interesting": 0}

        for m in interesting:
            payload = {"text": m}
            self.publish(payload, topic="interesting")
            sent["interesting"] += 1

        for m in not_interesting:
            payload = {"text": m}
            self.publish(payload, topic="not_interesting")
            sent["not_interesting"] += 1

        return sent
        
    


publisher = Publisher()

if __name__ == "__main__":
    print("Sending sample messages to Kafka...")
    print(publisher.send_messages_to_topics())