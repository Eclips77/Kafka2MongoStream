# Kafka2MongoStream

Simple project: a FastAPI publisher that sends messages to Kafka and two subscriber services that consume from Kafka and insert into MongoDB.

Environment variables used:
- KAFKA_BOOTSTRAP: Kafka bootstrap servers (default: localhost:9092)
- KAFKA_TOPIC: topic name (default: test-topic)
- MONGO_URI: MongoDB connection string (default: mongodb://localhost:27017)

Run publisher API:

```powershell
# install deps
pip install -r requirements.txt

# run publisher
uvicorn publisher.pubapi:app --reload --port 8000
```

Run subscriber (example):

```powershell
# run subscriber script (keeps running, consumes messages)
python subscriber1/sub1.py
python subscriber2/sub2.py
```

The publisher provides a POST /publish endpoint which accepts JSON {"message": "..."} and sends it to Kafka. Subscribers consume and write to MongoDB collection `messages` in database `kafka_streams`.