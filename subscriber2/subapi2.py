from fastapi import FastAPI
import os

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "subscriber2 ok"}


@app.get("/config")
def config():
    return {"kafka_bootstrap": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            "kafka_topic": os.getenv("KAFKA_TOPIC", "test-topic"),
            "mongo_uri": os.getenv("MONGO_URI", "mongodb://localhost:27017")}
