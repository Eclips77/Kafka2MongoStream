from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os

from publisher.pub import publisher


class PublishRequest(BaseModel):
	message: dict


app = FastAPI()


@app.get("/health")
def health():
	return {"status": "ok"}


@app.post("/publish")
def publish(req: PublishRequest):
	try:
		res = publisher.publish(req.message,"f")
		return {"status": "sent", "kafka_offset": getattr(res, 'offset', None)}
	except Exception as e:
		raise HTTPException(status_code=500, detail=str(e))
