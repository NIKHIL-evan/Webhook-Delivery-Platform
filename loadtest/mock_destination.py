import asyncio
import hashlib
import hmac
import time
import json
import os
import random

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Optional

app = FastAPI()

CONFIG_FILE = "mock_config.json"
_last_mtime = 0

_config = {"secret": None, "delay_ms": 0, "dynamic_latency": False}
_stats = {
    "count": 0,
    "rejected_no_config": 0,
    "rejected_bad_signature": 0,
    "first_ts": None,
    "last_ts": None,
}

def sync_config():
    """Fast check to sync state across all 8 Uvicorn workers."""
    global _last_mtime
    if not os.path.exists(CONFIG_FILE):
        return
    mtime = os.path.getmtime(CONFIG_FILE)
    if mtime > _last_mtime:
        with open(CONFIG_FILE, "r") as f:
            _config.update(json.load(f))
        _last_mtime = mtime

class MockConfig(BaseModel):
    secret: Optional[str] = Field(default=None, min_length=1)
    delay_ms: Optional[int] = Field(default=None, ge=0)
    dynamic_latency: Optional[bool] = Field(default=None)

@app.post("/config")
async def set_config(body: MockConfig):
    sync_config()
    
    if body.secret is not None:
        _config["secret"] = body.secret
    if body.delay_ms is not None:
        _config["delay_ms"] = body.delay_ms
    if body.dynamic_latency is not None:
        _config["dynamic_latency"] = body.dynamic_latency
        
    with open(CONFIG_FILE, "w") as f:
        json.dump(_config, f)
        
    return {"status": "configured"}

@app.post("/webhook")
async def receive_webhook(request: Request):
    sync_config()
    
    secret = _config["secret"]
    if secret is None:
        _stats["rejected_no_config"] += 1
        raise HTTPException(status_code=401, detail="Mock not configured: no signing secret")

    header = request.headers.get("X-Webhook-Signature", "")
    body = await request.body()                      # raw bytes, exactly as sent
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    provided = header.removeprefix("sha256=")

    # Compare bytes, not str: compare_digest raises TypeError on non-ASCII str,
    # and a junk header should be a 401, not a 500.
    if not header.startswith("sha256=") or not hmac.compare_digest(
        provided.encode(), expected.encode()
    ):
        _stats["rejected_bad_signature"] += 1
        raise HTTPException(status_code=401, detail="Invalid signature")

    if _config.get("dynamic_latency"):
        roll = random.random()
        if roll < 0.65:
            await asyncio.sleep(random.uniform(0.03, 0.15))
        elif roll < 0.90:
            await asyncio.sleep(random.uniform(0.15, 0.60))
        else:
            await asyncio.sleep(random.uniform(0.60, 3.00))
    elif _config.get("delay_ms", 0) > 0:
        await asyncio.sleep(_config["delay_ms"] / 1000)

    # No lock needed: there is no await between reading and writing _stats,
    # and the event loop runs one task at a time, so nothing can interrupt this block.
    now = time.time()
    _stats["count"] += 1
    if _stats["first_ts"] is None:
        _stats["first_ts"] = now
    _stats["last_ts"] = now
    return {"status": "received"}

@app.get("/stats")
async def get_stats():
    sync_config()
    
    first, last = _stats["first_ts"], _stats["last_ts"]
    elapsed = (last - first) if first is not None else 0
    rps = _stats["count"] / elapsed if elapsed > 0 else 0
    return {
        "total_received": _stats["count"],
        "rejected_no_config": _stats["rejected_no_config"],
        "rejected_bad_signature": _stats["rejected_bad_signature"],
        "elapsed_seconds": round(elapsed, 2),
        "avg_delivery_rps": round(rps, 2),
        "secret_set": _config["secret"] is not None,
        "delay_ms": _config["delay_ms"],
    }

@app.post("/stats/reset")
async def reset_stats():
    sync_config()
    
    _stats.update(
        count=0,
        rejected_no_config=0,
        rejected_bad_signature=0,
        first_ts=None,
        last_ts=None,
    )
    return {"status": "reset"}