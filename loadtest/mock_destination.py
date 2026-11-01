import asyncio
import hashlib
import hmac
import time

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

app = FastAPI()

# Set by POST /config. secret=None means "not configured": every webhook is rejected.
_config = {"secret": None, "delay_ms": 0}

_stats = {
    "count": 0,
    "rejected_no_config": 0,
    "rejected_bad_signature": 0,
    "first_ts": None,
    "last_ts": None,
}


class MockConfig(BaseModel):
    secret: str = Field(min_length=1)
    delay_ms: int = Field(default=0, ge=0)   # ge=0: negative delays are refused with a 422


@app.post("/config")
async def set_config(body: MockConfig):
    _config["secret"] = body.secret
    _config["delay_ms"] = body.delay_ms
    # Never echo the secret back: responses end up in terminals and logs.
    return {"status": "configured", "delay_ms": body.delay_ms}


@app.post("/webhook")
async def receive_webhook(request: Request):
    secret = _config["secret"]
    if secret is None:
        # Reject instead of skipping: a check that silently turns itself off
        # would make a broken signer look correct.
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

    delay_ms = _config["delay_ms"]
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000)         # await: other requests keep running

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
    first, last = _stats["first_ts"], _stats["last_ts"]
    elapsed = (last - first) if first is not None else 0
    rps = _stats["count"] / elapsed if elapsed > 0 else 0
    return {
        "total_received": _stats["count"],
        "rejected_no_config": _stats["rejected_no_config"],
        "rejected_bad_signature": _stats["rejected_bad_signature"],
        "elapsed_seconds": round(elapsed, 2),
        "avg_delivery_rps": round(rps, 2),
        "secret_set": _config["secret"] is not None,   # shows THAT it's set, never the value
        "delay_ms": _config["delay_ms"],
    }


@app.post("/stats/reset")
async def reset_stats():
    _stats.update(
        count=0,
        rejected_no_config=0,
        rejected_bad_signature=0,
        first_ts=None,
        last_ts=None,
    )
    return {"status": "reset"}