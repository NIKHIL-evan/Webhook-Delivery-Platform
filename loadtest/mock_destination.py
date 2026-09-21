import threading
import time
from fastapi import FastAPI, Request

app = FastAPI()

_lock = threading.Lock()
_stats = {"count": 0, "first_ts": None, "last_ts": None}

@app.post("/webhook")
async def receive_webhook(request: Request):
    now = time.time()
    with _lock:
        _stats["count"] += 1
        if _stats["first_ts"] is None:
            _stats["first_ts"] = now
        _stats["last_ts"] = now
    return {"status": "received"}

@app.get("/stats")
async def get_stats():
    with _lock:
        elapsed = (_stats["last_ts"] - _stats["first_ts"]) if _stats["first_ts"] else 0
        rps = (_stats["count"] / elapsed) if elapsed > 0 else 0
        return {"total_received": _stats["count"], "elapsed_seconds": round(elapsed, 2), "avg_delivery_rps": round(rps, 2)}

@app.post("/stats/reset")
async def reset_stats():
    with _lock:
        _stats["count"] = 0
        _stats["first_ts"] = None
        _stats["last_ts"] = None
    return {"status": "reset"}