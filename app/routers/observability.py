from fastapi import APIRouter, Depends
from app.redis_client import redis_client
from app.database import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import time
from datetime import datetime, timezone
from fastapi import Response

router = APIRouter(tags=["observability"])

@router.get("/health")
async def basic_health():
    """Liveness probe for load balancers."""
    return {"status": "healthy"}

@router.get("/health/details")
async def detailed_health(response: Response, db: AsyncSession = Depends(get_db)):
    """Readiness probe verifying backend connections."""

    health_status = {
    "status": "healthy",
    "postgres": "unknown",
    "redis": "unknown",
    "worker_loop": "unknown",
    "retry_loop": "unknown"
    }

    postgres_status = "down"
    redis_status = "down"
    workers_status = "down"
    retry_scheduler_status = "down"
    # POSTGRES
    try:
        await db.execute(text("SELECT 1"))
        postgres_status = "up"
        health_status["postgres"] = postgres_status
    except Exception:
        postgres_status = "down"
        health_status["postgres"] = postgres_status

    # REDIS
    try:
        await redis_client.ping()
        redis_status = "up"
        health_status["redis"] = redis_status
    except Exception:
        redis_status = "down"
        health_status["redis"] = redis_status

    if redis_status == "up":

        # WORKER LOOP
        worker_last_seen = await redis_client.get("worker:worker-1:last_seen")
        if worker_last_seen is None:
            workers_status = "down"
        elif time.time() - float(worker_last_seen) > 30:
            workers_status = "down"
        else:
            workers_status = "up"
            health_status["worker_loop"] = workers_status

        # RETRY SCHEDULER
        retry_scheduler_last_seen = await redis_client.get("retry_scheduler:last_seen")
        if retry_scheduler_last_seen is None:
            retry_scheduler_status = "down"
        elif time.time() - float(retry_scheduler_last_seen) > 30:
            retry_scheduler_status = "down"
        else:
            retry_scheduler_status = "up"
            health_status["retry_loop"] = retry_scheduler_status
        
    # STATUS
    if postgres_status == "down":
        health_status["status"] = "unhealthy"

    elif redis_status == "down":
        health_status["status"] = "unhealthy"
        health_status["worker_loop"] = "unknown"
        health_status["retry_loop"] = "unknown"

    elif workers_status == "down":
        health_status["status"] = "degraded"
        health_status["worker_loop"] = "down"

    elif retry_scheduler_status == "down":
        health_status["status"] = "degraded"
        health_status["retry_loop"] = "down"

    else:
        health_status["status"] = "healthy"


    if health_status["status"] == "unhealthy":
        response.status_code = 503
    else :
        response.status_code = 200
    
    health_status["timestamp"] = (
    datetime.now(timezone.utc)
    .isoformat()
    .replace("+00:00", "Z"))

    return health_status

@router.get("/metrics/summary")
async def get_metrics():
    """Instant dashboard API for system health."""
    keys = [
        "metrics:events_created",
        "metrics:events_delivered_total", 
        "metrics:delivery_attempts",
        "metrics:events_failed_total", 
        "metrics:events_dead_total",
        "metrics:api_latency_total_ms",
        "metrics:api_request_count",
        "metrics:queue_delay_total_ms",
        "metrics:queue_delay_count",
        "metrics:queue_delay_max_ms",
    ]
        
    values = await redis_client.mget(keys)
    
    api_latency_total = float(values[5]) if values[5] else 0
    api_request_count = int(values[6]) if values[6] else 0

    queue_delay_total = int(values[7]) if values[7] else 0
    queue_delay_count = int(values[8]) if values[8] else 0
    queue_delay_max = int(values[9]) if values[9] else 0
    
    avg_api_latency = (
        api_latency_total / api_request_count
        if api_request_count
        else 0
    )

    avg_queue_delay = (
        queue_delay_total / queue_delay_count
        if queue_delay_count
        else 0
    )

    try:
        pending_info = await redis_client.xpending(
            "webhook_events",
            "delivery_workers")
        pending_messages = pending_info["pending"]

    except Exception:
        pending_messages = 0

    timestamp = datetime.now(timezone.utc).isoformat()
    return {
    "avg_api_latency_ms": round(avg_api_latency, 2),
    "avg_queue_delay_ms": round(avg_queue_delay, 2),
    "max_queue_delay_ms": queue_delay_max,
    "events_created": int(values[0]) if values[0] else 0,
    "events_delivered": int(values[1]) if values[1] else 0,
    "delivery_attempts": int(values[2]) if values[2] else 0,
    "events_failed": int(values[3]) if values[3] else 0,
    "events_dead": int(values[4]) if values[4] else 0,
    "pending messages": pending_messages,
    "timestamp": timestamp
}

@router.post("/metrics/reset")
async def reset_metrics_dashboard():
    """Admin route to wipe metrics counters before a new load test."""
    keys = [
        "metrics:events_created",
        "metrics:events_delivered_total", 
        "metrics:delivery_attempts",
        "metrics:events_failed_total", 
        "metrics:events_dead_total",
        "metrics:api_latency_total_ms",
        "metrics:api_request_count",
        "metrics:queue_delay_total_ms",
        "metrics:queue_delay_count",
        "metrics:queue_delay_max_ms",
    ]
    await redis_client.delete(*keys)
    return {"message": "All metrics successfully cleared"}
