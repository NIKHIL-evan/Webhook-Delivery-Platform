from fastapi import APIRouter, Depends, Response
from app.core.redis_client import redis_client
from app.core.database import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import time
from datetime import datetime, timezone

router = APIRouter(tags=["observability"])

@router.get("/health/details")
async def detailed_health(
    response: Response,
    db: AsyncSession = Depends(get_db)
):
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

    try:
        await db.execute(text("SELECT 1"))
        postgres_status = "up"
        health_status["postgres"] = postgres_status
    except Exception:
        health_status["postgres"] = "down"

    try:
        await redis_client.ping()
        redis_status = "up"
        health_status["redis"] = redis_status
    except Exception:
        health_status["redis"] = "down"

    if redis_status == "up":
        # Check any delivery worker
        worker_last_seen = await redis_client.get("worker:delivery-1:last_seen")
        if worker_last_seen is not None and time.time() - float(worker_last_seen) <= 30:
            workers_status = "up"
            health_status["worker_loop"] = "up"

        retry_scheduler_last_seen = await redis_client.get("retry_scheduler:last_seen")
        if retry_scheduler_last_seen is not None and time.time() - float(retry_scheduler_last_seen) <= 30:
            retry_scheduler_status = "up"
            health_status["retry_loop"] = "up"

    if postgres_status == "down" or redis_status == "down":
        health_status["status"] = "unhealthy"
    elif workers_status == "down" or retry_scheduler_status == "down":
        health_status["status"] = "degraded"
    else:
        health_status["status"] = "healthy"

    response.status_code = 503 if health_status["status"] == "unhealthy" else 200
    health_status["timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return health_status

@router.get("/metrics/summary")
async def get_metrics(reset: bool = True):
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
        
        "metrics:ingestion_lag_total_ms",
        "metrics:ingestion_lag_count",
        
        "metrics:batch_insert_total_ms",
        "metrics:batch_insert_count",
    ]

    LUA_FETCH_AND_RESET = """
    local vals = redis.call('MGET', unpack(KEYS))
    if ARGV[1] == '1' then
        redis.call('DEL', unpack(KEYS))
    end
    return vals
    """

    reset_flag = '1' if reset else '0'
    values = await redis_client.eval(LUA_FETCH_AND_RESET, len(keys), *keys, reset_flag)

    # Core Counts
    events_created = int(values[0] or 0)
    events_delivered = int(values[1] or 0)
    delivery_attempts = int(values[2] or 0)
    events_failed = int(values[3] or 0)
    events_dead = int(values[4] or 0)

    # API Latency
    api_latency_total = float(values[5] or 0)
    api_request_count = int(values[6] or 0)
    avg_api_latency = round(api_latency_total / api_request_count, 2) if api_request_count else 0

    # Delivery Queue Delay
    queue_delay_total = float(values[7] or 0)
    queue_delay_count = int(values[8] or 0)
    avg_queue_delay = round(queue_delay_total / queue_delay_count, 2) if queue_delay_count else 0
    max_queue_delay = float(values[9] or 0)

    # Ingestion Lag
    ingestion_lag_total = float(values[10] or 0)
    ingestion_lag_count = int(values[11] or 0)
    avg_ingestion_lag = round(ingestion_lag_total / ingestion_lag_count, 2) if ingestion_lag_count else 0

    # Batch Insert Time
    batch_insert_total = float(values[12] or 0)
    batch_insert_count = int(values[13] or 0)
    avg_batch_insert = round(batch_insert_total / batch_insert_count, 2) if batch_insert_count else 0

    # Pending Messages across all groups
    pending_delivery = 0
    pending_ingestion = 0
    pending_results = 0
    try:
        pending_delivery = (await redis_client.xpending("webhook_events", "delivery_workers"))["pending"]
        pending_ingestion = (await redis_client.xpending("webhook_events", "ingestion_workers"))["pending"]
        pending_results = (await redis_client.xpending("webhook_results", "results_ingestion_workers"))["pending"]
    except Exception:
        pass

    return {
        "pipeline_health": {
            "pending_delivery_tasks": pending_delivery,
            "pending_db_ingestions": pending_ingestion,
            "pending_result_ingestions": pending_results,
        },
        "latencies_ms": {
            "avg_api_edge": avg_api_latency,
            "avg_delivery_queue_delay": avg_queue_delay,
            "max_delivery_queue_delay": round(max_queue_delay, 2),
            "avg_db_ingestion_lag": avg_ingestion_lag,
            "avg_db_batch_insert": avg_batch_insert,
        },
        "throughput": {
            "events_created": events_created,
            "delivery_attempts": delivery_attempts,
            "events_delivered": events_delivered,
            "events_failed": events_failed,
            "events_dead": events_dead,
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }