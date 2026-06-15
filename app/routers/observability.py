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
    return {"status": "healthy"}


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

        worker_last_seen = await redis_client.get(
            "worker:worker-1:last_seen"
        )

        if (
            worker_last_seen is not None
            and time.time() - float(worker_last_seen) <= 30
        ):
            workers_status = "up"
            health_status["worker_loop"] = "up"

        retry_scheduler_last_seen = await redis_client.get(
            "retry_scheduler:last_seen"
        )

        if (
            retry_scheduler_last_seen is not None
            and time.time() - float(retry_scheduler_last_seen) <= 30
        ):
            retry_scheduler_status = "up"
            health_status["retry_loop"] = "up"

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

    response.status_code = (
        503 if health_status["status"] == "unhealthy"
        else 200
    )

    health_status["timestamp"] = (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

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

    "metrics:api_key_lookup_total_ms",
    "metrics:api_key_lookup_count",

    "metrics:endpoint_lookup_total_ms",
    "metrics:endpoint_lookup_count",

    "metrics:event_insert_total_ms",
    "metrics:event_insert_count",

    "metrics:redis_xadd_total_ms",
    "metrics:redis_xadd_count",

    "metrics:auth_stage_total_ms",
    "metrics:auth_stage_count",

    "metrics:endpoint_stage_total_ms",
    "metrics:endpoint_stage_count",

    "metrics:insert_stage_total_ms",
    "metrics:insert_stage_count",

    "metrics:xadd_stage_total_ms",
    "metrics:xadd_stage_count",

    "metrics:route_total_ms",
    "metrics:route_total_count",
]

    values = await redis_client.mget(keys)

    api_latency_total = float(values[5] or 0)
    api_request_count = int(values[6] or 0)

    queue_delay_total = float(values[7] or 0)
    queue_delay_count = int(values[8] or 0)
    queue_delay_max = int(values[9] or 0)

    api_key_lookup_total = float(values[10] or 0)
    api_key_lookup_count = int(values[11] or 0)

    endpoint_lookup_total = float(values[12] or 0)
    endpoint_lookup_count = int(values[13] or 0)

    event_insert_total = float(values[14] or 0)
    event_insert_count = int(values[15] or 0)

    redis_xadd_total = float(values[16] or 0)
    redis_xadd_count = int(values[17] or 0)
    auth_stage_total = float(values[18] or 0)
    auth_stage_count = int(values[19] or 0)

    endpoint_stage_total = float(values[20] or 0)
    endpoint_stage_count = int(values[21] or 0)

    insert_stage_total = float(values[22] or 0)
    insert_stage_count = int(values[23] or 0)

    xadd_stage_total = float(values[24] or 0)
    xadd_stage_count = int(values[25] or 0)

    route_total = float(values[26] or 0)
    route_total_count = int(values[27] or 0)

    avg_api_latency = (
        api_latency_total / api_request_count
        if api_request_count else 0
    )

    avg_queue_delay = (
        queue_delay_total / queue_delay_count
        if queue_delay_count else 0
    )

    avg_api_key_lookup = (
        api_key_lookup_total / api_key_lookup_count
        if api_key_lookup_count else 0
    )

    avg_endpoint_lookup = (
        endpoint_lookup_total / endpoint_lookup_count
        if endpoint_lookup_count else 0
    )

    avg_event_insert = (
        event_insert_total / event_insert_count
        if event_insert_count else 0
    )

    avg_redis_xadd = (
        redis_xadd_total / redis_xadd_count
        if redis_xadd_count else 0
    )
    avg_auth_stage = (
        auth_stage_total / auth_stage_count
        if auth_stage_count else 0
    )

    avg_endpoint_stage = (
        endpoint_stage_total / endpoint_stage_count
        if endpoint_stage_count else 0
    )

    avg_insert_stage = (
        insert_stage_total / insert_stage_count
        if insert_stage_count else 0
    )

    avg_xadd_stage = (
        xadd_stage_total / xadd_stage_count
        if xadd_stage_count else 0
    )

    avg_route_total = (
        route_total / route_total_count
        if route_total_count else 0
    )

    try:
        pending_info = await redis_client.xpending(
            "webhook_events",
            "delivery_workers"
        )
        pending_messages = pending_info["pending"]
    except Exception:
        pending_messages = 0

    result = {
        "avg_api_latency_ms": round(avg_api_latency, 2),

        "avg_api_key_lookup_ms": round(
            avg_api_key_lookup, 2
        ),

        "avg_endpoint_lookup_ms": round(
            avg_endpoint_lookup, 2
        ),

        "avg_event_insert_ms": round(
            avg_event_insert, 2
        ),

        "avg_redis_xadd_ms": round(
            avg_redis_xadd, 2
        ),

        "avg_auth_stage_ms": round(
            avg_auth_stage, 2
        ),

        "avg_endpoint_stage_ms": round(
            avg_endpoint_stage, 2
        ),

        "avg_insert_stage_ms": round(
            avg_insert_stage, 2
        ),

        "avg_xadd_stage_ms": round(
            avg_xadd_stage, 2
        ),

        "avg_route_total_ms": round(
            avg_route_total, 2
        ),

        "avg_queue_delay_ms": round(
            avg_queue_delay, 2
        ),

        "max_queue_delay_ms": queue_delay_max,

        "events_created": int(values[0] or 0),
        "events_delivered": int(values[1] or 0),
        "delivery_attempts": int(values[2] or 0),
        "events_failed": int(values[3] or 0),
        "events_dead": int(values[4] or 0),

        "pending_messages": pending_messages,

        "timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }
    if reset:
        await redis_client.delete(*keys)

    return result
