import time
from app.redis_client import redis_client

async def metrics_middleware(request, call_next):
    start = time.perf_counter()

    response = await call_next(request)

    duration_ms = (time.perf_counter() - start) * 1000

    await redis_client.incrbyfloat(
        "metrics:api_latency_total_ms",
        duration_ms
    )

    await redis_client.incr(
        "metrics:api_request_count"
    )

    return response