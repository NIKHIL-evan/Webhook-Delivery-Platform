from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.models import Endpoint, Tenant
from app.core.dependencies import get_current_tenant
from pydantic import BaseModel
import uuid
from typing import Optional
from sqlalchemy import select
from app.core.redis_client import redis_client
from app.core.telemetry import request_trace_id
import time, asyncio, json
from cachetools import TTLCache
from fastapi.responses import JSONResponse

_endpoint_cache: TTLCache = TTLCache(maxsize=5000, ttl=300)
_endpoint_locks: dict[str, asyncio.Lock] = {}
_endpoint_locks_lock = asyncio.Lock()

async def _get_endpoint_lock(cache_key: str) -> asyncio.Lock:
    async with _endpoint_locks_lock:
        if cache_key not in _endpoint_locks:
            _endpoint_locks[cache_key] = asyncio.Lock()
        return _endpoint_locks[cache_key]
    
async def get_endpoint_cached(
    endpoint_id: uuid.UUID,
    tenant_id: uuid.UUID,
    db: AsyncSession
) -> Endpoint:
    cache_key = f"{tenant_id}:{endpoint_id}"

    if cache_key in _endpoint_cache:
        return _endpoint_cache[cache_key]

    lock = await _get_endpoint_lock(cache_key)
    async with lock:
        if cache_key in _endpoint_cache:
            return _endpoint_cache[cache_key]

        stmt = select(Endpoint).where(
            Endpoint.id == endpoint_id,
            Endpoint.tenant_id == tenant_id
        )
        result = await db.execute(stmt)
        endpoint = result.scalar_one_or_none()

        if endpoint is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")

        _endpoint_cache[cache_key] = endpoint

    return endpoint

router = APIRouter()

class EventCreate(BaseModel):
    idempotency_key: Optional[str] = None
    endpoint_id: uuid.UUID
    payload: dict

@router.post("/events")
async def register_event(
    body: EventCreate,
    background_tasks: BackgroundTasks,
    tenant: Tenant = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    start_time = time.perf_counter()
    current_trace_id = request_trace_id.get()

    # 1. Endpoint Validation (Cached)
    endpoint = await get_endpoint_cached(body.endpoint_id, tenant.id, db)

    if endpoint is None:
        raise HTTPException(status_code=404, detail="Endpoint not found")

    # 2. Generate Event ID
    new_event_id = str(uuid.uuid4())
    event_id_to_return = new_event_id

    # Idempotency Check
    is_idempotent_retry = False

    if body.idempotency_key:
        redis_idem_key = f"idem:{tenant.id}:{body.idempotency_key}"
        is_new_request = await redis_client.set(redis_idem_key, new_event_id, nx=True, ex=86400)

        if not is_new_request:
            existing_id = await redis_client.get(redis_idem_key)
            event_id_to_return = existing_id.decode('utf-8') if isinstance(existing_id, bytes) else existing_id
            is_idempotent_retry = True
    
    if is_idempotent_retry:
        return JSONResponse(
            content={
                "event_id": event_id_to_return,
                "endpoint_id": str(endpoint.id),
                "status": "queued",
                "message": "Idempotent return"
            },
            status_code=202
        )
    
    # 3. Push Directly to Redis
    redis_payload = {
        "event_id": str(new_event_id),
        "tenant_id": str(tenant.id),
        "endpoint_id": str(endpoint.id),
        "destination_url": endpoint.url,
        "signing_secret": tenant.signing_secret,
        "payload": json.dumps(body.payload),
        "idempotency_key": body.idempotency_key or "",
        "trace_id": str(current_trace_id),
        "queued_at": str(time.time())
    }
    
    await redis_client.xadd("webhook_events", redis_payload)

    api_latency = (time.perf_counter() - start_time) * 1000

    async with redis_client.pipeline(transaction=False) as pipe:
        pipe.incr("metrics:events_created")
        pipe.incrbyfloat("metrics:api_latency_total_ms", api_latency)
        pipe.incr("metrics:api_request_count")
        await pipe.execute()

    # 5. Immediate Return
    response = JSONResponse(
        content={
            "event_id": str(new_event_id),
            "endpoint_id": str(endpoint.id),
            "status": "queued"
        },
        status_code=202
    )
    response.headers["X-Route-Time"] = f"{api_latency:.2f}"
    
    return response

