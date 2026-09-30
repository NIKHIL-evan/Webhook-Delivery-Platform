import asyncio
import time
import uuid
from typing import Optional

from cachetools import TTLCache
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.dependencies import authenticate_tenant
from app.core.local_metrics import add, incr
from app.core.redis_scripts import ingest_event
from app.core.telemetry import request_trace_id
from app.models import Endpoint, Tenant
import json

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
    tenant: Tenant = Depends(authenticate_tenant),   # ← no rate limit here; the script does it
    db: AsyncSession = Depends(get_db),
):
    start_time = time.perf_counter()
    current_trace_id = request_trace_id.get()

    # 1. Endpoint validation (cached)
    endpoint = await get_endpoint_cached(body.endpoint_id, tenant.id, db)

    # 2. Build the fat message
    new_event_id = str(uuid.uuid4())
    fields = {
        "event_id": new_event_id,
        "tenant_id": str(tenant.id),
        "endpoint_id": str(endpoint.id),
        "destination_url": endpoint.url,
        "signing_secret": tenant.signing_secret,
        "payload": json.dumps(body.payload),
        "idempotency_key": body.idempotency_key or "",
        "trace_id": str(current_trace_id),
        "queued_at": str(time.time()),
    }

    # 3. Rate limit + idempotency + enqueue: ONE Redis trip, one indivisible step.
    #    Redis errors are NOT caught: they become a 500 and the client retries.
    #    A 202 is only ever sent after the script said "queued" or "duplicate".
    status, event_id = await ingest_event(
        tenant_id=str(tenant.id),
        rate_limit=tenant.rate_limit,
        idempotency_key=body.idempotency_key,
        event_id=new_event_id,
        fields=fields,
    )

    if status == "limited":
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    if status == "duplicate":
        return JSONResponse(
            content={
                "event_id": event_id,
                "endpoint_id": str(endpoint.id),
                "status": "queued",
                "message": "Idempotent return"
            },
            status_code=202
        )

    # 4. Metrics (in memory) and response
    api_latency = (time.perf_counter() - start_time) * 1000
    incr("metrics:events_created")
    add("metrics:api_latency_total_ms", api_latency)
    incr("metrics:api_request_count")

    response = JSONResponse(
        content={
            "event_id": event_id,
            "endpoint_id": str(endpoint.id),
            "status": "queued"
        },
        status_code=202
    )
    response.headers["X-Route-Time"] = f"{api_latency:.2f}"
    return response