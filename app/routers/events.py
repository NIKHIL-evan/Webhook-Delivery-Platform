from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.models import Event, Endpoint, Tenant
from app.dependencies import get_current_tenant
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
import uuid
from typing import Optional
from sqlalchemy import select
from app.redis_client import redis_client
from app.telemetry import request_trace_id
import time, asyncio
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
    tenant: Tenant = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    route_start = time.perf_counter()
    current_trace_id = request_trace_id.get()

    try:

        start_endpoint = time.perf_counter()

        endpoint = await get_endpoint_cached(
        body.endpoint_id, tenant.id, db
        )

        endpoint_lookup_ms = (
            time.perf_counter() - start_endpoint
        ) * 1000

        after_endpoint = time.perf_counter()

        if endpoint is None:
            raise HTTPException(
                status_code=404,
                detail="Endpoint not found"
            )

        if body.idempotency_key:
            existing = await db.execute(
                select(Event).where(
                    Event.idempotency_key == body.idempotency_key,
                    Event.tenant_id == tenant.id
                )
            )

            event = existing.scalar_one_or_none()

            if event:
                return {
                    "event_id": str(event.id),
                    "endpoint_id": str(event.endpoint_id),
                    "payload": event.payload,
                    "status": event.status,
                    "created_at": str(event.created_at)
                }

    except HTTPException:
        raise

    except Exception as e:
        print(type(e).__name__)
        print(str(e))
        raise

    try:
        event = Event(
            tenant_id=tenant.id,
            endpoint_id=body.endpoint_id,
            payload=body.payload,
            idempotency_key=body.idempotency_key,
            trace_id=current_trace_id
        )
        
        start_insert = time.perf_counter()

        db.add(event)
        await db.commit()

        insert_event_ms = (
            time.perf_counter() - start_insert
        ) * 1000

        after_insert = time.perf_counter()

    except SQLAlchemyError as e:
        await db.rollback()

        print("\n" + "=" * 80)
        print("EVENT CREATION ERROR")
        print(type(e).__name__)
        print(str(e))
        print("=" * 80 + "\n")

        raise HTTPException(
            status_code=500,
            detail=f"Database error: {type(e).__name__}"
        )

    try:
        start_xadd = time.perf_counter()

        await redis_client.xadd(
            "webhook_events",
            {
                "event_id": str(event.id),
                "endpoint_id": str(endpoint.id),
                "trace_id": str(current_trace_id),
                "queued_at": str(time.time())
            }
        )

        redis_xadd_ms = (
            time.perf_counter() - start_xadd
        ) * 1000

        after_xadd = time.perf_counter()

    except Exception as e:
        print(type(e).__name__)
        print(str(e))
        raise

    route_end = time.perf_counter()

    endpoint_stage_ms = (
        after_endpoint - route_start
    ) * 1000

    insert_stage_ms = (
        after_insert - after_endpoint
    ) * 1000

    xadd_stage_ms = (
        after_xadd - after_insert
    ) * 1000

    route_total_ms = (
        route_end - route_start
    ) * 1000


    async with redis_client.pipeline(transaction=False) as pipe:
        pipe.incrbyfloat("metrics:endpoint_lookup_total_ms", endpoint_lookup_ms)
        pipe.incr("metrics:endpoint_lookup_count")
        pipe.incrbyfloat("metrics:event_insert_total_ms", insert_event_ms)
        pipe.incr("metrics:event_insert_count")
        pipe.incrbyfloat("metrics:redis_xadd_total_ms",redis_xadd_ms)
        pipe.incr("metrics:redis_xadd_count")
        pipe.incr("metrics:events_created")
        pipe.incrbyfloat("metrics:endpoint_stage_total_ms", endpoint_stage_ms)
        pipe.incr("metrics:endpoint_stage_count")
        pipe.incrbyfloat("metrics:insert_stage_total_ms", insert_stage_ms)
        pipe.incr("metrics:insert_stage_count")
        pipe.incrbyfloat("metrics:xadd_stage_total_ms", xadd_stage_ms)
        pipe.incr("metrics:xadd_stage_count")
        pipe.incrbyfloat("metrics:route_total_ms", route_total_ms)
        pipe.incr("metrics:route_total_count")
        await pipe.execute()

    response = JSONResponse(
        content={
            "event_id": str(event.id),
            "endpoint_id": str(event.endpoint_id),
            "payload": event.payload,
            "status": event.status,
            "created_at": str(event.created_at)
        }
)

    response.headers["X-Route-Time"] = f"{route_total_ms:.2f}"

    return response