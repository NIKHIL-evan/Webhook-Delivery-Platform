import asyncio
import json
import time
import uuid
from typing import NamedTuple, Optional

from cachetools import TTLCache
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.core.dependencies import _apply_rate_limit, authenticate_tenant
from app.core.local_metrics import add, incr
from app.core.redis_scripts import ingest_event
from app.core.telemetry import request_trace_id
from app.models import Endpoint, Tenant


ENDPOINT_TTL_S = 300
MISSING_TTL_S = 60


class CachedEndpoint(NamedTuple):
    id: str
    url: str


_endpoint_cache: TTLCache = TTLCache(maxsize=5_000, ttl=ENDPOINT_TTL_S)
_missing_cache: TTLCache = TTLCache(maxsize=10_000, ttl=MISSING_TTL_S)
_inflight: dict[str, asyncio.Task] = {}


async def _load_and_cache(key: str, endpoint_id: uuid.UUID, tenant: Tenant) -> Optional[CachedEndpoint]:
    # A miss costs a DB query, so it is rate-limited (closes the "404s aren't limited" gap).
    # Runs once per lookup; everyone waiting on this lookup shares the result (or the 429).
    await _apply_rate_limit(str(tenant.id), tenant.rate_limit)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Endpoint.id, Endpoint.url).where(
                Endpoint.id == endpoint_id,
                Endpoint.tenant_id == tenant.id,
            )
        )
        row = result.one_or_none()

    if row is None:
        _missing_cache[key] = True
        return None
    endpoint = CachedEndpoint(id=str(row.id), url=row.url)
    _endpoint_cache[key] = endpoint
    return endpoint


def _not_found() -> HTTPException:
    return HTTPException(status_code=404, detail="Endpoint not found")


async def get_endpoint_cached(endpoint_id: uuid.UUID, tenant: Tenant) -> CachedEndpoint:
    key = f"{tenant.id}:{endpoint_id}"

    # 1. Hot path: pure memory, no awaits
    endpoint = _endpoint_cache.get(key)
    if endpoint is not None:
        return endpoint
    if key in _missing_cache:
        raise _not_found()

    # 2. Single-flight: start the lookup, or join the one already running.
    task = _inflight.get(key)
    if task is None:
        task = asyncio.create_task(_load_and_cache(key, endpoint_id, tenant))
        _inflight[key] = task
        task.add_done_callback(lambda _t, k=key: _inflight.pop(k, None))  # never grows

    # shield: if THIS client disconnects, the shared lookup keeps going for the others
    endpoint = await asyncio.shield(task)
    if endpoint is None:
        raise _not_found()
    return endpoint


router = APIRouter()


class EventCreate(BaseModel):
    idempotency_key: Optional[str] = None
    endpoint_id: uuid.UUID
    payload: dict


@router.post("/events")
async def register_event(
    body: EventCreate,
    tenant: Tenant = Depends(authenticate_tenant),   # no rate limit here; the script does it
):
    start_time = time.perf_counter()
    current_trace_id = request_trace_id.get()

    # 1. Endpoint validation (cached; DB only on a miss)
    endpoint = await get_endpoint_cached(body.endpoint_id, tenant)

    # 2. Build the fat message
    new_event_id = str(uuid.uuid4())
    fields = {
        "event_id": new_event_id,
        "tenant_id": str(tenant.id),
        "endpoint_id": endpoint.id,
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
                "endpoint_id": endpoint.id,
                "status": "queued",
                "message": "Idempotent return",
            },
            status_code=202,
        )

    # 4. Metrics (in memory) and response
    api_latency = (time.perf_counter() - start_time) * 1000
    incr("metrics:events_created")
    add("metrics:api_latency_total_ms", api_latency)
    incr("metrics:api_request_count")

    response = JSONResponse(
        content={"event_id": event_id, "endpoint_id": endpoint.id, "status": "queued"},
        status_code=202,
    )
    response.headers["X-Route-Time"] = f"{api_latency:.2f}"
    return response