import asyncio
import hashlib
import json
from datetime import datetime, timezone

import structlog
from cachetools import TTLCache
from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select, update

from app.core.database import AsyncSessionLocal
from app.core.redis_client import redis_client
from app.core.redis_scripts import RATE_LIMIT_WINDOW_S
from app.models import ApiKey, Tenant

api_key_header = APIKeyHeader(name="API-Key", auto_error=True)
logger = structlog.get_logger()

# ---------------------------------------------------------
# RATE LIMITING for routes other than POST /events (unchanged)
# ---------------------------------------------------------
RATE_LIMIT_LUA = """
local current = redis.call('INCR', KEYS[1])
if tonumber(current) == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
"""

async def _apply_rate_limit(tenant_id: str, rate_limit: int) -> None:
    redis_key = f"rate_limit:{tenant_id}"
    current_count = await redis_client.eval(RATE_LIMIT_LUA, 1, redis_key, RATE_LIMIT_WINDOW_S)

    if current_count > rate_limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

# ---------------------------------------------------------
# LAST-USED GATE (unchanged from Task 2)
# ---------------------------------------------------------
_last_used_synced: TTLCache = TTLCache(maxsize=10_000, ttl=900)
_background_tasks: set[asyncio.Task] = set()

def _maybe_update_last_used(key_hash: str) -> None:
    if key_hash in _last_used_synced:
        return
    _last_used_synced[key_hash] = True

    task = asyncio.create_task(_write_last_used(key_hash))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

async def _write_last_used(key_hash: str) -> None:
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(ApiKey)
                .where(ApiKey.key_hash == key_hash)
                .values(last_used_at=datetime.now(timezone.utc))
            )
            await session.commit()
    except Exception as e:
        _last_used_synced.pop(key_hash, None)
        logger.error("last_used_write_failed", error=str(e))

# ---------------------------------------------------------
# TENANT CACHE: two levels
#   L1 = this process's memory, 30 s  → most requests, no Redis trip
#   L2 = Redis, 300 s                 → shared by all processes; DEL on revoke
# A revoked key stops working within L1_TTL (≤ 30 s) in every process.
# ---------------------------------------------------------
TENANT_L1_TTL_S = 30
TENANT_L2_TTL_S = 300

_tenant_l1: TTLCache = TTLCache(maxsize=10_000, ttl=TENANT_L1_TTL_S)

def _l2_key(key_hash: str) -> str:
    return f"tenant_cache:{key_hash}"

def _tenant_from_data(data: dict) -> Tenant:
    tenant = Tenant()
    tenant.id = data["id"]
    tenant.rate_limit = data["rate_limit"]
    tenant.signing_secret = data["signing_secret"]
    tenant.is_active = data["is_active"]
    return tenant

async def _load_tenant_data_from_db(key_hash: str) -> dict:
    """Postgres: the source of truth. Raises 401 for unknown, revoked,
    expired keys or inactive tenants."""
    async with AsyncSessionLocal() as session:
        current_time = datetime.now(timezone.utc)
        stmt = select(ApiKey, Tenant).join(
            Tenant, ApiKey.tenant_id == Tenant.id
        ).where(
            Tenant.is_active == True,  # noqa: E712 - SQLAlchemy needs ==, not "is"
            ApiKey.key_hash == key_hash,
            ApiKey.revoked_at.is_(None),
            (ApiKey.expires_at.is_(None)) | (ApiKey.expires_at > current_time)
        )
        result = await session.execute(stmt)
        row = result.unique().one_or_none()

    if row is None:
        raise HTTPException(status_code=401, detail="Invalid API key or inactive tenant")

    _, tenant = row
    return {
        "id": str(tenant.id),
        "rate_limit": tenant.rate_limit,
        "signing_secret": tenant.signing_secret,
        "is_active": tenant.is_active,
    }

async def _get_tenant_data(key_hash: str) -> dict:
    # L1: this process's memory (no trip)
    data = _tenant_l1.get(key_hash)
    if data is not None:
        return data

    # L2: Redis (shared)
    cached = await redis_client.get(_l2_key(key_hash))
    if cached:
        data = json.loads(cached)
    else:
        # Postgres (truth), then fill L2
        data = await _load_tenant_data_from_db(key_hash)
        await redis_client.setex(_l2_key(key_hash), TENANT_L2_TTL_S, json.dumps(data))

    _tenant_l1[key_hash] = data
    return data

async def invalidate_api_key(key_hash: str) -> None:
    """Call AFTER the revocation is committed in Postgres."""
    _tenant_l1.pop(key_hash, None)                       # this process: immediately
    try:
        await redis_client.delete(_l2_key(key_hash))     # shared L2: no process can refill from it
    except Exception as e:
        # The revocation is already committed; L2 still expires within TENANT_L2_TTL_S.
        logger.error("tenant_cache_invalidation_failed", error=str(e))

# ---------------------------------------------------------
# AUTH DEPENDENCIES
# ---------------------------------------------------------
async def authenticate_tenant(
    api_key: str = Security(api_key_header),
) -> Tenant:
    """API key → tenant. No rate limit (POST /events rate-limits in its script)."""
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    data = await _get_tenant_data(key_hash)
    _maybe_update_last_used(key_hash)
    return _tenant_from_data(data)


async def get_current_tenant(
    tenant: Tenant = Depends(authenticate_tenant),
) -> Tenant:
    """API key → tenant, plus the rate limit. Used by every route except POST /events."""
    await _apply_rate_limit(str(tenant.id), tenant.rate_limit)
    return tenant