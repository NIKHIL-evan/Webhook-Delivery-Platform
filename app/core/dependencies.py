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
# RATE LIMITING for routes other than POST /events
# (POST /events rate-limits inside the ingest script, using
# the SAME key and logic, so each tenant has one shared budget)
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
# LAST-USED GATE (per process, in memory) — unchanged from Task 2
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
# AUTH DEPENDENCIES
# ---------------------------------------------------------
async def authenticate_tenant(
    api_key: str = Security(api_key_header),
) -> Tenant:
    """API key → tenant. No rate limit (used by POST /events,
    which rate-limits inside the ingest script)."""
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    cache_key = f"tenant_cache:{key_hash}"

    # Fast path
    cached = await redis_client.get(cache_key)
    if cached:
        data = json.loads(cached)
        tenant = Tenant()
        tenant.id = data["id"]
        tenant.rate_limit = data["rate_limit"]
        tenant.signing_secret = data["signing_secret"]
        tenant.is_active = data["is_active"]

        _maybe_update_last_used(key_hash)
        return tenant

    # Slow path
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
            raise HTTPException(
                status_code=401,
                detail="Invalid API key or inactive tenant"
            )

        api_key_record, tenant = row

        await redis_client.setex(
            cache_key,
            300,
            json.dumps({
                "id": str(tenant.id),
                "rate_limit": tenant.rate_limit,
                "signing_secret": tenant.signing_secret,
                "is_active": tenant.is_active
            })
        )

    _maybe_update_last_used(key_hash)
    return tenant


async def get_current_tenant(
    tenant: Tenant = Depends(authenticate_tenant),
) -> Tenant:
    """API key → tenant, plus the rate limit. Used by every route except POST /events."""
    await _apply_rate_limit(str(tenant.id), tenant.rate_limit)
    return tenant