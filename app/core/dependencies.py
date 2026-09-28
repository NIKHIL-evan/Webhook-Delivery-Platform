import asyncio
import hashlib
import json
from datetime import datetime, timezone

import structlog
from cachetools import TTLCache
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select, update

from app.core.database import AsyncSessionLocal
from app.core.redis_client import redis_client
from app.models import ApiKey, Tenant

api_key_header = APIKeyHeader(name="API-Key", auto_error=True)
logger = structlog.get_logger()

# ---------------------------------------------------------
# ATOMIC RATE LIMITING (unchanged)
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
    current_count = await redis_client.eval(RATE_LIMIT_LUA, 1, redis_key, 60)

    if current_count > rate_limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

# ---------------------------------------------------------
# LAST-USED GATE (per process, in memory)
# Postgres gets at most one last_used_at write per key per 15 min
# per process. Duplicates across processes are harmless: the write
# is idempotent ("last used ≈ now").
# ---------------------------------------------------------
_last_used_synced: TTLCache = TTLCache(maxsize=10_000, ttl=900)
_background_tasks: set[asyncio.Task] = set()

def _maybe_update_last_used(key_hash: str) -> None:
    # Check and mark with no await in between → only one request wins.
    if key_hash in _last_used_synced:
        return
    _last_used_synced[key_hash] = True

    task = asyncio.create_task(_write_last_used(key_hash))
    _background_tasks.add(task)                        # hold a strong reference
    task.add_done_callback(_background_tasks.discard)  # release it when finished

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
        # Un-mark so the next request retries instead of waiting 15 min.
        _last_used_synced.pop(key_hash, None)
        logger.error("last_used_write_failed", error=str(e))

# ---------------------------------------------------------
# AUTH DEPENDENCY
# ---------------------------------------------------------
async def get_current_tenant(
    api_key: str = Security(api_key_header),
) -> Tenant:
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

        await _apply_rate_limit(str(tenant.id), tenant.rate_limit)
        _maybe_update_last_used(key_hash)          # ← was: asyncio.create_task(...)
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

    await _apply_rate_limit(str(tenant.id), tenant.rate_limit)
    _maybe_update_last_used(key_hash)              # ← was: asyncio.create_task(...)
    return tenant