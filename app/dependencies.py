import hashlib, time, asyncio
from datetime import datetime, timezone
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import ApiKey, Tenant
from app.redis_client import redis_client
from cachetools import TTLCache

_tenant_cache: TTLCache = TTLCache(maxsize=1000, ttl=300)
_key_locks: dict[str, asyncio.Lock] = {}
_locks_lock = asyncio.Lock()

api_key_header = APIKeyHeader(name="API-Key", auto_error=True)


async def _get_key_lock(key_hash: str) -> asyncio.Lock:
    async with _locks_lock:
        if key_hash not in _key_locks:
            _key_locks[key_hash] = asyncio.Lock()
        return _key_locks[key_hash]


async def _apply_rate_limit(tenant: Tenant) -> None:
    redis_key = f"rate_limit:{tenant.id}"
    current_count = await redis_client.incr(redis_key)
    if current_count == 1:
        await redis_client.expire(redis_key, 60)
    if current_count > tenant.rate_limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")


async def get_current_tenant(
    api_key: str = Security(api_key_header),
) -> Tenant:
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    current_time = datetime.now(timezone.utc)

    # Fast path — cache hit, zero DB, zero transaction
    if key_hash in _tenant_cache:
        tenant = _tenant_cache[key_hash]
        await _apply_rate_limit(tenant)
        return tenant

    # Slow path — own session, completely isolated from request session
    lock = await _get_key_lock(key_hash)
    async with lock:
        if key_hash in _tenant_cache:
            tenant = _tenant_cache[key_hash]
            await _apply_rate_limit(tenant)
            return tenant

        start = time.perf_counter()

        async with AsyncSessionLocal() as session:
            stmt = select(ApiKey, Tenant).join(
                Tenant, ApiKey.tenant_id == Tenant.id
            ).where(
                Tenant.is_active == True,
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
            api_key_record.last_used_at = current_time
            await session.commit()
        

        api_lookup_ms = (time.perf_counter() - start) * 1000
        await redis_client.incrbyfloat("metrics:api_key_lookup_total_ms", api_lookup_ms)
        await redis_client.incr("metrics:api_key_lookup_count")

        _tenant_cache[key_hash] = tenant

    await _apply_rate_limit(tenant)
    return tenant