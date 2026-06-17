import hashlib, time, asyncio, json
from datetime import datetime, timezone
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select, update
from app.core.database import AsyncSessionLocal
from app.models import ApiKey, Tenant
from app.core.redis_client import redis_client

api_key_header = APIKeyHeader(name="API-Key", auto_error=True)

# ---------------------------------------------------------
# ATOMIC RATE LIMITING
# Evaluates INCR and EXPIRE in a single Redis network trip
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
# BUFFERED DATABASE WRITES
# Prevents connection pool starvation under heavy load
# ---------------------------------------------------------
async def _update_last_used_buffered(key_hash: str) -> None:
    """Updates last_used_at a maximum of once per 15 minutes per key."""
    cache_key = f"last_used_synced:{key_hash}"
    
    # If the key was set successfully, it means 15 minutes have passed
    is_time_to_update = await redis_client.set(cache_key, "1", nx=True, ex=900)
    
    if is_time_to_update:
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(
                    update(ApiKey)
                    .where(ApiKey.key_hash == key_hash)
                    .values(last_used_at=datetime.now(timezone.utc))
                )
                await session.commit()
        except Exception:
            # If the DB fails, delete the lock so it tries again on the next request
            await redis_client.delete(cache_key)

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
        asyncio.create_task(_update_last_used_buffered(key_hash))
        return tenant

    # Slow path
    async with AsyncSessionLocal() as session:
        current_time = datetime.now(timezone.utc)
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
    asyncio.create_task(_update_last_used_buffered(key_hash))
    
    return tenant