import hashlib
from datetime import datetime, timezone
from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import ApiKey, Tenant
from app.redis_client import redis_client

api_key_header = APIKeyHeader(name="API-Key", auto_error=True)

async def get_current_tenant(
    api_key: str = Security(api_key_header),
    db: AsyncSession = Depends(get_db),

) -> Tenant:
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    current_time = datetime.now(timezone.utc)

    stmt = select(ApiKey,Tenant).join(Tenant, ApiKey.tenant_id == Tenant.id).where(
        Tenant.is_active == True,
        ApiKey.key_hash == key_hash,
        ApiKey.revoked_at.is_(None),
        (ApiKey.expires_at.is_(None)) | (ApiKey.expires_at > current_time)
    )
    result = await db.execute(stmt)
    row = result.unique().one_or_none()

    if row is None:
        raise HTTPException(status_code=401, detail="Invalid API key or inactive tenant")
    
    api_key_record: ApiKey
    tenant: Tenant
    api_key_record, tenant = row

    api_key_record.last_used_at = current_time

    redis_key = f"rate_limit:{api_key_record.tenant_id}"

    current_count = await redis_client.incr(redis_key)
    await redis_client.expire(redis_key, 60)  # set every time, harmless

    if current_count > tenant.rate_limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    return tenant