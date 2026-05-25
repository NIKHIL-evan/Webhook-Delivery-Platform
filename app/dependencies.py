import hashlib
from datetime import datetime, timezone
from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models import ApiKey, Tenant

api_key_header = APIKeyHeader(name="API-Key", auto_error=True)

async def get_current_tenant(
    api_key: str = Security(api_key_header),
    db: AsyncSession = Depends(get_db)
) -> Tenant:
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    current_time = datetime.now(timezone.utc)

    stmt = select(ApiKey).where(
        ApiKey.key_hash == key_hash,
        ApiKey.revoked_at.is_(None),
        (ApiKey.expires_at.is_(None)) | (ApiKey.expires_at > current_time)
    )
    result = await db.execute(stmt)
    api_key_record = result.scalar_one_or_none()

    if api_key_record is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    tenant_stmt = select(Tenant).where(
        Tenant.id == api_key_record.tenant_id,
        Tenant.is_active == True
    )
    tenant_result = await db.execute(tenant_stmt)
    tenant = tenant_result.scalar_one_or_none()

    if tenant is None:
        raise HTTPException(status_code=401, detail="Tenant not found or inactive")

    api_key_record.last_used_at = current_time

    return tenant