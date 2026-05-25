from fastapi import APIRouter
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from sqlalchemy import select
from pydantic import BaseModel
from app.database import get_db
from app.models import Tenant, ApiKey
from datetime import datetime, timezone
import uuid
import secrets
import hashlib

router = APIRouter()

class ApiKeyCreate(BaseModel):
    name: str


@router.post("/tenants/{tenant_id}/api-keys")
async def generate_api_key(tenant_id: uuid.UUID, body: ApiKeyCreate, db: AsyncSession = Depends(get_db)):

    stmt = select(Tenant).where(Tenant.id == tenant_id)
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()

    if tenant is None:
        raise HTTPException(
            status_code=404,
            detail="Tenant not found"
        )

    # Generate raw API key
    raw_key = f"wh_live_{secrets.token_urlsafe(32)}"

    # Hash key
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    key_prefix = raw_key[:12]

    # Store only hash
    api_key = ApiKey(
        tenant_id=tenant.id,
        key_hash=key_hash,
        key_prefix=key_prefix,
        name=body.name
    )

    db.add(api_key)
    await db.commit()
    await db.refresh(api_key)

    return {
        "api_key_id": str(api_key.id),
        "name": api_key.name,
        "api_key": raw_key,
        "created_at": str(api_key.created_at)
    }



@router.delete("/api-keys/{api_key_id}")
async def revoke_api_key(api_key_id: uuid.UUID,db: AsyncSession = Depends(get_db)):

    stmt = select(ApiKey).where(ApiKey.id == api_key_id)
    result = await db.execute(stmt)
    api_key = result.scalar_one_or_none()

    if api_key is None:
        raise HTTPException(
            status_code=404,
            detail="API key not found"
        )

    # Already revoked
    if api_key.revoked_at is not None:
        raise HTTPException(
            status_code=400,
            detail="API key already revoked"
        )

    # Soft revoke
    api_key.revoked_at = datetime.now(timezone.utc)
    db.add(api_key)
    await db.commit()
    await db.refresh(api_key)

    return {
        "message": "API key revoked",
        "api_key_id": str(api_key.id),
        "revoked_at": str(api_key.revoked_at)
    }