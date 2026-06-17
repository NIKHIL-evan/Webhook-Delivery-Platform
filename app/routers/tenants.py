from fastapi import APIRouter
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.models import Tenant
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from fastapi import Depends
import secrets

router = APIRouter()

class TenantCreate(BaseModel):
    name: str

@router.post("/tenants")
async def register_tenant(body: TenantCreate, db: AsyncSession = Depends(get_db)):
    try:
        secret = "whsec_" + secrets.token_urlsafe(32)
        tenant = Tenant(name=body.name, signing_secret=secret)
        db.add(tenant)
        await db.commit()
        await db.refresh(tenant)
        return {
            "id": str(tenant.id),
            "name": str(tenant.name),
            "signing_secret": secret
        }
    except SQLAlchemyError:
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Database error"
        )