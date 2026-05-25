from fastapi import APIRouter, HTTPException, Depends
from app.models import Endpoint, Tenant
from app.dependencies import get_current_tenant
from pydantic import BaseModel
from app.database import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter()

class EndpointCreate(BaseModel):
    url: str

@router.post("/endpoints")
async def register_url(
    body: EndpointCreate,
    tenant: Tenant = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db)
):
    try:
        endpoint = Endpoint(url=body.url, tenant_id=tenant.id)
        db.add(endpoint)
        await db.commit()
        await db.refresh(endpoint)
        return {
            "endpoint_id": str(endpoint.id),
            "url": endpoint.url,
            "created_at": str(endpoint.created_at)
        }
    except SQLAlchemyError as e:
        print(f"DB ERROR: {e}", flush=True)
        await db.rollback()
        raise HTTPException(status_code=500, detail="Database error")