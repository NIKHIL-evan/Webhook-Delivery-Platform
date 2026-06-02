import uuid
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Depends
from app.database import get_db
from app.models import DeliveryAttempt, Event, Tenant
from app.dependencies import get_current_tenant
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError 

router = APIRouter()

@router.get("/events/{event_id}/delivery_attempts")
async def fetch_attempts(event_id: uuid.UUID, db: AsyncSession = Depends(get_db), tenant: Tenant = Depends(get_current_tenant)):
    try:
        stm = select(Event).where(Event.id == event_id, Event.tenant_id == tenant.id)
        result = await db.execute(stm)
        event = result.scalar_one_or_none()
        if event is None:
            raise HTTPException(status_code=404, detail="Event not found")
        else:
            stmt = select(DeliveryAttempt).where(DeliveryAttempt.event_id == event_id)
            result = await db.execute(stmt)
            attempts = result.scalars().all()
            return attempts
        
    except SQLAlchemyError:
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Database error"
        )