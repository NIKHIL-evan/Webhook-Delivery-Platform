import uuid
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Depends
from app.database import get_db
from app.models import DeliveryAttempts, Events
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

router = APIRouter()

@router.get("/events/{event_id}/delivery_attempts")
async def fetch_attempts(event_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    stm = select(Events).where(Events.event_id == event_id)
    result = await db.execute(stm)
    event = result.scalar_one_or_none()
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    else:
        stmt = select(DeliveryAttempts).where(DeliveryAttempts.event_id == event_id)
        result = await db.execute(stmt)
        attempts = result.scalars().all()
        return attempts