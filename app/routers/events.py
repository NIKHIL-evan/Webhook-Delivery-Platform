from fastapi import APIRouter
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.models import Events, Endpoints
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from fastapi import Depends
import uuid
from typing import Optional
from sqlalchemy import select
from app.redis_client import redis_client


router = APIRouter()

class EventCreate(BaseModel):
    idempotency_key: Optional[str]
    url_id: uuid.UUID
    payload: dict

@router.post("/events")
async def register_event(body: EventCreate, db: AsyncSession = Depends(get_db)):
    try:
        url_id=body.url_id
        smt = select(Endpoints).where(Endpoints.url_id == url_id)
        result = await db.execute(smt)
        endpoint = result.scalar_one_or_none()

        if endpoint is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")
        else:
            if body.idempotency_key :
                existing = await db.execute(
                    select(Events).where(Events.idempotency_key == body.idempotency_key)
                )
                event = existing.scalar_one_or_none()

                if event:
                    return {
                        "event_id": str(event.event_id),
                        "url_id": str(event.url_id),
                        "payload": event.payload,
                        "status": event.status,
                        "created_at": str(event.created_at)
                    }

            payload=body.payload
            event = Events(url_id=url_id, payload=payload, idempotency_key=body.idempotency_key)
            db.add(event)
            await db.commit()
            await db.refresh(event)
            event_id = event.event_id
            event_url_id = event.url_id
            event_payload = event.payload
            event_status = event.status
            event_created_at = event.created_at
            
            await redis_client.xadd("webhook_events", {"event_id": str(event_id), "endpoint_id": str(url_id)})

            return {
            "event_id": str(event_id),
            "url_id": str(event_url_id),
            "payload": event_payload,
            "status": event_status,
            "created_at": str(event_created_at)}
    
    except SQLAlchemyError:
        await db.rollback()

        raise HTTPException(
            status_code=500,
            detail="Database error"
        )