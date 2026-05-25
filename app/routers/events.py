from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.models import Event, Endpoint, Tenant
from app.dependencies import get_current_tenant
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
import uuid
from typing import Optional
from sqlalchemy import select
from app.redis_client import redis_client

router = APIRouter()

class EventCreate(BaseModel):
    idempotency_key: Optional[str] = None
    endpoint_id: uuid.UUID
    payload: dict

@router.post("/events")
async def register_event(
    body: EventCreate, 
    tenant: Tenant = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db), 
):
    try:
        smt = select(Endpoint).where(
            Endpoint.id == body.endpoint_id,
            Endpoint.tenant_id == tenant.id
        )
        result = await db.execute(smt)
        endpoint = result.scalar_one_or_none()

        if endpoint is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")

        if body.idempotency_key:
            existing = await db.execute(
                select(Event).where(
                    Event.idempotency_key == body.idempotency_key,
                    Event.tenant_id == tenant.id
                )
            )
            event = existing.scalar_one_or_none()

            if event:
                return {
                    "event_id": str(event.id),
                    "endpoint_id": str(event.endpoint_id),
                    "payload": event.payload,
                    "status": event.status,
                    "created_at": str(event.created_at)
                }

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Database connection failed. Please try again later."
        )

    try:
        event = Event(
            tenant_id=tenant.id, 
            endpoint_id=body.endpoint_id, 
            payload=body.payload, 
            idempotency_key=body.idempotency_key
        )
        db.add(event)
        await db.commit()
        await db.refresh(event)
    except SQLAlchemyError:
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Database error while saving event"
        )
    
    try:
        await redis_client.xadd(
            "webhook_events", 
            {"event_id": str(event.id), "endpoint_id": str(body.endpoint_id)}
        )
    except Exception:
        pass

    return {
        "event_id": str(event.id),
        "endpoint_id": str(event.endpoint_id),
        "payload": event.payload,
        "status": event.status,
        "created_at": str(event.created_at)
    }