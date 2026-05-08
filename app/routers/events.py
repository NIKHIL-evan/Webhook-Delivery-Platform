from fastapi import APIRouter
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.models import Events, Endpoints, DeliveryAttempts
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from fastapi import Depends
import uuid
import httpx
from sqlalchemy import select


router = APIRouter()

class EventCreate(BaseModel):
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
            destination_url = endpoint.url
            payload=body.payload
            event = Events(url_id=url_id, payload=payload)
            db.add(event)
            await db.commit()
            await db.refresh(event)
            event_id = event.event_id
            event_url_id = event.url_id
            event_payload = event.payload
            event_status = event.status
            event_created_at = event.created_at
            
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        destination_url,
                        json=payload,
                        timeout=10.0  #to catch httpx.TimeoutException if destination url hangs
                    )
                    if response.status_code == 200:
                        attempt = DeliveryAttempts(event_id=event_id, 
                                                    attempt_number=1,
                                                    status="success",
                                                    response_code= response.status_code)
                        db.add(attempt)
                        await db.commit()

                        event.status = "delivered"
                        db.add(event)
                        await db.commit()

                    else:
                        f_attempt = DeliveryAttempts(event_id=event_id,
                                                        attempt_number=1,
                                                        status="failed",
                                                        response_code=response.status_code)
                        db.add(f_attempt)
                        await db.commit()
            except httpx.RequestError as e:
                print("[ERROR] ",e)
                f2_attempt = DeliveryAttempts(event_id=event_id,
                                            attempt_number=1,
                                            status="failed",
                                            response_code=None)
                db.add(f2_attempt)
                await db.commit()

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