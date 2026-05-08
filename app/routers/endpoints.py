from fastapi import APIRouter
from app.models import Endpoints
from pydantic import BaseModel
from app.database import get_db
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

class EndpointCreate(BaseModel):
    url: str 

@router.post("/endpoints")
async def register_url(body: EndpointCreate, db: AsyncSession = Depends(get_db)):
    endpoint = Endpoints(url=body.url)
    db.add(endpoint)
    await db.commit()
    await db.refresh(endpoint)
    return {
    "url_id": str(endpoint.url_id),
    "url": endpoint.url,
    "created_at": str(endpoint.created_at)
    }