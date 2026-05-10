# Entry point for the FastAPI application.
# Registers all routers and starts the server.
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.models import Base
from app.database import engine
from app.routers import endpoints, events, attempts
from app.redis_client import redis_client
import asyncio
from worker import worker_loop

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        print("Database Created")

    # create redis stream and worker group 
    try:
        await redis_client.xgroup_create(
            name="webhook_events",
            groupname="delivery_workers",
            id=0,
            mkstream=True
        )
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise
    
    asyncio.create_task(worker_loop())

    yield

app = FastAPI(lifespan=lifespan)
app.include_router(endpoints.router)
app.include_router(events.router)
app.include_router(attempts.router)