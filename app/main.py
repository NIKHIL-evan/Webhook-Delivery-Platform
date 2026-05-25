# Entry point for the FastAPI application.
# Registers all routers and starts the server.
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.routers import endpoints, events, attempts, tenants, generate_key
from app.redis_client import redis_client
import asyncio
from worker import worker_loop, retry_loop

@asynccontextmanager
async def lifespan(app: FastAPI):
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
    asyncio.create_task(retry_loop())

    yield

app = FastAPI(lifespan=lifespan)
app.include_router(endpoints.router)
app.include_router(events.router)
app.include_router(attempts.router)
app.include_router(tenants.router)
app.include_router(generate_key.router)