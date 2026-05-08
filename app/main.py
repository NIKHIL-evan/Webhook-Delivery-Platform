# Entry point for the FastAPI application.
# Registers all routers and starts the server.
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.models import Base
from app.database import engine
from app.routers import endpoints, events, attempts

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        print("Database Created")
    yield

app = FastAPI(lifespan=lifespan)
app.include_router(endpoints.router)
app.include_router(events.router)
app.include_router(attempts.router)