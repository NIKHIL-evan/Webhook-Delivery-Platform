# Manages the database connection pool.
# Provides a reusable connection instance for routers to interact with Postgres.
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
load_dotenv()

engine = create_async_engine(os.getenv("DATABASE_URL"))

AsyncSessionLocal = async_sessionmaker(engine)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

