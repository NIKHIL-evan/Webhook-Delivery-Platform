# Manages the database connection pool.
# Provides a reusable connection instance for routers to interact with Postgres.
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
load_dotenv()

engine = create_async_engine(
    os.getenv("DATABASE_URL"),
    pool_size=15,
    max_overflow=5,    
    pool_timeout=5,     # don't queue for 30s, fail fast
    pool_pre_ping=False,
    pool_recycle=1800,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,
)

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()