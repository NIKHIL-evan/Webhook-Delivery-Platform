import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

load_dotenv()

engine = create_async_engine(
    os.getenv("DATABASE_URL"),
    pool_size=5,        # 12 processes * 5 = 60 base connections
    max_overflow=2,     # 12 processes * 2 = 24 overflow connections (84 total max)
    pool_timeout=10,    # Increased slightly to handle burst queuing safely
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