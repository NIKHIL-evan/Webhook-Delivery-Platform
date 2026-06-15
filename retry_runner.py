import asyncio

from worker import retry_loop

if __name__ == "__main__":
    asyncio.run(retry_loop())