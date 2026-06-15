import asyncio
import aiohttp
import time
from collections import Counter

API_KEY = "wh_live_F1Gx9zkHO4qGfiuuIEi1PWi6gV7zrclLQ5zhdkkeNHk"
ENDPOINT_ID = "2232c8f6-71a9-4a58-9b0b-76585a0845ca"
URL = "http://localhost:8000/events"

TOTAL_REQUESTS = 10000
CONCURRENCY = 1000

latencies = []
semaphore = asyncio.Semaphore(CONCURRENCY)

async def send_request(session, i):
    async with semaphore:
        try:
            start = time.perf_counter()
            async with session.post(
                URL,
                headers={"API-Key": API_KEY},
                json={
                    "endpoint_id": ENDPOINT_ID,
                    "payload": {"request": i}
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                await response.read()
            latencies.append((time.perf_counter() - start) * 1000)
            return response.status
        except Exception as e:
            print(type(e).__name__, e)
            return 0

async def main():
    connector = aiohttp.TCPConnector(limit=2000, limit_per_host=2000)
    start = time.perf_counter()

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [send_request(session, i) for i in range(TOTAL_REQUESTS)]
        results = await asyncio.gather(*tasks)

    end = time.perf_counter()
    duration = end - start

    successful_latencies = sorted(latencies)
    n = len(successful_latencies)

    print(f"\np50: {successful_latencies[int(n * 0.50)]:.2f} ms")
    print(f"p95: {successful_latencies[int(n * 0.95)]:.2f} ms")
    print(f"p99: {successful_latencies[int(n * 0.99)]:.2f} ms")
    print(f"Requests/sec: {len(results) / duration:.2f}")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Total: {len(results)}")
    print(f"Success: {sum(1 for r in results if 200 <= r < 300)}")
    print(f"Failures: {sum(1 for r in results if r == 0)}")
    print(f"Non-2xx: {sum(1 for r in results if r not in (0,) and not (200 <= r < 300))}")

    print("\nStatus Codes:")
    for status, count in sorted(Counter(results).items()):
        print(f"  {status}: {count}")

if __name__ == "__main__":
    asyncio.run(main())