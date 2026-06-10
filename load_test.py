import asyncio
import aiohttp
import time
from collections import Counter

API_KEY = "wh_live_F1Gx9zkHO4qGfiuuIEi1PWi6gV7zrclLQ5zhdkkeNHk"
ENDPOINT_ID = "2232c8f6-71a9-4a58-9b0b-76585a0845ca"

URL = "http://localhost:8000/events"

latencies = []

semaphore = asyncio.Semaphore(10)

async def send_request(session, i):
    async with semaphore:
        start = time.perf_counter()

        async with session.post(
            URL,
            headers={"API-Key": API_KEY},
            json={
                "endpoint_id": ENDPOINT_ID,
                "payload": {"request": i}
            }
        ) as response:
            await response.text()

        end = time.perf_counter()
        latencies.append((end - start) * 1000)

        return response.status

async def main():
    start = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, i) for i in range(10000)]
        results = await asyncio.gather(*tasks)

    end = time.perf_counter()
    latencies.sort()

    p50 = latencies[int(len(latencies) * 0.50)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print(f"p50: {p50:.2f} ms")
    print(f"p95: {p95:.2f} ms")
    print(f"p99: {p99:.2f} ms")
    rps = len(results) / (end - start)
    print(f"Requests/sec: {rps:.2f}")
    print("Requests:", len(results))
    success = sum(1 for r in results if 200 <= r < 300)
    failures = len(results) - success

    print("Success:", success)
    print("Failures:", failures)
    print("Duration:", round(end - start, 2), "seconds")

    status_counts = Counter(results)

    print("\nStatus Codes:")
    for status, count in sorted(status_counts.items()):
        print(f"{status}: {count}")

asyncio.run(main())