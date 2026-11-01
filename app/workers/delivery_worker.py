import asyncio
import hashlib
import hmac
import json
import sys
import time
import uuid
from datetime import UTC, datetime

import httpx
import structlog

from app.core.local_metrics import add, flush_forever, incr, observe_max
from app.core.redis_client import redis_client

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
base_logger = structlog.get_logger()

HEARTBEAT_INTERVAL_S = 5 
CONCURRENCY_POOL_SIZE = 600
REDIS_BATCH_SIZE = 100

async def worker_loop(worker_name: str):
    base_logger.info("starting_delivery_worker_engine", worker_name=worker_name)
    try:
        await redis_client.xgroup_create("webhook_events", "delivery_workers", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise

    metrics_task = asyncio.create_task(flush_forever()) 
    shared_queue = asyncio.Queue(maxsize=CONCURRENCY_POOL_SIZE)

    limits = httpx.Limits(max_connections=CONCURRENCY_POOL_SIZE+400, max_keepalive_connections=100)
    timeout = httpx.Timeout(10.0, connect=2.0, read=5.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as http_client:
        pool_workers = []
        for i in range(CONCURRENCY_POOL_SIZE):
            task = asyncio.create_task(process_message_from_queue(i, shared_queue, http_client))
            pool_workers.append(task)

        last_heartbeat = 0.0  

        try:
            while True:
                now = time.monotonic()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL_S:
                    await redis_client.set(
                        f"worker:{worker_name}:last_seen",
                        datetime.now(UTC).timestamp(),
                    )
                    last_heartbeat = now

                response = await redis_client.xreadgroup(
                    groupname="delivery_workers",
                    consumername=worker_name,
                    streams={"webhook_events": ">"},
                    count=REDIS_BATCH_SIZE,
                    block=2000
                )
                if not response:
                    continue

                messages = response[0][1]
                for message_id, fields in messages:
                    await shared_queue.put((message_id, fields))

        except asyncio.CancelledError:
            base_logger.info("shutdown_signal_received", strategy="draining_internal_queue")
            
            await shared_queue.join()
            
            for task in pool_workers:
                task.cancel()
            metrics_task.cancel()
            
            base_logger.info("graceful_shutdown_complete")


async def process_message_from_queue(worker_id: int, queue: asyncio.Queue, http_client: httpx.AsyncClient):
    """Long-running permanent worker thread abstraction sitting in local RAM."""
    while True:
        message_id, fields = await queue.get()
        try:
            await process_message(message_id, fields, http_client)
        finally:
            queue.task_done()


async def process_message(message_id: str, fields: dict, http_client: httpx.AsyncClient):
    try:    
        event_id = fields["event_id"]
        destination_url = fields["destination_url"]
        secret_key = fields["signing_secret"]
        payload = json.loads(fields["payload"])
        queued_at = float(fields.get("queued_at", time.time()))
        
        queue_delay_ms = (time.time() - queued_at) * 1000
        attempt_count = 1 

        payload_bytes = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        signature_hex = hmac.new(secret_key.encode('utf-8'), payload_bytes, hashlib.sha256).hexdigest()

        custom_headers = {
            "X-Webhook-Signature": f"sha256={signature_hex}",
            "Content-Type": "application/json"
        }

        status_str = "failed"
        resp_code = None

        try:
            resp = await http_client.post(destination_url, content=payload_bytes, headers=custom_headers)
            resp_code = resp.status_code
            if resp.status_code < 300:
                status_str = "success"
        except httpx.RequestError:
            pass 

        result_payload = {
            "event_id": event_id,
            "attempt_number": str(attempt_count),
            "status": status_str,
            "response_code": str(resp_code) if resp_code else "0",
            "attempted_at": str(time.time())
        }
        
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.xadd("webhook_results", result_payload)
            pipe.xack("webhook_events", "delivery_workers", message_id)
            await pipe.execute()

        incr("metrics:delivery_attempts")
        add("metrics:queue_delay_total_ms", queue_delay_ms)
        incr("metrics:queue_delay_count")

        if status_str == "success":
            incr("metrics:events_delivered_total")
        else:
            incr("metrics:events_failed_total")

        observe_max("metrics:queue_delay_max_ms", queue_delay_ms)

    except Exception as exc:  # noqa: BLE001
        base_logger.error("payload_processing_exception", error=str(exc), message_id=message_id)


async def main():
    worker_name = sys.argv[1] if len(sys.argv) > 1 else f"delivery-{uuid.uuid4().hex[:6]}"
    loop_task = asyncio.create_task(worker_loop(worker_name))
    
    try:
        await loop_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown sequence complete safely. Exiting.")