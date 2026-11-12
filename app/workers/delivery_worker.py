import uvloop
import asyncio
import orjson
import hashlib
import hmac
import sys
import random
import time
import uuid
from datetime import UTC, datetime
import collections
import functools

import aiohttp
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
CONCURRENCY_POOL_SIZE = 1000
REDIS_BATCH_SIZE = 1000
RESULTS_BATCH_SIZE = 500
ENDPOINT_CAP = 50

def prepare_payload_and_signature(payload: dict, secret_key: str) -> tuple[bytes, str]:
    """CPU-bound cryptographic and serialization work."""
    payload_bytes = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    signature_hex = hmac.new(secret_key.encode('utf-8'), payload_bytes, hashlib.sha256).hexdigest()
    return payload_bytes, signature_hex

async def results_batcher_loop(results_queue: asyncio.Queue):
    """Consumes delivery results and batches them into a single Redis pipeline trip."""
    base_logger.info("starting_results_batcher")
    while True:
        batch = []
        item = await results_queue.get()
        batch.append(item)
        
        while len(batch) < RESULTS_BATCH_SIZE:
            try:
                batch.append(results_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        
        try:
            async with redis_client.pipeline(transaction=True) as pipe:
                acks_by_stream = collections.defaultdict(list)
                
                for data in batch:
                    stream_name = data["stream_name"]
                    message_id = data["message_id"]
                    acks_by_stream[stream_name].append(message_id)
                    
                    result_payload = {
                        "event_id": data["event_id"],
                        "attempt_number": str(data["attempt_count"]),
                        "status": data["status_str"],
                        "response_code": str(data["resp_code"]) if data["resp_code"] else "0",
                        "attempted_at": str(data["attempted_at"])
                    }
                    pipe.xadd("webhook_results", result_payload)
                
                # Execute the XACKs dynamically based on where they came from
                for stream, msg_ids in acks_by_stream.items():
                    if msg_ids:
                        pipe.xack(stream, "delivery_workers", *msg_ids)
                    
                await pipe.execute()
            
            for data in batch:
                incr("metrics:delivery_attempts")
                add("metrics:queue_delay_total_ms", data["queue_delay_ms"])
                incr("metrics:queue_delay_count")

                if data["status_str"] == "success":
                    incr("metrics:events_delivered_total")
                else:
                    incr("metrics:events_failed_total")

                observe_max("metrics:queue_delay_max_ms", data["queue_delay_ms"])
                
        except Exception as exc:  # noqa: BLE001
            base_logger.error("results_batcher_failed", error=str(exc))
            
        finally:
            for _ in range(len(batch)):
                results_queue.task_done()


async def worker_loop(worker_name: str):
    base_logger.info("starting_delivery_worker_engine", worker_name=worker_name)
    try:
        await redis_client.xgroup_create("webhook_events", "delivery_workers", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise
    try:
        await redis_client.xgroup_create("webhook_retry", "delivery_workers", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise

    metrics_task = asyncio.create_task(flush_forever()) 

    results_queue = asyncio.Queue()
    batcher_task = asyncio.create_task(results_batcher_loop(results_queue))

    # AIOHTTP optimized for massive persistent concurrency
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_POOL_SIZE, keepalive_timeout=30)
    timeout = aiohttp.ClientTimeout(total=10.0, connect=2.0)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as http_client:
        inflight = set() # keeps track of active tasks
        endpoint_counts = collections.defaultdict(int)

        def task_done_callback(endpoint_id: str,task: asyncio.Task):
            inflight.discard(task)
            endpoint_counts[endpoint_id] -= 1
            if endpoint_counts[endpoint_id] == 0:
                del endpoint_counts[endpoint_id]

        last_heartbeat = 0.0  

        current_read_id = "0"

        try:
            while True:
                now = time.monotonic()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL_S:
                    await redis_client.set(
                        f"worker:{worker_name}:last_seen",
                        datetime.now(UTC).timestamp(),
                    )
                    last_heartbeat = now
                
                free_slots = CONCURRENCY_POOL_SIZE - len(inflight)

                if free_slots <= 0:
                    await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                    continue

                count_to_fetch = min(free_slots, REDIS_BATCH_SIZE)

                response = await redis_client.xreadgroup(
                    groupname="delivery_workers",
                    consumername=worker_name,
                    streams={"webhook_events": current_read_id, "webhook_retry": current_read_id},
                    count=count_to_fetch,
                    block=2000
                )
                # THE RECOVERY FLIP: If ID "0" returns empty, we've cleared our ghosts. Switch to ">"
                if not response or not response[0][1]:
                    if current_read_id == "0":
                        base_logger.info("ghost_recovery_complete_switching_to_new")
                        current_read_id = ">"
                    continue

                for stream_name, messages in response:
                    for message_id, fields in messages:
                        
                        endpoint_id = fields["endpoint_id"]

                        # THE BOUNCER LOGIC
                        if endpoint_counts.get(endpoint_id, 0) >= ENDPOINT_CAP:
                            # Park the message in the deferred queue
                            delay_ms = 5000 + random.randint(0, 1000)
                            score_ms = int(time.time() * 1000) + delay_ms
                            
                            async with redis_client.pipeline(transaction=True) as pipe:
                                pipe.zadd("deferred", {orjson.dumps(fields): score_ms})
                                pipe.xack(stream_name, "delivery_workers", message_id) # Acknowledge from the correct stream
                                await pipe.execute()
                            
                        else:
                            endpoint_counts[endpoint_id] += 1
                            task =  asyncio.create_task(
                                process_message(message_id, fields, stream_name, http_client, results_queue)
                            )
                            inflight.add(task)
                            task.add_done_callback(functools.partial(task_done_callback, endpoint_id))

        except asyncio.CancelledError:
            base_logger.info("shutdown_signal_received")
            if inflight:
                await asyncio.wait(inflight, timeout=15.0)
            await results_queue.join()
            batcher_task.cancel()
            metrics_task.cancel()


async def process_message(message_id: str, fields: dict, stream_name: str, http_client: aiohttp.ClientSession, results_queue: asyncio.Queue):
    try:    
        event_id = fields["event_id"]
        destination_url = fields["destination_url"]
        secret_key = fields["signing_secret"]
        payload = orjson.loads(fields["payload"])
        queued_at = float(fields.get("queued_at", time.time()))
        
        queue_delay_ms = (time.time() - queued_at) * 1000
        attempt_count = 1 

        # Offload synchronous CPU work to a background thread to prevent event loop freezing
        payload_bytes, signature_hex = await asyncio.to_thread(
            prepare_payload_and_signature, payload, secret_key
        )

        custom_headers = {
            "X-Webhook-Signature": f"sha256={signature_hex}",
            "Content-Type": "application/json"
        }

        status_str = "failed"
        resp_code = None

        try:
            async with http_client.post(destination_url, data=payload_bytes, headers=custom_headers) as resp:
                resp_code = resp.status
                if resp.status < 300:
                    status_str = "success"
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass 

        await results_queue.put({
            "message_id": message_id,
            "stream_name": stream_name,
            "event_id": event_id,
            "attempt_count": attempt_count,
            "status_str": status_str,
            "resp_code": resp_code,
            "attempted_at": time.time(),
            "queue_delay_ms": queue_delay_ms
        })

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
    uvloop.install()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown sequence complete safely. Exiting.")