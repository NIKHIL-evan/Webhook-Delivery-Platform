import asyncio
import collections
import functools
import hashlib
import hmac
import random
import sys
import time
import uuid
from datetime import UTC, datetime

import aiohttp
import orjson
import structlog
import uvloop

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
GLOBAL_ENDPOINT_CAP = 1000
MAX_ATTEMPTS = 5
BASE_BACKOFF_MS = 5000
CIRCUIT_BREAKER_PENALTY_S = 30 
local_circuit_breakers: dict[str, float] = {}

def prepare_payload_and_signature(payload: dict, secret_key: str) -> tuple[bytes, str]:
    """CPU-bound cryptographic and serialization work."""
    payload_bytes = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    signature_hex = hmac.new(secret_key.encode('utf-8'), payload_bytes, hashlib.sha256).hexdigest()
    return payload_bytes, signature_hex

async def results_batcher_loop(results_queue: asyncio.Queue[dict]):
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
                    
                    # --- ROUTING FAILED EVENTS ---
                    if data["status_str"] == "failed":
                        if data["needs_retry"]:
                            # Park it in deferred for the Lua Mover to wake up later
                            score_ms = int(time.time() * 1000) + data["retry_delay_ms"]
                            pipe.zadd("deferred", {orjson.dumps(data["fields"]): score_ms})
                        else:
                            # MAX_ATTEMPTS reached. Send to Dead-Letter Queue.
                            data["fields"]["final_failure_reason"] = f"exhausted_{MAX_ATTEMPTS}_attempts"
                            pipe.xadd("webhook_dlq", data["fields"])
                
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

    results_queue: asyncio.Queue[dict] = asyncio.Queue()
    batcher_task = asyncio.create_task(results_batcher_loop(results_queue))

    # AIOHTTP optimized for massive persistent concurrency
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_POOL_SIZE, keepalive_timeout=30)
    timeout = aiohttp.ClientTimeout(total=10.0, connect=2.0)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as http_client:
        inflight: set[asyncio.Task] = set() # keeps track of active tasks
        endpoint_counts: collections.defaultdict[str, int] = collections.defaultdict(int)

        def task_done_callback(endpoint_id: str,task: asyncio.Task):
            inflight.discard(task)
            endpoint_counts[endpoint_id] -= 1
            if endpoint_counts[endpoint_id] == 0:
                del endpoint_counts[endpoint_id]

        last_heartbeat = 0.0  
        active_worker_count = 1
        current_read_id = "0"
        local_endpoint_cap = GLOBAL_ENDPOINT_CAP

        try:
            while True:
                now = time.monotonic()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL_S:
                    current_ts = datetime.now(UTC).timestamp()
                    
                    async with redis_client.pipeline(transaction=True) as pipe:
                        # 1. Register this worker
                        pipe.zadd("cluster:active_workers", {worker_name: current_ts})
                        # 2. Prune dead workers (no check-in for 15s)
                        pipe.zremrangebyscore("cluster:active_workers", "-inf", current_ts - 15)
                        # 3. Count surviving workers
                        pipe.zcard("cluster:active_workers")
                        results = await pipe.execute()

                    active_worker_count = max(1, results[2])
                    local_endpoint_cap = max(1, GLOBAL_ENDPOINT_CAP // active_worker_count)
                    last_heartbeat = now

                    base_logger.info(
                        "cluster_sync", 
                        worker=worker_name,
                        active_cluster_size=active_worker_count, 
                        global_cap=GLOBAL_ENDPOINT_CAP,
                        new_local_cap=local_endpoint_cap
                    )
                
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
                ) # type: ignore
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
                        if endpoint_counts.get(endpoint_id, 0) >= local_endpoint_cap:
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
        endpoint_id = fields["endpoint_id"]
        destination_url = fields["destination_url"]
        secret_key = fields["signing_secret"]
        payload = orjson.loads(fields["payload"])
        queued_at = float(fields.get("queued_at", time.time()))
        attempt_count = int(fields.get("attempt_count", "1"))
        queue_delay_ms = (time.time() - queued_at) * 1000

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
        current_time = time.time()
        server_requested_delay_ms = 0

        breaker_expiry = local_circuit_breakers.get(endpoint_id, 0)

        if current_time < breaker_expiry:
            # Circuit breaker is active; skip the request
            status_str = "failed"
            resp_code = 503  # Service Unavailable
            base_logger.debug("circuit_breaker_tripped", endpoint_id=endpoint_id)

        else:
            try:
                async with http_client.post(destination_url, data=payload_bytes, headers=custom_headers) as resp:
                    resp_code = resp.status
                    if resp.status < 300:
                        status_str = "success"

                    elif 400 <= resp.status < 500 and resp.status != 429:
                        # FAST-FAIL: 400, 401, 403, 404, etc.
                        status_str = "failed"
                        attempt_count = MAX_ATTEMPTS  # Forces it straight to the DLQ
                        base_logger.info("fatal_4xx_fast_fail", endpoint_id=endpoint_id, status=resp.status)
                    
                    elif resp.status == 429:
                        # RATE LIMIT: Check for Retry-After header
                        status_str = "failed"
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            try:
                                # Convert seconds to ms. (If they send an HTTP Date string instead, 
                                # this fails safely to the except block and defaults to exponential math).
                                server_requested_delay_ms = int(retry_after) * 1000
                            except ValueError:
                                pass
                                
                    elif resp.status >= 500:
                        # SERVER ERROR: Trip the breaker
                        local_circuit_breakers[endpoint_id] = current_time + CIRCUIT_BREAKER_PENALTY_S

            except (TimeoutError, aiohttp.ClientError) as e:
                status_str = "failed"
                local_circuit_breakers[endpoint_id] = current_time + CIRCUIT_BREAKER_PENALTY_S
                base_logger.warning("endpoint_offline_breaker_opened", endpoint_id=endpoint_id, error=str(e))
        
        # --- EXPONENTIAL BACKOFF LOGIC ---
        needs_retry = False
        retry_delay_ms = 0
        
        if status_str == "failed" and attempt_count < MAX_ATTEMPTS:
            needs_retry = True
            if server_requested_delay_ms > 0:
                retry_delay_ms = server_requested_delay_ms
            else:
                # Default Exponential backoff: 5s, 10s, 20s, 40s + jitter
                jitter = random.randint(0, 1000)
                retry_delay_ms = int(BASE_BACKOFF_MS * (2 ** (attempt_count - 1))) + jitter
                
            # Increment the attempt count in the payload so the Mover preserves it
            fields["attempt_count"] = str(attempt_count + 1)

        await results_queue.put({
            "message_id": message_id,
            "stream_name": stream_name,
            "event_id": event_id,
            "attempt_count": attempt_count,
            "status_str": status_str,
            "resp_code": resp_code,
            "attempted_at": time.time(),
            "queue_delay_ms": queue_delay_ms,
            "fields": fields, # Pass the raw Redis payload down
            "needs_retry": needs_retry,
            "retry_delay_ms": retry_delay_ms
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