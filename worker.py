from app.redis_client import redis_client
from app.models import Event, DeliveryAttempt, Endpoint, Tenant
import httpx
from sqlalchemy import select
from app.database import AsyncSessionLocal
import asyncio
from datetime import timezone, datetime, timedelta
import random, json, hmac, hashlib
import structlog, time

http_client = httpx.AsyncClient(timeout=10.0)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
base_logger = structlog.get_logger()

# Lua script to atomically update max value in Redis
LUA_UPDATE_MAX = """
local current = redis.call('get', KEYS[1])
if not current or tonumber(ARGV[1]) > tonumber(current) then
    redis.call('set', KEYS[1], ARGV[1])
    return 1
end
return 0
"""

async def worker_loop(worker_name: str):
    print(f"Starting {worker_name}")
    while True:
        await redis_client.set(
            f"worker:{worker_name}:last_seen",
            datetime.now(timezone.utc).timestamp()
        )
        try:
            response = await redis_client.xreadgroup(
                groupname="delivery_workers",
                consumername=worker_name,
                streams={"webhook_events": ">"},
                count=1,
                block=2000
            )
            if not response:
                continue

            messages = response[0][1]
            message_id, fields = messages[0]
            event_id = fields["event_id"]
            endpoint_id = fields["endpoint_id"]
            trace_id = fields.get("trace_id", "unknown")

            log = base_logger.bind(
                worker=worker_name,
                trace_id=trace_id,
                event_id=event_id,
                endpoint_id=endpoint_id
            )
            log.info("processing event")

            async with AsyncSessionLocal() as session:

                endpoint = (
                    await session.execute(
                        select(Endpoint).where(Endpoint.id == endpoint_id)
                    )
                ).scalar_one_or_none()

                if not endpoint:
                    await redis_client.xack("webhook_events", "delivery_workers", message_id)
                    continue

                event = (
                    await session.execute(
                        select(Event).where(Event.id == event_id)
                    )
                ).scalar_one_or_none()

                if not event:
                    await redis_client.xack("webhook_events", "delivery_workers", message_id)
                    continue

                tenant = (
                    await session.execute(
                        select(Tenant).where(Tenant.id == endpoint.tenant_id)
                    )
                ).scalar_one_or_none()

                if not tenant:
                    await redis_client.xack("webhook_events", "delivery_workers", message_id)
                    continue

                destination_url = endpoint.url
                payload = event.payload
                attempt_count = event.attempt_count + 1
                secret_key = tenant.signing_secret
                
                
            # 3. Calculate Latency and Queue Metrics
            queued_at = float(fields["queued_at"])
            queue_delay_ms = int((time.time() - queued_at) * 1000)
            
            await redis_client.incrby("metrics:queue_delay_total_ms", queue_delay_ms)
            await redis_client.incr("metrics:queue_delay_count")
            
            # Atomic Max Update using Lua script
            await redis_client.eval(LUA_UPDATE_MAX, 1, "metrics:queue_delay_max_ms", queue_delay_ms)

            log = log.bind(queue_delay_ms=queue_delay_ms)
            log.info("delivery_started")


            payload_json_string = json.dumps(payload, sort_keys=True, separators=(',', ':'))
            payload_bytes = payload_json_string.encode('utf-8')
            
            signature_hex = hmac.new(
                secret_key.encode('utf-8'),
                payload_bytes,
                hashlib.sha256
            ).hexdigest()

            custom_headers = {
                "X-Webhook-Signature": f"sha256={signature_hex}",
                "Content-Type": "application/json"
            }
            
            # 5. Execute HTTP request variables initialized for block scope
            status_str = "failed"
            resp_code = None
            
            try:
                response = await http_client.post(
                    destination_url,
                    json=payload,
                    headers=custom_headers
                )
                await redis_client.incr("metrics:delivery_attempts")
                resp_code = response.status_code
                
                if response.status_code < 300:
                    status_str = "success"
                else:
                    log.warn("delivery_failed_non_2xx", attempt=attempt_count)
                    
            except httpx.RequestError as e:
                log.warn("delivery_network_error", error=str(e), attempt=attempt_count)

            # 6. Apply States and Commit changes dynamically
            async with AsyncSessionLocal() as session:

                event = (await session.execute(select(Event).where(Event.id == event_id))).scalar_one()
                if status_str == "success":
                    event.status = "delivered"
                    event.attempt_count = attempt_count
                    session.add(event)
                    
                    attempt = DeliveryAttempt(event_id=event_id, attempt_number=attempt_count, status="success", response_code=resp_code)
                    session.add(attempt)
                    
                    await session.commit()
                    await redis_client.incr("metrics:events_delivered_total")
                    log.info("delivery_successful")
                else:
                    if attempt_count >= 5:
                        event.status = "dead"
                        event.next_retry_at = None
                        await redis_client.incr("metrics:events_dead_total")
                        log.error("event_marked_dead")
                    else:
                        event.status = "failed"
                        # Exponential backoff: 2^attempt + jitter
                        event.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=(2 ** attempt_count) + random.randint(0, 3))
                        await redis_client.incr("metrics:events_failed_total")

                    event.attempt_count = attempt_count
                    session.add(event)

                    f_attempt = DeliveryAttempt(event_id=event_id, attempt_number=attempt_count, status="failed", response_code=resp_code)
                    session.add(f_attempt)
                    
                    await session.commit()

                # Always acknowledge the stream item if processed through to state resolution
                await redis_client.xack("webhook_events", "delivery_workers", message_id)

        except Exception as e:
            base_logger.error("worker_loop_crashed", error=str(e))
            await asyncio.sleep(1)
            
async def retry_loop():
    while True:
        await redis_client.set(
            "retry_scheduler:last_seen",
            datetime.now(timezone.utc).timestamp()
        )
        await asyncio.sleep(5)
        try:
            async with AsyncSessionLocal() as session:
                # Query failed items
                stm = select(Event).where(
                    Event.status == "failed", 
                    Event.next_retry_at <= datetime.now(timezone.utc), 
                    Event.attempt_count < 5
                )
                result = await session.execute(stm)
                events = result.scalars().all()

                for event in events:
                    log = base_logger.bind(
                        trace_id=str(event.trace_id), 
                        event_id=str(event.id), 
                        component="retry_scheduler"
                    )
                    log.info("re_enqueuing_event")

                    # Crucial Fix: Change status to 'queued' so next iteration ignores it
                    event.status = "queued"
                    session.add(event)

                    await redis_client.xadd(
                        "webhook_events",
                        {
                            "event_id": str(event.id),
                            "endpoint_id": str(event.endpoint_id),
                            "trace_id": str(event.trace_id) or "unknown",
                            "queued_at": str(time.time())
                        }
                    )
                
                # Commit all status updates to 'queued' at once
                if events:
                    await session.commit()

        except Exception as e:
            base_logger.error("retry_loop_crashed", error=str(e))
            await asyncio.sleep(1)


