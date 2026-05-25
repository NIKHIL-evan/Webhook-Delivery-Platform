from app.redis_client import redis_client
from app.models import Event, DeliveryAttempt, Endpoint, Tenant
import httpx
from sqlalchemy import select
from app.database import AsyncSessionLocal
import asyncio
from datetime import timezone, datetime, timedelta
import random, json, hmac, hashlib

async def worker_loop():
    while True:
        try:
            response = await redis_client.xreadgroup(
                groupname="delivery_workers",
                consumername="worker-1",
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
            
            print(f"worker: processing event {event_id}", flush=True)

            async with AsyncSessionLocal() as session:
                stm = select(Endpoint).where(Endpoint.id == endpoint_id)
                result = await session.execute(stm)
                endpoint = result.scalar_one_or_none()
                destination_url = endpoint.url

                stm2 = select(Event).where(Event.id == event_id)
                result2 = await session.execute(stm2)
                event = result2.scalar_one_or_none()
                payload = event.payload
                attempt_count = event.attempt_count + 1

                stm3 = select(Tenant).where(Tenant.id == endpoint.tenant_id)
                result3 = await session.execute(stm3)
                tenant = result3.scalar_one_or_none()
                
                if not tenant:
                    print(f"worker: Tenant missing for endpoint {endpoint_id}. Skipping.", flush=True)
                    await redis_client.xack("webhook_events", "delivery_workers", message_id)
                    continue

                secret_key = tenant.signing_secret
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
                
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            destination_url,
                            json=payload,
                            headers=custom_headers,
                            timeout=10.0
                        )

                        print(f"worker: status {response.status_code}", flush=True)

                        if response.status_code < 300:
                            attempt = DeliveryAttempt(event_id=event_id, 
                                                        attempt_number=attempt_count,
                                                        status="success",
                                                        response_code=response.status_code)
                            session.add(attempt)

                            event.status = "delivered"
                            event.attempt_count = attempt_count
                            session.add(event)
                            await session.commit()

                            await redis_client.xack("webhook_events", "delivery_workers", message_id)

                        else:
                            print(f"worker: entering failure path attempt {attempt_count}", flush=True)
                            await redis_client.xack("webhook_events", "delivery_workers", message_id)
                            print(f"worker: acked message", flush=True)

                            if attempt_count >= 5:
                                event.status = "dead"
                                event.next_retry_at = None
                                event.attempt_count = attempt_count
                                session.add(event)
                            else:
                                event.status = "failed"
                                event.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=(2 ** attempt_count) + random.randint(0, 3))
                                event.attempt_count = attempt_count
                                session.add(event)

                            f_attempt = DeliveryAttempt(event_id=event_id,
                                                            attempt_number=attempt_count,
                                                            status="failed",
                                                            response_code=response.status_code)
                            session.add(f_attempt)
                            await session.commit()
                            print(f"worker: committed failure for attempt {attempt_count}", flush=True)

                except httpx.RequestError:
                    print(f"worker: entering failure path attempt {attempt_count}", flush=True)
                    await redis_client.xack("webhook_events", "delivery_workers", message_id)
                    print(f"worker: acked message", flush=True)

                    if attempt_count >= 5:
                        event.status = "dead"
                        event.next_retry_at = None
                        event.attempt_count = attempt_count
                        session.add(event)
                    else:
                        event.status = "failed"
                        event.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=(2 ** attempt_count) + random.randint(0, 3))
                        event.attempt_count = attempt_count
                        session.add(event)

                    f2_attempt = DeliveryAttempt(event_id=event_id,
                                                attempt_number=attempt_count,
                                                status="failed",
                                                response_code=None)
                    session.add(f2_attempt)
                    await session.commit()
                    print(f"worker: committed failure for attempt {attempt_count}", flush=True)

        except Exception as e:
            print(f"worker_loop crashed: {e}", flush=True)
            await asyncio.sleep(1)
            
async def retry_loop():
    while True:
        await asyncio.sleep(5)
        try:
            async with AsyncSessionLocal() as session:
                stm = select(Event).where(Event.status == "failed", Event.next_retry_at <= datetime.now(timezone.utc), Event.attempt_count < 5)
                result = await session.execute(stm)
                events = result.scalars().all()

                for event in events:
                    endpoint_id = event.endpoint_id
                    event_id = event.id
                    print(f"retry_scheduler: re-enqueuing {event_id}", flush=True)
                    await redis_client.xadd("webhook_events", {"event_id": str(event_id), "endpoint_id": str(endpoint_id)})

        except Exception as e:
            print(f"worker_loop error: {e}", flush=True)
            await asyncio.sleep(1)
            
if __name__ == "__main__":
    asyncio.run(worker_loop())