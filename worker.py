from app.redis_client import redis_client
from app.models import Events, DeliveryAttempts, Endpoints
import httpx
from sqlalchemy import select
from app.database import AsyncSessionLocal
import asyncio
from datetime import timezone, datetime, timedelta
import random

async def worker_loop():
    while True:

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
        

        async with AsyncSessionLocal() as session:
            stm = select(Endpoints).where(Endpoints.url_id == endpoint_id)
            result = await session.execute(stm)
            endpoint = result.scalar_one_or_none()
            destination_url = endpoint.url

            stm2 = select(Events).where(Events.event_id == event_id)
            result2 = await session.execute(stm2)
            event = result2.scalar_one_or_none()
            payload = event.payload
            attempt_count = event.attempt_count + 1

            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        destination_url,
                        json=payload,
                        timeout=10.0  #to catch httpx.TimeoutException if destination url hangs
                    )
                    if response.status_code < 300:
                        attempt = DeliveryAttempts(event_id=event_id, 
                                                    attempt_number=attempt_count,
                                                    status="success",
                                                    response_code= response.status_code)
                        session.add(attempt)

                        event.status = "delivered"
                        event.attempt_count = attempt_count
                        session.add(event)
                        await session.commit()

                        # Acknowledge to stream
                        await redis_client.xack("webhook_events", "delivery_workers", message_id)

                    else:
                        await redis_client.xack("webhook_events", "delivery_workers", message_id)

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

                        f_attempt = DeliveryAttempts(event_id=event_id,
                                                        attempt_number=attempt_count,
                                                        status="failed",
                                                        response_code=response.status_code)
                        session.add(f_attempt)
                        await session.commit()

            except  httpx.RequestError:
                await redis_client.xack("webhook_events", "delivery_workers", message_id)

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

                                
                f2_attempt = DeliveryAttempts(event_id=event_id,
                                            attempt_number=attempt_count,
                                            status="failed",
                                            response_code=None)
                session.add(f2_attempt)
                await session.commit()

async def retry_loop():
    while True:
        await asyncio.sleep(5)
        async with AsyncSessionLocal() as session:
            stm = select(Events).where(Events.status == "failed", Events.next_retry_at <= datetime.now(timezone.utc), Events.attempt_count < 5)
            result = await session.execute(stm)
            events = result.scalars().all()
            for event in events:
                url_id = event.url_id
                event_id = event.event_id
                await redis_client.xadd("webhook_events", {"event_id": str(event_id), "endpoint_id": str(url_id)})
                event.status = "pending"
                session.add(event)
            await session.commit()
            




























if __name__ == "__main__":
    asyncio.run(worker_loop())