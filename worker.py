from app.redis_client import redis_client
from app.models import Events, DeliveryAttempts, Endpoints
import httpx
from sqlalchemy import select
from app.database import AsyncSessionLocal
import asyncio

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

            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        destination_url,
                        json=payload,
                        timeout=10.0  #to catch httpx.TimeoutException if destination url hangs
                    )
                    if response.status_code == 200:
                        attempt = DeliveryAttempts(event_id=event_id, 
                                                    attempt_number=1,
                                                    status="success",
                                                    response_code= response.status_code)
                        session.add(attempt)
                        await session.commit()

                        event.status = "delivered"
                        session.add(event)
                        await session.commit()

                        # Acknowledge to stream
                        await redis_client.xack("webhook_events", "delivery_workers", message_id)

                    else:
                        f_attempt = DeliveryAttempts(event_id=event_id,
                                                        attempt_number=1,
                                                        status="failed",
                                                        response_code=response.status_code)
                        session.add(f_attempt)
                        await session.commit()

            except  httpx.RequestError:
                f2_attempt = DeliveryAttempts(event_id=event_id,
                                            attempt_number=1,
                                            status="failed",
                                            response_code=None)
                session.add(f2_attempt)
                await session.commit()

              
if __name__ == "__main__":
    asyncio.run(worker_loop())