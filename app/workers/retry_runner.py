from app.core.redis_client import redis_client
from app.models import Event, Endpoint, Tenant
from app.core.database import AsyncSessionLocal
from sqlalchemy import select
import asyncio, time, json
from datetime import timezone, datetime
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
base_logger = structlog.get_logger()

async def retry_loop():
    print("Starting Retry Scheduler")
    while True:
        await redis_client.set(
            "retry_scheduler:last_seen",
            datetime.now(timezone.utc).timestamp()
        )
        await asyncio.sleep(5)
        try:
            async with AsyncSessionLocal() as session:
                stm = (
                    select(Event, Endpoint, Tenant)
                    .join(Endpoint, Event.endpoint_id == Endpoint.id)
                    .join(Tenant, Event.tenant_id == Tenant.id)
                    .where(
                        Event.status == "failed",
                        Event.next_retry_at <= datetime.now(timezone.utc),
                        Event.attempt_count < 5
                    )
                )
                result = await session.execute(stm)
                events = result.scalars().all()

                for event, endpoint, tenant in events:
                    log = base_logger.bind(
                        trace_id=str(event.trace_id),
                        event_id=str(event.id),
                        component="retry_scheduler"
                    )
                    log.info("re_enqueuing_event")
                    event.status = "queued"
                    session.add(event)
                    
                    await redis_client.xadd(
                        "webhook_events",
                        {
                            "event_id": str(event.id),
                            "tenant_id": str(event.tenant_id),
                            "endpoint_id": str(event.endpoint_id),
                            "destination_url": str(endpoint.url), 
                            "signing_secret": str(tenant.signing_secret),
                            "payload": json.dumps(event.payload),
                            "trace_id": str(event.trace_id) or "unknown",
                            "queued_at": str(time.time())
                        }
                    )
                    
                if events:
                    await session.commit()

        except Exception as e:
            base_logger.error("retry_loop_crashed", error=str(e))
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(retry_loop())