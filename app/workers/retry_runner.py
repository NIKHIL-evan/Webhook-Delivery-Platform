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

async def postgres_retry_loop():
    print("Starting PostgreSQL Retry Scheduler")
    while True:
        await redis_client.set("retry_scheduler:last_seen", time.time())
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
                rows = result.all()

                for event, endpoint, tenant in rows:
                    log = base_logger.bind(
                        trace_id=str(event.trace_id),
                        event_id=str(event.id),
                        component="retry_scheduler"
                    )
                    log.info("re_enqueuing_postgres_event")
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
                    
                if rows:
                    await session.commit()

        except Exception as e:
            base_logger.error("postgres_retry_loop_crashed", error=str(e))
            await asyncio.sleep(1)

async def redis_sweeper_loop():
    print("Starting Redis Dead-Letter Sweeper")
    while True:
        await asyncio.sleep(10) # Run every 10 seconds
        try:
            # 1. Sweep webhook_events for dead delivery workers (idle > 30 seconds)
            claim_response = await redis_client.xautoclaim(
                name="webhook_events",
                groupname="delivery_workers",
                consumername="sweeper_worker",
                min_idle_time=30000, 
                count=100
            )
            
            if claim_response and len(claim_response) == 2:
                claimed_messages = claim_response[1]
                for msg_id, fields in claimed_messages:
                    base_logger.info("reclaiming_dead_message", message_id=msg_id)
                    
                    # Acknowledge the dead message to clear it from the PEL
                    await redis_client.xack("webhook_events", "delivery_workers", msg_id)
                    
                    # Inject a fresh timestamp and requeue it for the healthy workers
                    fields["queued_at"] = str(time.time())
                    await redis_client.xadd("webhook_events", fields)

        except Exception as e:
            base_logger.error("redis_sweeper_loop_crashed", error=str(e))

async def main():
    # Run both loops concurrently
    await asyncio.gather(
        postgres_retry_loop(),
        redis_sweeper_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())