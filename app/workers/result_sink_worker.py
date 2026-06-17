from app.core.redis_client import redis_client
from app.models import DeliveryAttempt
from app.core.database import AsyncSessionLocal
from sqlalchemy.dialects.postgresql import insert as pg_insert
import asyncio, uuid
from datetime import timezone, datetime

async def results_sink_loop():
    print("Starting Results Sink Worker")
    try:
        await redis_client.xgroup_create("webhook_results", "results_ingestion_workers", mkstream=True)
    except Exception:
        pass 

    while True:
        try:
            response = await redis_client.xreadgroup(
                groupname="results_ingestion_workers",
                consumername="results_sink_worker_1",
                streams={"webhook_results": ">"},
                count=500,
                block=2000
            )
            
            if not response:
                continue

            messages = response[0][1]
            batch_data = []
            message_ids = []

            for msg_id, fields in messages:
                batch_data.append({
                    "id": uuid.uuid4(),
                    "event_id": uuid.UUID(fields["event_id"]),
                    "attempt_number": int(fields["attempt_number"]),
                    "status": fields["status"],
                    "response_code": int(fields["response_code"]) if fields["response_code"] != "0" else None,
                    "attempted_at": datetime.fromtimestamp(float(fields["attempted_at"]), tz=timezone.utc)
                })
                message_ids.append(msg_id)

            if not batch_data:
                continue

            async with AsyncSessionLocal() as session:
                stmt = pg_insert(DeliveryAttempt).values(batch_data)
                await session.execute(stmt)
                await session.commit()
            
            await redis_client.xack("webhook_results", "results_ingestion_workers", *message_ids)

        except Exception as e:
            print(f"Results Sink Worker Error: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(results_sink_loop())