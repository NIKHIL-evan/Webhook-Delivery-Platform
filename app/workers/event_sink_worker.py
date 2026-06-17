from app.core.redis_client import redis_client
from app.models import Event
from app.core.database import AsyncSessionLocal
from sqlalchemy.dialects.postgresql import insert as pg_insert
import asyncio, uuid, json, time

async def sink_loop():
    print("Starting PostgreSQL Event Sink Worker")
    
    try:
        await redis_client.xgroup_create("webhook_events", "ingestion_workers", mkstream=True)
    except Exception:
        pass 

    while True:
        try:
            response = await redis_client.xreadgroup(
                groupname="ingestion_workers",
                consumername="sink_worker_1",
                streams={"webhook_events": ">"},
                count=500,
                block=2000
            )
            
            if not response:
                continue

            messages = response[0][1]
            batch_data = []
            message_ids = []

            for msg_id, fields in messages:
                idem_key = fields.get("idempotency_key")
                batch_data.append({
                    "id": uuid.UUID(fields["event_id"]),
                    "tenant_id": uuid.UUID(fields["tenant_id"]),
                    "endpoint_id": uuid.UUID(fields["endpoint_id"]),
                    "payload": json.loads(fields["payload"]),
                    "idempotency_key": idem_key if idem_key else None,
                    "trace_id": fields["trace_id"],
                    "status": "queued"
                })
                message_ids.append(msg_id)

            if not batch_data:
                continue
            
            db_start_time = time.perf_counter()
            async with AsyncSessionLocal() as session:
                # ON CONFLICT DO NOTHING handles duplicates via the composite index
                stmt = pg_insert(Event).values(batch_data).on_conflict_do_nothing(
                    index_elements=['tenant_id', 'idempotency_key']
                )
                
                await session.execute(stmt)
                await session.commit()
            
            batch_insert_ms = (time.perf_counter - db_start_time) * 1000

            last_message_fields = messages[-1][1]
            queued_at = float(last_message_fields.get("queued_at", time.time()))
            ingestion_lag_ms = (time.time() - queued_at) * 1000

            async with redis_client.pipeline(transaction=False) as pipe:
                pipe.incrbyfloat("metrics:batch_insert_total_ms", batch_insert_ms)
                pipe.incr("metrics:batch_insert_count")
                pipe.incrbyfloat("metrics:ingestion_lag_total_ms", ingestion_lag_ms)
                pipe.incr("metrics:ingestion_lag_count")
                await pipe.execute()
            
            await redis_client.xack("webhook_events", "ingestion_workers", *message_ids)

        except Exception as e:
            print(f"Sink Worker Error: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(sink_loop())