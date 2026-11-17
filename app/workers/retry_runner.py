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

LUA_MOVER_SCRIPT = """
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local due_items = redis.call('ZRANGEBYSCORE', 'deferred', '-inf', now_ms, 'LIMIT', 0, 500)
if #due_items == 0 then return 0 end
for i, member in ipairs(due_items) do
    local fields = cjson.decode(member)
    local xadd_args = {'webhook_retry', '*'}
    for k, v in pairs(fields) do
        table.insert(xadd_args, k)
        table.insert(xadd_args, tostring(v))
    end
    redis.call('XADD', unpack(xadd_args))
    redis.call('ZREM', 'deferred', member)
end
return #due_items
"""

async def mover_loop():
    print("Starting Redis Deferred Mover Loop")
    while True:
        try:
            await redis_client.eval(LUA_MOVER_SCRIPT, 0)
        except Exception as e:
            base_logger.error("mover_loop_crashed", error=str(e))

        await asyncio.sleep(0.1)  # Sleep before checking again


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

# ---------------------------------------------------------
# STREAM TRIMMER: delete only entries EVERY consumer group has finished.
# Never trims unread or un-ACKed entries, so it can't lose events.
# ---------------------------------------------------------
STREAM_GROUPS = {
    "webhook_events": ["delivery_workers", "ingestion_workers"],
    "webhook_results": ["results_ingestion_workers"],
}
TRIM_INTERVAL_S = 10

def _id_key(stream_id: str) -> tuple[int, int]:
    # "1790600000000-12" → (1790600000000, 12), so IDs compare as numbers
    ms, seq = stream_id.split("-")
    return int(ms), int(seq)

async def _safe_trim_point(stream: str, expected_groups: list[str]) -> str | None:
    groups = {g["name"]: g for g in await redis_client.xinfo_groups(stream)}
    candidates = []
    for name in expected_groups:
        group = groups.get(name)
        if group is None:
            return None                      # a group is missing → trim nothing
        if group["pending"] > 0:
            summary = await redis_client.xpending(stream, name)
            candidates.append(summary["min"])          # oldest un-ACKed entry
        else:
            candidates.append(group["last-delivered-id"])
    return min(candidates, key=_id_key)

async def stream_trimmer_loop():
    print("Starting Stream Trimmer")
    while True:
        await asyncio.sleep(TRIM_INTERVAL_S)
        for stream, groups in STREAM_GROUPS.items():
            try:
                min_id = await _safe_trim_point(stream, groups)
                if not min_id or min_id == "0-0":
                    continue
                removed = await redis_client.xtrim(stream, minid=min_id, approximate=True)
            except Exception as e:
                base_logger.error("stream_trimmer_failed", stream=stream, error=str(e))
                
async def main():
    # Run both loops concurrently
    await asyncio.gather(
        redis_sweeper_loop(),
        stream_trimmer_loop(),
        mover_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())