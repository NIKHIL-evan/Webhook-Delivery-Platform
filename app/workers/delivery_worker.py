from app.core.redis_client import redis_client
import httpx, asyncio, uuid, sys
from datetime import timezone, datetime
import json, hmac, hashlib
import structlog, time

http_client = httpx.AsyncClient(timeout=10.0)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
base_logger = structlog.get_logger()

# Lua script to atomically update the max queue delay
LUA_UPDATE_MAX = """
local current = redis.call('get', KEYS[1])
if not current or tonumber(ARGV[1]) > tonumber(current) then
    redis.call('set', KEYS[1], ARGV[1])
    return 1
end
return 0
"""

async def worker_loop(worker_name: str):
    print(f"Starting Delivery Worker {worker_name}")
    try:
        await redis_client.xgroup_create("webhook_events", "delivery_workers", mkstream=True)
    except Exception:
        pass

    while True:
        await redis_client.set(f"worker:{worker_name}:last_seen", datetime.now(timezone.utc).timestamp())
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
            destination_url = fields["destination_url"]
            secret_key = fields["signing_secret"]
            payload = json.loads(fields["payload"])
            queued_at = float(fields.get("queued_at", time.time()))
            
            queue_delay_ms = (time.time() - queued_at) * 1000
            attempt_count = 1 

            payload_bytes = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
            signature_hex = hmac.new(secret_key.encode('utf-8'), payload_bytes, hashlib.sha256).hexdigest()

            custom_headers = {
                "X-Webhook-Signature": f"sha256={signature_hex}",
                "Content-Type": "application/json"
            }

            status_str = "failed"
            resp_code = None

            try:
                resp = await http_client.post(destination_url, json=payload, headers=custom_headers)
                resp_code = resp.status_code
                if resp.status_code < 300:
                    status_str = "success"
            except httpx.RequestError as e:
                pass 

            # Push Result to Redis
            result_payload = {
                "event_id": event_id,
                "attempt_number": str(attempt_count),
                "status": status_str,
                "response_code": str(resp_code) if resp_code else "0",
                "attempted_at": str(time.time())
            }
            await redis_client.xadd("webhook_results", result_payload)
            
            # Update Metrics Pipeline
            async with redis_client.pipeline(transaction=False) as pipe:
                pipe.incr("metrics:delivery_attempts")
                pipe.incrbyfloat("metrics:queue_delay_total_ms", queue_delay_ms)
                pipe.incr("metrics:queue_delay_count")
                
                if status_str == "success":
                    pipe.incr("metrics:events_delivered_total")
                else:
                    pipe.incr("metrics:events_failed_total")
                await pipe.execute()

            # Execute Max Queue Delay Lua Script
            await redis_client.eval(LUA_UPDATE_MAX, 1, "metrics:queue_delay_max_ms", queue_delay_ms)

            # Acknowledge the original event
            await redis_client.xack("webhook_events", "delivery_workers", message_id)

        except Exception as e:
            base_logger.error("worker_loop_crashed", error=str(e))
            await asyncio.sleep(1)

if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else f"delivery-{uuid.uuid4().hex[:6]}"
    asyncio.run(worker_loop(name))