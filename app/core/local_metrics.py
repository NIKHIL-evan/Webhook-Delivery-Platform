from app.core.redis_client import redis_client
import structlog, asyncio

base_logger = structlog.get_logger()

_counts, _totals, _maxes = {},{},{}

def incr(name, amount=1):
    _counts[name] = _counts.get(name, 0) + amount

def add(name, value):
    _totals[name] = _totals.get(name, 0) + value

def observe_max(name, value):
    current = _maxes.get(name, 0) 
    _maxes[name] = max(current, value)

async def flush_forever():
    global _counts, _totals, _maxes
    while True:
        await asyncio.sleep(1)

        try:
            counts, _counts = _counts, {}
            totals, _totals = _totals, {}
            maxes, _maxes = _maxes, {}

            if not any([counts, totals, maxes]):
                continue

            async with redis_client.pipeline(transaction=False) as pipe:
                for name, value in counts.items():
                    pipe.incrby(name, value)
                for name, value in totals.items():
                    pipe.incrbyfloat(name, value)
                for name, value in maxes.items():
                    pipe.eval(
                        """
                        local current = redis.call("GET", KEYS[1])
                        if not current or tonumber(current) < tonumber(ARGV[1]) then
                            redis.call("SET", KEYS[1], ARGV[1])
                        end
                        """,
                        1,
                        name,
                        value
                    )
                await pipe.execute()
            
        except Exception as e:
            base_logger.error("Error flushing metrics to Redis", error=str(e))
