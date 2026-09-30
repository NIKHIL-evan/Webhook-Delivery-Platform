from app.core.redis_client import redis_client

RATE_LIMIT_WINDOW_S = 60       # rate-limit window (same as before)
IDEMPOTENCY_TTL_S = 86_400     # idempotency keys live 24 h (same as before)
STREAM_KEY = "webhook_events"

# ---------------------------------------------------------
# INGEST SCRIPT: rate limit → idempotency check → XADD → store key
# Runs inside Redis as one uninterrupted step (isolation, NOT rollback).
# Order B: the key is stored AFTER the XADD, so a failure halfway
# causes at worst a duplicate on retry, never a lost event.
# ---------------------------------------------------------
INGEST_LUA = """
-- KEYS[1] = rate_limit:<tenant_id>
-- KEYS[2] = idem:<tenant_id>:<idempotency_key>  (placeholder if none)
-- KEYS[3] = webhook_events
-- ARGV[1] = rate limit     ARGV[2] = window seconds
-- ARGV[3] = idem TTL       ARGV[4] = new event_id
-- ARGV[5] = '1' if the request has an idempotency key, else '0'
-- ARGV[6..] = stream fields: field1, value1, field2, value2, ...

-- 1. Rate limit
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
if count > tonumber(ARGV[1]) then
    return {'limited'}
end

local has_idem = ARGV[5] == '1'

-- 2. Idempotency check (read only)
if has_idem then
    local existing = redis.call('GET', KEYS[2])
    if existing then
        return {'duplicate', existing}
    end
end

-- 3. Enqueue
redis.call('XADD', KEYS[3], '*', unpack(ARGV, 6))

-- 4. Remember the idempotency key
if has_idem then
    redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[3])
end

return {'queued', ARGV[4]}
"""

_ingest_script = redis_client.register_script(INGEST_LUA)


async def ingest_event(
    *,
    tenant_id: str,
    rate_limit: int,
    idempotency_key: str | None,
    event_id: str,
    fields: dict[str, str],
) -> tuple[str, str | None]:
    """Returns (status, event_id). status: 'limited' | 'duplicate' | 'queued'.
    Raises on Redis errors, and callers must let that become a 500."""
    has_idem = bool(idempotency_key)
    idem_key = (
        f"idem:{tenant_id}:{idempotency_key}" if has_idem
        else f"idem:{tenant_id}:__none__"   # never touched when has_idem is False
    )

    flat_fields: list[str] = []
    for name, value in fields.items():
        flat_fields += [name, value]

    result = await _ingest_script(
        keys=[f"rate_limit:{tenant_id}", idem_key, STREAM_KEY],
        args=[
            rate_limit,
            RATE_LIMIT_WINDOW_S,
            IDEMPOTENCY_TTL_S,
            event_id,
            "1" if has_idem else "0",
            *flat_fields,
        ],
    )
    status = result[0]
    returned_id = result[1] if len(result) > 1 else None
    return status, returned_id