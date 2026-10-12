from app.core.redis_client import redis_client

RATE_LIMIT_WINDOW_S = 60       # "per minute": a full bucket refills in this many seconds
IDEMPOTENCY_TTL_S = 86_400     # idempotency keys live 24 h (same as before)
STREAM_KEY = "webhook_events"


def rate_limit_key(tenant_id: str) -> str:
    """The ONE place the rate-limit key is built (both scripts use it)."""
    return f"rl:{tenant_id}"   # new name: the old 'rate_limit:*' keys were plain counters


# ---------------------------------------------------------
# TOKEN BUCKET (Round 2 / Task 5) — shared by every rate-limited path.
#   capacity  = tenant's rate_limit  → max burst
#   refill    = capacity per window  → same average as before (rate_limit / minute)
# Lazy refill: we store only (tokens, ts) and compute the drip when a request arrives.
# Runs inside Redis, so read → refill → take → save can't be interrupted (no race).
# Uses Redis's clock (TIME), so every API process/server agrees on "now".
# Returns two integers (Lua numbers are truncated to integers when returned to Python).
# ---------------------------------------------------------
TOKEN_BUCKET_LUA = """
local function take_token(key, capacity, window_ms)
    local rate = capacity / window_ms                      -- tokens per millisecond

    local t = redis.call('TIME')                           -- {seconds, microseconds} as strings
    local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

    local state = redis.call('HMGET', key, 'tokens', 'ts')
    local tokens = tonumber(state[1]) or capacity           -- no bucket yet = full
    local last = tonumber(state[2]) or now

    local elapsed = math.max(0, now - last)                -- never negative
    tokens = math.min(capacity, tokens + elapsed * rate)   -- lazy refill, capped

    local allowed, wait_ms = 1, 0
    if tokens >= 1 then
        tokens = tokens - 1
    else
        allowed = 0
        wait_ms = math.ceil((1 - tokens) / rate)           -- ms until 1 whole token
    end

    redis.call('HSET', key, 'tokens', tokens, 'ts', now)
    redis.call('PEXPIRE', key, window_ms)                  -- idle 1 window = full again → delete
    return allowed, wait_ms
end
"""

# Standalone check: other routes + endpoint-cache misses (via _apply_rate_limit)
RATE_LIMIT_LUA = TOKEN_BUCKET_LUA + """
-- KEYS[1] = rl:<tenant_id>   ARGV[1] = capacity   ARGV[2] = window seconds
local allowed, wait_ms = take_token(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]) * 1000)
return {allowed, wait_ms}
"""

_rate_limit_script = redis_client.register_script(RATE_LIMIT_LUA)


async def check_rate_limit(tenant_id: str, rate_limit: int) -> tuple[bool, int]:
    """Takes one token. Returns (allowed, wait_ms)."""
    allowed, wait_ms = await _rate_limit_script(
        keys=[rate_limit_key(tenant_id)],
        args=[rate_limit, RATE_LIMIT_WINDOW_S],
    )
    return allowed == 1, int(wait_ms)


# ---------------------------------------------------------
# INGEST SCRIPT: rate limit → idempotency check → XADD → store key
# Runs inside Redis as one uninterrupted step (isolation, NOT rollback).
# Order B: the key is stored AFTER the XADD, so a failure halfway
# causes at worst a duplicate on retry, never a lost event.
# ---------------------------------------------------------
INGEST_LUA = TOKEN_BUCKET_LUA + """
-- KEYS[1] = rl:<tenant_id>
-- KEYS[2] = idem:<tenant_id>:<idempotency_key>  (placeholder if none)
-- KEYS[3] = webhook_events
-- ARGV[1] = rate limit     ARGV[2] = window seconds
-- ARGV[3] = idem TTL       ARGV[4] = new event_id
-- ARGV[5] = '1' if the request has an idempotency key, else '0'
-- ARGV[6..] = stream fields: field1, value1, field2, value2, ...

-- 1. Rate limit (token bucket)
local allowed, wait_ms = take_token(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]) * 1000)
if allowed == 0 then
    return {'limited', wait_ms}
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
) -> tuple[str, str | int | None]:
    """Returns (status, value).
      'queued'    → value = event_id
      'duplicate' → value = the original event_id
      'limited'   → value = wait_ms (int) until a token is available
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
        keys=[rate_limit_key(tenant_id), idem_key, STREAM_KEY],
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
    value = result[1] if len(result) > 1 else None
    return status, value