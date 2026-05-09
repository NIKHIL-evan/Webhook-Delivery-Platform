Failure Modes — Phase 0
# Database is unreachable

    What happens: PostgreSQL is down when an API request tries to access the database.

    Current behavior: SQLAlchemy raises an exception and the API returns:

    {
    "detail": "Database error"
    }

    with status code 500.

    Impact: Endpoints and events cannot be created until the database is available again.

# Destination URL is unreachable

    What happens: The destination webhook server is offline or connection fails.

    Current behavior: httpx.RequestError is raised, a failed delivery attempt is stored, and no retry happens.

    Impact: The event delivery fails permanently in Phase 0.

# Destination URL hangs

    What happens: The destination server accepts the request but never responds.

    Current behavior: httpx waits until the 10s timeout is reached, then marks delivery as failed.

    Impact: API response is delayed because delivery is synchronous.

# Invalid payload

    What happens: Request body is missing required fields or contains invalid data.

    Current behavior: FastAPI/Pydantic return 422 Unprocessable Entity.

    Impact: Invalid requests are rejected before database operations occur.

# Worker crashes mid-delivery

    What happens: Not applicable in Phase 0.

    Current behavior: Delivery happens directly inside the API request. No background workers exist yet.

    Impact: If the app crashes during delivery, the request fails and no automatic recovery exists.


Failure modes - Phase 1:
# Phase 1 Design Notes

## Event Flow

When the API receives a `POST /events` request, the event is first saved to PostgreSQL before being written to the Redis Stream. PostgreSQL acts as the durable source of truth for the system. If the application crashes after saving to Postgres but before writing to Redis, the event still exists safely in the database and the queue can later be rebuilt from it.

After the database commit succeeds, the API writes a delivery job into the Redis Stream. The stream acts as the asynchronous delivery mechanism used by workers.

---

## Worker Flow

When a worker picks up a message from the Redis Stream, it performs the following steps:

1. Read the event information from the stream message.
2. Fetch the event and endpoint URL from PostgreSQL.
3. Send the webhook HTTP POST request to the destination URL.
4. If delivery succeeds:
   - record a successful delivery attempt in PostgreSQL
   - update the event status to `delivered`
   - acknowledge the message to Redis
5. If delivery fails:
   - record a failed delivery attempt in PostgreSQL
   - leave the message unacknowledged for future retry handling

---

## At-Least-Once Delivery

At-least-once delivery means the system guarantees that a webhook event will be delivered one or more times, but possibly more than once.

Example failure scenario:

1. Worker sends the webhook successfully.
2. Destination server receives the payload and responds with `200 OK`.
3. Worker crashes before acknowledging the message to Redis.

From Redis's perspective, the message was never acknowledged, so it remains pending and can later be retried by another worker. This prevents message loss, but it also means the destination may receive duplicate webhook deliveries.

The accepted side effect of at-least-once delivery is duplicate event delivery.
