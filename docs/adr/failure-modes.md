Failure Modes — Phase 0
Database is unreachable

    What happens: PostgreSQL is down when an API request tries to access the database.

    Current behavior: SQLAlchemy raises an exception and the API returns:

    {
    "detail": "Database error"
    }

    with status code 500.

    Impact: Endpoints and events cannot be created until the database is available again.

Destination URL is unreachable

    What happens: The destination webhook server is offline or connection fails.

    Current behavior: httpx.RequestError is raised, a failed delivery attempt is stored, and no retry happens.

    Impact: The event delivery fails permanently in Phase 0.

Destination URL hangs

    What happens: The destination server accepts the request but never responds.

    Current behavior: httpx waits until the 10s timeout is reached, then marks delivery as failed.

    Impact: API response is delayed because delivery is synchronous.

Invalid payload

    What happens: Request body is missing required fields or contains invalid data.

    Current behavior: FastAPI/Pydantic return 422 Unprocessable Entity.

    Impact: Invalid requests are rejected before database operations occur.

Worker crashes mid-delivery

    What happens: Not applicable in Phase 0.

    Current behavior: Delivery happens directly inside the API request. No background workers exist yet.

    Impact: If the app crashes during delivery, the request fails and no automatic recovery exists.