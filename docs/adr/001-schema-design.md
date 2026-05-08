Context

This system needs a database because it has to remember things even after the server restarts.

The system needs to store:

Webhook URLs registered by users
Events sent by users
Delivery attempts and retry history

For example, if a webhook delivery fails, the system should remember to retry later. Users should also be able to check whether their events were delivered successfully or failed.

Decision

I decided to use three tables.

1. endpoints

    Stores webhook URLs registered by users.

    url_id
    url
    created_at

2. events

    Stores incoming events.

    event_id
    url_id (foreign key)
    payload (jsonb)
    status
    created_at

3. delivery_attempts

    Stores every delivery try for an event.

    attempt_id
    event_id (foreign key)
    attempt_number
    status
    response_code
    attempted_at

The payload is stored using PostgreSQL jsonb because event data can have different JSON structures and jsonb data type is faster then json data type because it doest require reparsing for querying and processing.

The events table also stores a status column so the system can quickly find pending events without checking all delivery attempts every time.

Alternatives Considered

I first thought about storing everything in one table, but that would repeat the same webhook URL many times and make retry tracking messy.

I also considered keeping status only inside delivery_attempts, but then the system would need extra queries and joins just to find pending, delivered or failed events.

Trade-offs Accepted:

Using nested routes makes the API paths slightly longer, but they are more readable and self-explanatory.
Using REST conventions also means following standard HTTP methods and status codes, which improves consistency and developer experience.
The current schema and API design also assume that one event belongs to one endpoint. Because of this, the events table stores a single url_id, and delivery attempts are accessed using paths like:
                GET /events/{event_id}/delivery_attempts
This works for the current phase, where one event is delivered to one destination URL.
However, in a future fan-out scenario, one event may need to be delivered to multiple endpoints (for example, both a shipping system and a warehouse system). In that case, the current relationship model becomes limiting because a single url_id in the events table would no longer be enough.

Future phases may require:

a separate mapping table between events and endpoints
changes to the API path structure
or query-based endpoints instead of deeply nested routes
This complexity is intentionally postponed until fan-out support is actually needed, to keep Phase 0 simpler and easier to build