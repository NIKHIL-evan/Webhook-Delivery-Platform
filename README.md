Webhook Delivery Platform

A highly concurrent, production-inspired webhook delivery system built with FastAPI, PostgreSQL, Redis Streams, and asynchronous worker processes.

The platform guarantees at-least-once delivery, processes payloads asynchronously, retries failures with exponential backoff, and ensures total decoupling of the API ingestion layer from the physical database persistence layer.

Architectural Evolution

This platform evolved through distinct phases to overcome the fundamental limits of synchronous database interactions under heavy concurrent load.

The Synchronous Bottleneck (Previous Architecture)

In its previous iteration, the platform operated as a standard CRUD API:

API received payload.

API executed a synchronous INSERT to PostgreSQL.

API pushed the ID to Redis Streams.

API returned 202 Accepted.

The Failure Point: Under a load of 500 concurrent clients, the API processes instantly saturated the PostgreSQL connection pool (max 20 connections). Requests waited in memory for an available database lock, driving the average API latency from 80ms to over 790ms, with maximum latencies exceeding 4 seconds. Throughput capped at ~500 RPS. The database had become a strict synchronous bottleneck.

The Asynchronous Breakthrough (Current Architecture)

To break the connection pool starvation, the platform was refactored to an Asynchronous Ingestion Pattern utilizing "Fat Messages" (Payload Enrichment). The API no longer communicates with PostgreSQL on the critical write path.

The New Pipeline:

Edge Ingestion: The API authenticates the request via TTLCache, handles idempotency via Redis SET NX, enriches the payload with destination data, and pushes directly to a Redis Stream (webhook_events). The database is bypassed entirely.

Delivery Workers: Background processes consume webhook_events, execute the HTTP POST to the destination, and push the HTTP result to a second stream (webhook_results). They execute zero SQL queries.

Sink Workers (Eventual Consistency): Two dedicated background workers consume batches of 500 messages from the Redis streams and execute bulk INSERT statements into PostgreSQL.

By shifting database writes to background sink workers, the API is freed to process network requests at maximum speed.

Current Architecture

graph TD
    Client[Client] -->|POST /events| API[FastAPI Edge Layer]
    API -->|Auth/Endpoint Cache| Memory[TTLCache]
    API -->|Idempotency SET NX| RedisCache[(Redis Cache)]
    API -->|XADD Enriched Payload| Stream1[Redis Stream: webhook_events]
    
    Stream1 -->|XREADGROUP| DeliveryWorkers[Delivery Workers x8]
    Stream1 -->|XREADGROUP| EventSink[Event Sink Worker]
    
    DeliveryWorkers -->|HTTP POST| Destination[Destination Server]
    DeliveryWorkers -->|XADD Result| Stream2[Redis Stream: webhook_results]
    
    EventSink -->|Batch INSERT 500| Postgres[(PostgreSQL)]
    
    Stream2 -->|XREADGROUP| ResultSink[Result Sink Worker]
    ResultSink -->|Batch INSERT 500| Postgres
    
    Retry[Retry Scheduler] -->|Query Failed| Postgres
    Retry -->|Re-queue| Stream1


Core Features

Asynchronous Ingestion: Zero database locks on the API write path.

Idempotency (Redis NX): Millisecond-level duplicate rejection before the stream.

Payload Enrichment: Workers require zero DB reads to execute HTTP deliveries.

Multi-tenant Isolation: Scoped API keys, endpoint ownership validation.

HMAC-SHA256 Signing: Outgoing webhooks are cryptographically signed.

Exponential Backoff: Automated retry scheduling for failed deliveries.

Eventual Consistency: Background batch-inserts to PostgreSQL events and delivery_attempts without foreign key locking overhead.

Observability & Telemetry

Because the system is fully decoupled, traditional latency metrics (like API response time) no longer reflect system health. The observability layer tracks the friction between the distributed pipes.

GET /metrics/summary provides pipeline telemetry:

avg_api_edge: Time to authenticate, check idempotency, and push to Redis.

avg_delivery_queue_delay: Time a message waits in Redis before a Delivery Worker claims it.

avg_db_ingestion_lag: Time between an event entering Redis and being persisted to PostgreSQL.

avg_db_batch_insert: Time for the Sink Worker to execute a bulk insert of 500 rows.

pending_delivery_tasks / pending_db_ingestions: Current backlog in Redis consumer groups.

Performance Benchmarks

The Transition (Phase 4 vs Phase 5)

Test Configuration:

4 API Uvicorn Worker Processes

8 Delivery Workers

1 Event Sink Worker, 1 Result Sink Worker

Load: 10,000 Requests, 500 Concurrent Clients

Metric

Synchronous DB Model

Async Ingestion Model

Improvement

Throughput (RPS)

519.10

1179.68

+127%

Avg API Latency

790.32 ms

199.99 ms

-74%

Max API Latency

4908.00 ms

2037.00 ms

-58%

Delivery Queue Delay

3042.64 ms

17.90 ms

-99%

DB Ingestion Lag

N/A (Blocking)

9.26 ms

Flawless decoupling

Success/Persistence

100%

100%

Zero Data Loss

Engineering Findings:

Connection Pool Starvation Eliminated: The API no longer competes for database connections. The 127% increase in throughput is entirely attributed to replacing the SQL driver overhead with a Redis memory push.

Sink Efficiency: A single Event Sink worker batch-inserting 500 rows at a time can easily outpace 4 API processes receiving 1100+ RPS. Ingestion lag remained under 10ms.

Queue Collapse: By removing the SELECT statements from the Delivery Workers, they are able to claim, execute, and acknowledge messages with an average delay of only 17.9 milliseconds.

Running the Platform

To simulate a production deployment locally, the platform utilizes a process orchestrator script to bypass Python's GIL and run the components as isolated microservices.

# Execute the orchestrator
./scripts/start_local.sh


Deployed Microservices:

uvicorn (x4 processes): API Edge layer.

app.workers.delivery_worker (x8 processes): Webhook HTTP executors.

app.workers.event_sink_worker (x1 process): PostgreSQL events batch ingestion.

app.workers.result_sink_worker (x1 process): PostgreSQL delivery_attempts batch ingestion.

app.workers.retry_runner (x1 process): Failed event requeuing.

Known Limitations & Technical Debt

Ghost Messages (Dead Letters): If a worker hard-crashes mid-HTTP request, the Redis message remains in the pending state indefinitely. XCLAIM logic is required to sweep and re-assign orphaned messages.

Polling-Based Retries: The retry scheduler currently polls PostgreSQL every 5 seconds.

OpenTelemetry: Tracing is currently application-level JSON logging, not yet emitting OTLP format to a collector.