Webhook Delivery Platform

A production-inspired webhook delivery system built with FastAPI, PostgreSQL, Redis Streams, and asynchronous worker processes.

The platform accepts events, persists them durably, delivers them asynchronously, retries failures with exponential backoff, and provides operational visibility through metrics, health checks, structured logging, and tracing.

Delivery Guarantee: At-Least-Once Delivery

Architecture
Client
   ↓
API Key Authentication
   ↓
Tenant Resolution
   ↓
Rate Limiting
   ↓
FastAPI API Layer
   ↓
PostgreSQL (Source of Truth)
   ↓
Redis Streams
   ↓
Consumer Group Workers
   ↓
HMAC Signed Webhook Delivery
   ↓
Destination Endpoint
Core Features
Event Processing
Multi-tenant architecture
API key authentication
Endpoint registration
Event creation API
Idempotency support
Event persistence
Delivery System
Redis Streams
Redis Consumer Groups
Background worker processing
Delivery attempt tracking
Exponential backoff retries
Dead-letter handling
Security
Hashed API keys
Tenant isolation
HMAC-SHA256 webhook signing
Redis-backed rate limiting
Observability
Structured JSON logging
Distributed trace IDs
Health endpoints
Metrics endpoint
Worker heartbeats
Queue monitoring
Event Lifecycle
Create Event
      ↓
Persist to PostgreSQL
      ↓
Publish to Redis Stream
      ↓
Worker Consumes Event
      ↓
Webhook Delivery
      ↓
Success → ACK

Failure Path:

Delivery Failure
      ↓
Persist Attempt
      ↓
Schedule Retry
      ↓
Requeue Event
      ↓
Retry Delivery
      ↓
Maximum Attempts Reached
      ↓
Mark Event Dead
Retry Policy
Attempt 1 → 2s
Attempt 2 → 4s
Attempt 3 → 8s
Attempt 4 → 16s
Attempt 5 → Dead

Jitter is applied to reduce retry synchronization.

Security Model
API Authentication
Tenant-scoped API keys
SHA256 key hashing
Revocation support
Expiration support
Tenant Isolation

Tenants cannot:

Access another tenant's endpoints
Create events for another tenant
View another tenant's delivery history
Webhook Signing

Outgoing webhooks include:

X-Webhook-Signature

Generated using:

HMAC-SHA256

Allowing receivers to verify:

Authenticity
Integrity
Origin
Observability
Health Endpoints
GET /health

Simple liveness probe.

GET /health/details

Checks:

PostgreSQL
Redis
Worker heartbeat
Retry scheduler heartbeat

Possible states:

healthy
degraded
unhealthy
Metrics
GET /metrics/summary

Tracks:

events_created
events_delivered
delivery_attempts
events_failed
events_dead
pending_messages
avg_api_latency_ms
avg_queue_delay_ms
max_queue_delay_ms
API Endpoints
POST /tenants

POST /tenants/{tenant_id}/api-keys

POST /endpoints

POST /events

GET /events/{event_id}/delivery_attempts

GET /health

GET /health/details

GET /metrics/summary
Performance Evolution
Phase 3 Baseline

Environment:

PostgreSQL
Redis
Authentication
Tenant isolation
Rate limiting

Load:

1000 requests
10 concurrent clients
Metric	Result
p50	83.93 ms
p95	141.06 ms
p99	203.94 ms
Requests/sec	109.05
Phase 4 Baseline

Environment:

Redis Streams
Async Workers
Retry Scheduler
HMAC Signing
Tracing
Metrics
Health Checks

Load:

1000 requests
10 concurrent clients
Metric	Result
p50	83.97 ms
p95	130.35 ms
p99	187.96 ms
Requests/sec	110.88

Results:

100% delivery success
No queue backlog
No retries required
Observability overhead negligible
Worker Scaling Analysis
Workers	Avg Queue Delay
2	25.96 s
4	9.08 s
6	10.47 ms
8	11.57 ms
Findings
Worker throughput was the initial bottleneck.
Queue delay collapsed after scaling to 6 workers.
Additional workers showed diminishing returns.
Resource contention appeared beyond 6 workers.

For this workload:

6–8 workers ≈ optimal operating range
Performance Optimization Work

Implemented:

API key caching (TTLCache)
Endpoint caching (TTLCache)
Redis pipeline rate limiting
Multi-process API deployment
Connection pool tuning
Trace ID index removal
PostgreSQL lock analysis
Wait-event analysis
High-concurrency load testing
Latest Load Test Result

Configuration:

4 API Processes
8 Worker Processes

API Key Cache Enabled
Endpoint Cache Enabled

PostgreSQL
Redis Streams

Load:

10,000 Requests
500 Concurrent Clients

Results:

Metric	Result
p50	608.16 ms
p95	2099.73 ms
p99	2742.31 ms
Requests/sec	519.10
Success Rate	100%

Status Codes:

200 → 10000
500 → 0
Connection Failures → 0

Operational Metrics:

Metric	Result
Avg API Latency	790.32 ms
Avg Insert Stage	526.41 ms
Avg Redis Enqueue	30.17 ms
Avg Queue Delay	3042.64 ms
Max Queue Delay	4908 ms
Events Created	10000
Events Delivered	10000
Pending Messages	0
Engineering Findings
Authentication Bottleneck Eliminated

API key caching reduced database authentication lookups to near zero.

Endpoint Lookup Eliminated

Endpoint ownership validation became effectively free through caching.

PostgreSQL Not Saturated

Investigation included:

pg_stat_activity
wait events
lock analysis
transaction analysis
connection pool tuning

No significant database lock contention was observed.

Remaining Bottleneck

Under very high concurrency, latency increases disproportionately despite healthy database metrics.

Areas identified for future investigation:

asyncpg internals
connection lifecycle behavior
uvicorn worker scheduling
Linux networking limits
Redis connection behavior under burst traffic
Known Limitations
At-Least-Once delivery may produce duplicates
Polling-based retry scheduler
No replay tooling
Metrics stored in Redis instead of Prometheus
Application-level tracing instead of OpenTelemetry
Local infrastructure load testing only
Technology Stack
FastAPI
PostgreSQL
SQLAlchemy
AsyncPG
Redis Streams
AsyncIO
Alembic
Uvicorn
Deployment

API:

uvicorn app.main:app --workers 4

Workers:

python worker_process.py worker-1
...
python worker_process.py worker-8

This version reads like a backend systems project report and clearly shows the progression from ~110 RPS to ~520 RPS while documenting the architectural decisions and investigation work.