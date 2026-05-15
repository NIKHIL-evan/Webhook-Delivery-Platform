# ADR 004 Retry Strategy 
# content 
Phase 2 introduces retry handling for failed webhook deliveries: , which requires:
- exponential backofff with jitter
- retry attempts tracking
- delayed retry scheduling
- dead letter queue support

Redis stream and worker group already handles 
- asynchronous delivery
- acknowledgement
- pending entry list
- worker coordination

# Decision 
To track and retry failed attempts we need to record them. The PEL is just Redis tracking "this message was handed out but not yet acknowledged.", so to record the retry we considered two options:

Option A: Track retry state in the message itself.
When a delivery fails, instead of leaving the message in the PEL, you acknowledge it and write a new message into the stream with the next retry time and attempt number included in the message body. The worker reads it, checks if now >= retry_at, and if not, skips it temporarily.

Option B: Track retry state in PostgreSQL.
Your events table already has a status column. Add a next_retry_at and attempt_count column. When delivery fails, update those columns. Your worker queries Postgres for events that are due for retry.

Option A has a flaw - we want delays with jitter in the retry otherwise there will be retry storm for the worker, so lets say we want a 5 sec delay and we included that in the stream message and now the worker has that message - it has only two options either do the task or acknowledge it neither is what we want because there is a delay, worker cant just filter out messages xreadgroup() doesn't allow that it only give worker new tasks. So, Option A rejected

Option B is perfect we just add two new columns in the Events table which will track attempt number and Retry delay (with exponential backoff with jitter logic) which we easily filter out. Now for this we need another loop running asynchrounously with the worker process whcih only handles failed task:
Retry scheduler. Runs on a timer, say every 5 seconds. Queries Postgres for events where status = 'failed' and next_retry_at <= now and attempt_count < max_attempts. For each one, writes a new message into the Redis Stream and updates status back to pending 

# Trade-offs Accepted
This adds tighter coupling between Redis and PostgreSQL.
Previously the responsibilities were more separated:

Redis handled:
- queueing
- message ownership
- pending state

PostgreSQL handled:
- durable business data

After introducing retry scheduling, PostgreSQL now also stores retry state such as `attempt_count`and `next_retry_at`
This means the retry system now depends on PostgreSQL queries to determine when events should be retried.

Trade-offs introduced:
- additional database queries
- more retry-related state management
- tighter dependency between worker logic and database schema

However, this design provides:
- durable retry state
- reliable delayed retry scheduling
- simpler worker logic
- easier observability and debugging of failed events

Redis Streams remain focused on asynchronous delivery coordination while PostgreSQL becomes the source of truth for retry scheduling.
