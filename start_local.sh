#!/bin/bash

echo "Starting Webhook Delivery Platform using uv..."

# 1. Start the API with 4 worker processes
echo "Starting API (4 workers)..."
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4 &

# 2. Start the 8 Delivery Workers
echo "Starting 8 Delivery Workers..."
for i in {1..8}
do
   uv run python -m app.workers.delivery_worker "delivery-$i" &
done

# 3. Start the Event Sink Worker (PostgreSQL Ingestion)
echo "Starting Event Sink Worker..."
uv run python -m app.workers.event_sink_worker &

# 4. Start the Results Sink Worker (Delivery Attempt Ingestion)
echo "Starting Results Sink Worker..."
uv run python -m app.workers.result_sink_worker &

# 5. Start the Retry Scheduler
echo "Starting Retry Scheduler..."
uv run python -m app.workers.retry_runner &

echo "All processes started. Press Ctrl+C to stop all."

wait