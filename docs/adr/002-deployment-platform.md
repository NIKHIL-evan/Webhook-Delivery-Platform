# ADR 002: Deployment Platform

## Context

The project needed a deployment platform capable of running a FastAPI application with a PostgreSQL database in a production-like environment. The goal for Phase 0 was to move beyond local development and verify that the webhook delivery flow worked correctly over the public internet. The platform also needed to support environment variables, automatic deployments from GitHub, and a free tier suitable for development and testing.

## Decision

Render was selected as the deployment platform for Phase 0. It provided a simple deployment workflow for Python applications, integrated PostgreSQL hosting, GitHub-based deployments, and environment variable management without requiring additional infrastructure setup. The deployment process allowed the application to run publicly and successfully deliver real webhook events to external services.

## Alternatives Considered

Fly.io was considered because of its strong developer experience and container-based deployment model. However, account verification required a credit card and triggered a high-risk verification flow with a temporary verification charge, making it less convenient for this phase.

Railway was also considered because of its simple deployment experience. However, the free usage limits were more restrictive and would require payment after the initial free period, making it less suitable for longer-term experimentation during development.

## Trade-offs Accepted

The Render free tier spins down after periods of inactivity, which introduces cold starts when the application receives new traffic. In practice, startup delays can take several seconds and occasionally up to around 50 seconds depending on service state.

Another trade-off is that the current deployment is a single synchronous FastAPI service without background workers or queue infrastructure. Webhook delivery happens during the API request itself, so slow destination servers directly affect API response time. More scalable asynchronous delivery architecture will be introduced in later phases.
