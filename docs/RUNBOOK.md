# Operations Runbook

## Health checks

- `/actuator/health/readiness` must be `UP` before routing traffic.
- `/actuator/health/liveness` must be `UP` for container liveness.

## Important metrics

- `jobqueue_jobs_created_total`
- `jobqueue_worker_completed_total`
- `jobqueue_worker_retry_scheduled_total`
- `jobqueue_worker_dlq_total`
- `jobqueue_outbox_published_total`
- `jobqueue_outbox_publish_failed_total`

## DLQ handling

1. Inspect the job via `GET /api/v1/jobs/{id}`.
2. Fix bad payloads or downstream dependencies.
3. Replay with `POST /api/v1/jobs/{id}/replay`.
4. If the original side effect was already created, replay remains safe because the receipt key is unique.

## Scaling

- Scale high-priority workers separately when latency matters.
- Increase Kafka partitions before increasing listener concurrency beyond partition count.
- Keep PostgreSQL connection pool size below database limits across all replicas.
