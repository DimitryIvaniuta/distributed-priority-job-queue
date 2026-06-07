# Distributed Priority Job Queue

**GitHub repository name:** `distributed-priority-job-queue`  
**Description:** Production-grade Java 25 / Spring Boot 4 distributed job queue with Kafka priority topics, retry backoff topics, DLQ, R2DBC/PostgreSQL persistence, Redis distributed leases, idempotency keys, transactional outbox, Flyway migrations, tests, Docker Compose, and Postman collection.

## Research-based stack choice

- **Java 25 + Spring Boot 4.0.6**: modern production baseline for 2026. Spring Boot 4.0.6 supports Java 17 through Java 26 and Gradle 8.14+/9.x.
- **Gradle 9.5.1**: current Gradle 9 line with Java 25/26 era support.
- **WebFlux + R2DBC**: non-blocking API and PostgreSQL access for high-throughput job ingestion.
- **PostgreSQL + Flyway**: durable source of truth for job state, idempotency, attempts, outbox, and side-effect receipts.
- **Kafka KRaft**: queue transport with independent high/low priority lanes, retry lanes, and DLQ.
- **Redis**: short-lived distributed leases for multi-instance workers.
- **Micrometer + Actuator + Prometheus**: operational visibility for queues, retries, DLQ, and publisher errors.

## Implemented improvements over the initial skeleton

- Added the full Java source tree; the previous archive had only build/config/migration files.
- Added API, service, persistence, messaging, config, domain, exception, and utility layers.
- Added explicit transactional outbox to close the database-write/Kafka-publish consistency gap.
- Added idempotency conflict detection using deterministic request hashes.
- Added Redis worker leases to reduce duplicate concurrent processing across instances.
- Added side-effect receipts with a unique business effect key to prevent duplicate side effects under Kafka redelivery.
- Added retry policy with backoff topics and DLQ transition.
- Added DLQ replay endpoint for safe operator recovery.
- Added job attempt audit table.
- Added Micrometer counters and Prometheus registry.
- Added Dockerfile, GitHub Actions CI, Postman collection, and unit tests.

## Architecture

```text
Client
  -> WebFlux API
  -> PostgreSQL jobs + job_outbox in one reactive transaction
  -> OutboxPublisher claims due rows with FOR UPDATE SKIP LOCKED
  -> Kafka jobs-high / jobs-low / retry topics / DLQ
  -> Multi-instance workers
  -> Redis lease + PostgreSQL side-effect receipt
  -> SUCCEEDED | RETRY_SCHEDULED | DLQ
```

## Topics

| Purpose | Topic |
| --- | --- |
| High-priority queue | `jobs-high` |
| Low-priority queue | `jobs-low` |
| Retry 1 | `jobs-retry-5s` |
| Retry 2 | `jobs-retry-30s` |
| Retry 3+ | `jobs-retry-2m` |
| Dead letter queue | `jobs-dlq` |

## Correctness model

Kafka gives **at-least-once delivery**, not exactly-once side effects. This project achieves no duplicate business side effects using application-level controls:

1. **Create request idempotency**: `jobs.idempotency_key` is unique.
2. **Idempotency conflict detection**: the same key with different request payload/type/priority returns HTTP `409`.
3. **Transactional outbox**: the API commits job state and Kafka-publish intent in the same PostgreSQL transaction.
4. **Manual Kafka ACK**: a message is acknowledged only after success, retry scheduling, DLQ scheduling, or safe terminal skip.
5. **Redis lease**: reduces concurrent processing of the same job across worker instances.
6. **Side-effect receipt**: `side_effect_receipts.effect_key` is unique, so re-delivery cannot create the same business effect twice.
7. **DLQ**: exhausted or permanent failures are observable and replayable.

## Run locally

```bash
docker compose up -d
./gradlew bootRun
```

Then open:

```text
http://localhost:8080/actuator/health
http://localhost:8080/actuator/prometheus
```

## Test

```bash
./gradlew clean test jacocoTestReport
```

## API examples

### Create a high-priority banking job

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotencyKey": "transfer-2026-0001",
    "type": "BANK_TRANSFER_SETTLEMENT",
    "priority": "HIGH",
    "payload": {
      "from": "PL61109010140000071219812874",
      "to": "PL27109010140000071219812875",
      "amount": "1250.00",
      "currency": "PLN"
    },
    "maxAttempts": 4
  }'
```

### Create a retryable failure simulation

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotencyKey": "retry-demo-0001",
    "type": "RETRYABLE_DEMO",
    "priority": "LOW",
    "payload": {"simulateRetryableFailure": true},
    "maxAttempts": 3
  }'
```

### Create a permanent failure simulation

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotencyKey": "permanent-demo-0001",
    "type": "PERMANENT_DEMO",
    "priority": "LOW",
    "payload": {"simulatePermanentFailure": true},
    "maxAttempts": 4
  }'
```

### Get job

```bash
curl http://localhost:8080/api/v1/jobs/{jobId}
```

### Get job by idempotency key

```bash
curl http://localhost:8080/api/v1/jobs/idempotency/transfer-2026-0001
```

### Replay DLQ job

```bash
curl -X POST http://localhost:8080/api/v1/jobs/{jobId}/replay \
  -H 'Content-Type: application/json' \
  -d '{"priority":"HIGH"}'
```

### Stats

```bash
curl http://localhost:8080/api/v1/jobs/stats
```

## Postman

Import:

```text
postman/distributed-job-queue.postman_collection.json
```

## Production notes

- Use Kafka topic replication factor `3` and `min.insync.replicas=2` outside local development.
- Use TLS/SASL for Kafka and TLS/passwords/ACLs for Redis and PostgreSQL.
- Add retention policies for `job_attempts`, `job_outbox`, and DLQ topics.
- Keep side-effect receipts for the full idempotency window required by the business domain.
- Split workers into separate deployments when high-priority traffic requires dedicated autoscaling.
- Use dashboards for `jobqueue.worker.retry.scheduled`, `jobqueue.worker.dlq`, and `jobqueue.outbox.publish.failed`.

## Interview talking points

- Priority is handled by separate topics, not fragile in-memory ordering.
- At-least-once delivery is accepted; duplicate side effects are prevented with idempotency and receipts.
- Transactional outbox avoids losing jobs when Kafka is temporarily unavailable.
- Retry topics keep hot queues clean and DLQ provides a controlled failure lane.
- Redis leases improve multi-instance safety but correctness does not depend only on Redis; PostgreSQL uniqueness is the final guard.
