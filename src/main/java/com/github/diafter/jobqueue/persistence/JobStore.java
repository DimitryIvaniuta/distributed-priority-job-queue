package com.github.diafter.jobqueue.persistence;

import com.github.diafter.jobqueue.domain.JobStatus;
import com.github.diafter.jobqueue.domain.OutboxStatus;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive PostgreSQL data access layer for jobs, outbox events, attempts, and
 * side-effect receipts.
 *
 * <p>The store intentionally uses SQL for critical state transitions because job
 * queue correctness depends on explicit, reviewable update semantics.</p>
 */
@Repository
@RequiredArgsConstructor
public class JobStore {

    private final DatabaseClient databaseClient;

    /**
     * Inserts a job if its idempotency key was not used before.
     *
     * @param job normalized new job command.
     * @return true when inserted, false when the key already exists.
     */
    public Mono<Boolean> insertJobIfAbsent(final NewJob job) {
        return databaseClient.sql("""
                        INSERT INTO jobs (
                            id, idempotency_key, request_hash, type, priority, status,
                            payload_json, trace_id, attempt, max_attempts, created_at, updated_at
                        ) VALUES (
                            :id, :idempotencyKey, :requestHash, :type, :priority, :status,
                            :payloadJson, :traceId, 0, :maxAttempts, :createdAt, :updatedAt
                        )
                        ON CONFLICT (idempotency_key) DO NOTHING
                        RETURNING id
                        """)
                .bind("id", job.id())
                .bind("idempotencyKey", job.idempotencyKey())
                .bind("requestHash", job.requestHash())
                .bind("type", job.type())
                .bind("priority", job.priority())
                .bind("status", JobStatus.QUEUED.name())
                .bind("payloadJson", job.payloadJson())
                .bind("traceId", job.traceId())
                .bind("maxAttempts", job.maxAttempts())
                .bind("createdAt", toOffset(job.now()))
                .bind("updatedAt", toOffset(job.now()))
                .fetch()
                .first()
                .hasElement();
    }

    /**
     * Finds a job by id.
     *
     * @param id job id.
     * @return job record when present.
     */
    public Mono<JobRecord> findJobById(final UUID id) {
        return databaseClient.sql("SELECT * FROM jobs WHERE id = :id")
                .bind("id", id)
                .map((row, metadata) -> mapJob((name, type) -> row.get(name, type)))
                .one();
    }

    /**
     * Finds a job by idempotency key.
     *
     * @param idempotencyKey idempotency key.
     * @return job record when present.
     */
    public Mono<JobRecord> findJobByIdempotencyKey(final String idempotencyKey) {
        return databaseClient.sql("SELECT * FROM jobs WHERE idempotency_key = :idempotencyKey")
                .bind("idempotencyKey", idempotencyKey)
                .map((row, metadata) -> mapJob((name, type) -> row.get(name, type)))
                .one();
    }

    /**
     * Inserts a transactional outbox event.
     *
     * @param event outbox event command.
     * @return completion signal.
     */
    public Mono<Void> insertOutboxEvent(final NewOutboxEvent event) {
        return databaseClient.sql("""
                        INSERT INTO job_outbox (
                            id, job_id, topic, message_key, payload_json, headers_json,
                            status, attempts, not_before, created_at, updated_at
                        ) VALUES (
                            :id, :jobId, :topic, :messageKey, :payloadJson, :headersJson,
                            :status, 0, :notBefore, :createdAt, :updatedAt
                        )
                        """)
                .bind("id", event.id())
                .bind("jobId", event.jobId())
                .bind("topic", event.topic())
                .bind("messageKey", event.messageKey())
                .bind("payloadJson", event.payloadJson())
                .bind("headersJson", event.headersJson())
                .bind("status", OutboxStatus.PENDING.name())
                .bind("notBefore", toOffset(event.notBefore()))
                .bind("createdAt", toOffset(event.now()))
                .bind("updatedAt", toOffset(event.now()))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Claims due outbox rows for publication.
     *
     * @param now current timestamp.
     * @param limit batch limit.
     * @return claimed rows.
     */
    public Flux<OutboxRecord> claimDueOutbox(final Instant now, final int limit) {
        return databaseClient.sql("""
                        WITH claimed AS (
                            SELECT id
                            FROM job_outbox
                            WHERE status IN ('PENDING', 'FAILED') AND not_before <= :now
                            ORDER BY created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE job_outbox outbox
                        SET status = 'PUBLISHING', attempts = attempts + 1, updated_at = :now
                        FROM claimed
                        WHERE outbox.id = claimed.id
                        RETURNING outbox.*
                        """)
                .bind("now", toOffset(now))
                .bind("limit", limit)
                .map((row, metadata) -> new OutboxRecord(
                        row.get("id", UUID.class),
                        row.get("job_id", UUID.class),
                        row.get("topic", String.class),
                        row.get("message_key", String.class),
                        row.get("payload_json", String.class),
                        row.get("headers_json", String.class),
                        row.get("attempts", Integer.class),
                        instant((name, type) -> row.get(name, type), "not_before"),
                        instant((name, type) -> row.get(name, type), "created_at")))
                .all();
    }

    /**
     * Marks an outbox row as published.
     *
     * @param id outbox row id.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> markOutboxPublished(final UUID id, final Instant now) {
        return databaseClient.sql("""
                        UPDATE job_outbox
                        SET status = 'PUBLISHED', published_at = :now, updated_at = :now, last_error = NULL
                        WHERE id = :id
                        """)
                .bind("id", id)
                .bind("now", toOffset(now))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Marks an outbox row as failed so the scheduler can retry publication.
     *
     * @param id outbox row id.
     * @param error safe error message.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> markOutboxFailed(final UUID id, final String error, final Instant now) {
        return databaseClient.sql("""
                        UPDATE job_outbox
                        SET status = 'FAILED', last_error = :error, updated_at = :now
                        WHERE id = :id
                        """)
                .bind("id", id)
                .bind("error", error)
                .bind("now", toOffset(now))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Marks a job as processing and records worker lease metadata.
     *
     * @param jobId job id.
     * @param attempt attempt number.
     * @param workerId worker instance id.
     * @param now timestamp.
     * @param leasedUntil lease expiry timestamp.
     * @return completion signal.
     */
    public Mono<Void> markProcessing(
            final UUID jobId,
            final int attempt,
            final String workerId,
            final Instant now,
            final Instant leasedUntil) {
        return databaseClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            attempt = :attempt,
                            locked_at = :now,
                            leased_until = :leasedUntil,
                            worker_id = :workerId,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId AND status <> 'SUCCEEDED'
                        """)
                .bind("jobId", jobId)
                .bind("status", JobStatus.PROCESSING.name())
                .bind("attempt", attempt)
                .bind("workerId", workerId)
                .bind("now", toOffset(now))
                .bind("leasedUntil", toOffset(leasedUntil))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Records an attempt start in the audit table.
     *
     * @param jobId job id.
     * @param attempt attempt number.
     * @param topic Kafka topic consumed.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> insertAttemptStarted(final UUID jobId, final int attempt, final String topic, final Instant now) {
        return databaseClient.sql("""
                        INSERT INTO job_attempts (id, job_id, attempt, status, topic, started_at)
                        VALUES (:id, :jobId, :attempt, :status, :topic, :startedAt)
                        ON CONFLICT (job_id, attempt) DO NOTHING
                        """)
                .bind("id", UUID.randomUUID())
                .bind("jobId", jobId)
                .bind("attempt", attempt)
                .bind("status", JobStatus.PROCESSING.name())
                .bind("topic", topic)
                .bind("startedAt", toOffset(now))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Finishes an attempt audit row.
     *
     * @param jobId job id.
     * @param attempt attempt number.
     * @param status final attempt status.
     * @param error error message.
     * @param failureClass exception class name.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> finishAttempt(
            final UUID jobId,
            final int attempt,
            final String status,
            final String error,
            final String failureClass,
            final Instant now) {
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        UPDATE job_attempts
                        SET status = :status,
                            error_message = :error,
                            failure_class = :failureClass,
                            finished_at = :finishedAt
                        WHERE job_id = :jobId AND attempt = :attempt
                        """)
                .bind("jobId", jobId)
                .bind("attempt", attempt)
                .bind("status", status)
                .bind("finishedAt", toOffset(now));
        spec = bindNullable(spec, "error", error, String.class);
        spec = bindNullable(spec, "failureClass", failureClass, String.class);
        return spec.fetch().rowsUpdated().then();
    }

    /**
     * Inserts a side-effect receipt using a unique effect key.
     *
     * @param effectKey business effect key.
     * @param jobId job id.
     * @param jobType job type.
     * @param payloadHash SHA-256 payload hash.
     * @param now timestamp.
     * @return true when this call created the receipt, false when it already existed.
     */
    public Mono<Boolean> insertSideEffectReceipt(
            final String effectKey,
            final UUID jobId,
            final String jobType,
            final String payloadHash,
            final Instant now) {
        return databaseClient.sql("""
                        INSERT INTO side_effect_receipts (id, effect_key, job_id, job_type, payload_hash, created_at)
                        VALUES (:id, :effectKey, :jobId, :jobType, :payloadHash, :createdAt)
                        ON CONFLICT (effect_key) DO NOTHING
                        RETURNING id
                        """)
                .bind("id", UUID.randomUUID())
                .bind("effectKey", effectKey)
                .bind("jobId", jobId)
                .bind("jobType", jobType)
                .bind("payloadHash", payloadHash)
                .bind("createdAt", toOffset(now))
                .fetch()
                .first()
                .hasElement();
    }

    /**
     * Marks a job as successfully completed.
     *
     * @param jobId job id.
     * @param resultJson result JSON.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> markSucceeded(final UUID jobId, final String resultJson, final Instant now) {
        return databaseClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            result_json = :resultJson,
                            error_message = NULL,
                            failure_class = NULL,
                            next_retry_at = NULL,
                            leased_until = NULL,
                            worker_id = NULL,
                            completed_at = :now,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId
                        """)
                .bind("jobId", jobId)
                .bind("status", JobStatus.SUCCEEDED.name())
                .bind("resultJson", resultJson)
                .bind("now", toOffset(now))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Marks a job as retry scheduled.
     *
     * @param jobId job id.
     * @param error error message.
     * @param failureClass exception class name.
     * @param nextRetryAt next retry due timestamp.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> markRetryScheduled(
            final UUID jobId,
            final String error,
            final String failureClass,
            final Instant nextRetryAt,
            final Instant now) {
        return databaseClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            error_message = :error,
                            failure_class = :failureClass,
                            next_retry_at = :nextRetryAt,
                            leased_until = NULL,
                            worker_id = NULL,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId
                        """)
                .bind("jobId", jobId)
                .bind("status", JobStatus.RETRY_SCHEDULED.name())
                .bind("error", error)
                .bind("failureClass", failureClass)
                .bind("nextRetryAt", toOffset(nextRetryAt))
                .bind("now", toOffset(now))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Marks a job as sent to DLQ.
     *
     * @param jobId job id.
     * @param error error message.
     * @param failureClass exception class name.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> markDlq(
            final UUID jobId,
            final String error,
            final String failureClass,
            final Instant now) {
        return databaseClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            error_message = :error,
                            failure_class = :failureClass,
                            next_retry_at = NULL,
                            leased_until = NULL,
                            worker_id = NULL,
                            completed_at = :now,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId
                        """)
                .bind("jobId", jobId)
                .bind("status", JobStatus.DLQ.name())
                .bind("error", error)
                .bind("failureClass", failureClass)
                .bind("now", toOffset(now))
                .fetch()
                .rowsUpdated()
                .then();
    }

    /**
     * Requeues a DLQ job for operator replay.
     *
     * @param jobId job id.
     * @param priority target priority.
     * @param now timestamp.
     * @return completion signal.
     */
    public Mono<Void> requeueDlqJob(final UUID jobId, final String priority, final Instant now) {
        return databaseClient.sql("""
                        UPDATE jobs
                        SET status = :status,
                            priority = :priority,
                            next_retry_at = NULL,
                            leased_until = NULL,
                            worker_id = NULL,
                            updated_at = :now,
                            version = version + 1
                        WHERE id = :jobId AND status = 'DLQ'
                        """)
                .bind("jobId", jobId)
                .bind("status", JobStatus.QUEUED.name())
                .bind("priority", priority)
                .bind("now", toOffset(now))
                .fetch()
                .rowsUpdated()
                .filter(rows -> rows > 0)
                .switchIfEmpty(Mono.<Long>error(new IllegalStateException("Job is not in DLQ state")))
                .then();
    }

    /**
     * Counts jobs by status.
     *
     * @return map of status to count.
     */
    public Mono<Map<String, Long>> countJobsByStatus() {
        return databaseClient.sql("SELECT status, COUNT(*) AS total FROM jobs GROUP BY status")
                .map((row, metadata) -> Map.entry(
                        row.get("status", String.class),
                        row.get("total", Long.class)))
                .all()
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    private JobRecord mapJob(final RowGetter getter) {
        return new JobRecord(
                value(getter, "id", UUID.class),
                value(getter, "idempotency_key", String.class),
                value(getter, "request_hash", String.class),
                value(getter, "type", String.class),
                value(getter, "priority", String.class),
                value(getter, "status", String.class),
                value(getter, "payload_json", String.class),
                value(getter, "result_json", String.class),
                value(getter, "error_message", String.class),
                value(getter, "failure_class", String.class),
                value(getter, "trace_id", String.class),
                value(getter, "attempt", Integer.class),
                value(getter, "max_attempts", Integer.class),
                instant(getter, "next_retry_at"),
                instant(getter, "created_at"),
                instant(getter, "updated_at"),
                instant(getter, "completed_at"));
    }

    private DatabaseClient.GenericExecuteSpec bindNullable(
            final DatabaseClient.GenericExecuteSpec spec,
            final String name,
            final Object value,
            final Class<?> type) {
        return value == null ? spec.bindNull(name, type) : spec.bind(name, value);
    }

    private OffsetDateTime toOffset(final Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private Instant instant(final RowGetter getter, final String name) {
        final OffsetDateTime value = getter.get(name, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private <T> T value(final RowGetter getter, final String name, final Class<T> type) {
        return getter.get(name, type);
    }

    @FunctionalInterface
    private interface RowGetter {
        /**
         * Reads a typed column value from a database row.
         *
         * @param name column name.
         * @param type target type.
         * @param <T> target type parameter.
         * @return column value.
         */
        <T> T get(String name, Class<T> type);
    }
}
