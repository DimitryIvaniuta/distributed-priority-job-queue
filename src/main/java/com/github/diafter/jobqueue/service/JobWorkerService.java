package com.github.diafter.jobqueue.service;

import com.github.diafter.jobqueue.config.JobQueueProperties;
import com.github.diafter.jobqueue.domain.JobStatus;
import com.github.diafter.jobqueue.domain.RetryDecision;
import com.github.diafter.jobqueue.messaging.DlqMessage;
import com.github.diafter.jobqueue.messaging.JobQueueMessage;
import com.github.diafter.jobqueue.persistence.JobRecord;
import com.github.diafter.jobqueue.persistence.JobStore;
import com.github.diafter.jobqueue.persistence.NewOutboxEvent;
import com.github.diafter.jobqueue.util.JsonSupport;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/**
 * Worker orchestration service that processes Kafka job messages safely.
 *
 * <p>Kafka provides at-least-once delivery. This service provides exactly-once
 * business behavior by combining Redis leases, durable job state, and unique
 * side-effect receipts.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobWorkerService {

    private final Clock clock;
    private final DistributedLockService lockService;
    private final JobExecutor jobExecutor;
    private final JobQueueProperties properties;
    private final JobStore jobStore;
    private final JsonSupport jsonSupport;
    private final MeterRegistry meterRegistry;
    private final RetryPolicy retryPolicy;
    private final TransactionalOperator transactionalOperator;
    private final WorkerIdentity workerIdentity;

    /**
     * Processes one serialized Kafka message.
     *
     * @param rawMessage serialized {@link JobQueueMessage}.
     * @param topic Kafka topic from which the message was consumed.
     * @return completion signal after durable state transition.
     */
    public Mono<Void> process(final String rawMessage, final String topic) {
        return Mono.defer(() -> {
            final JobQueueMessage message = jsonSupport.read(rawMessage, JobQueueMessage.class);
            final String lockKey = "jobqueue:job-lock:" + message.jobId();
            final Mono<Void> work = processLocked(message, topic);
            return lockService.withLock(lockKey, properties.getWorker().getLockTtl(), work)
                    .doOnNext(acquired -> {
                        if (!acquired) {
                            meterRegistry.counter("jobqueue.worker.lock.skipped", "topic", topic).increment();
                            log.info("Job {} is already leased by another worker", message.jobId());
                        }
                    })
                    .then();
        });
    }

    private Mono<Void> processLocked(final JobQueueMessage message, final String topic) {
        final Instant now = Instant.now(clock);
        if (message.notBefore() != null && now.isBefore(message.notBefore())) {
            return rescheduleEarlyMessage(message, topic, message.notBefore());
        }
        return jobStore.findJobById(message.jobId())
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("Ignoring message for missing job {}", message.jobId());
                    return Mono.empty();
                }))
                .flatMap(job -> {
                    if (JobStatus.SUCCEEDED.name().equals(job.status()) || JobStatus.DLQ.name().equals(job.status())) {
                        meterRegistry.counter("jobqueue.worker.terminal.skipped", "status", job.status()).increment();
                        return Mono.empty();
                    }
                    return executeAttempt(job, message, topic);
                });
    }

    private Mono<Void> executeAttempt(final JobRecord job, final JobQueueMessage message, final String topic) {
        final Instant startedAt = Instant.now(clock);
        final Instant leaseUntil = startedAt.plus(properties.getWorker().getLockTtl());
        return jobStore.markProcessing(job.id(), message.attempt(), workerIdentity.value(), startedAt, leaseUntil)
                .then(jobStore.insertAttemptStarted(job.id(), message.attempt(), topic, startedAt))
                .then(jobExecutor.execute(job))
                .flatMap(result -> transactionalOperator.transactional(
                        jobStore.markSucceeded(job.id(), result.resultJson(), Instant.now(clock))
                                .then(jobStore.finishAttempt(
                                        job.id(),
                                        message.attempt(),
                                        JobStatus.SUCCEEDED.name(),
                                        null,
                                        null,
                                        Instant.now(clock)))))
                .doOnSuccess(ignored -> meterRegistry.counter("jobqueue.worker.completed", "topic", topic).increment())
                .onErrorResume(error -> handleFailure(job, message, topic, error));
    }

    private Mono<Void> handleFailure(
            final JobRecord job,
            final JobQueueMessage message,
            final String topic,
            final Throwable error) {
        final Instant now = Instant.now(clock);
        final String safeError = safeMessage(error);
        final String failureClass = error.getClass().getName();
        final RetryDecision decision = retryPolicy.decide(message.attempt(), job.maxAttempts(), error);
        if (decision.retry()) {
            final Instant retryAt = now.plus(decision.delay());
            final JobQueueMessage retryMessage = new JobQueueMessage(
                    job.id(), job.idempotencyKey(), decision.nextAttempt(), retryAt, job.traceId());
            final NewOutboxEvent retryOutbox = new NewOutboxEvent(
                    UUID.randomUUID(),
                    job.id(),
                    decision.topic(),
                    job.idempotencyKey(),
                    jsonSupport.write(retryMessage),
                    "{}",
                    retryAt,
                    now);
            return transactionalOperator.transactional(
                            jobStore.markRetryScheduled(job.id(), safeError, failureClass, retryAt, now)
                                    .then(jobStore.finishAttempt(
                                            job.id(),
                                            message.attempt(),
                                            JobStatus.RETRY_SCHEDULED.name(),
                                            safeError,
                                            failureClass,
                                            now))
                                    .then(jobStore.insertOutboxEvent(retryOutbox)))
                    .doOnSuccess(ignored -> meterRegistry.counter("jobqueue.worker.retry.scheduled", "topic", topic).increment());
        }

        final DlqMessage dlqMessage = new DlqMessage(
                job.id(), job.idempotencyKey(), message.attempt(), safeError, failureClass, job.traceId(), now);
        final NewOutboxEvent dlqOutbox = new NewOutboxEvent(
                UUID.randomUUID(),
                job.id(),
                properties.getTopics().getDlq(),
                job.idempotencyKey(),
                jsonSupport.write(dlqMessage),
                "{}",
                now,
                now);
        return transactionalOperator.transactional(
                        jobStore.markDlq(job.id(), safeError, failureClass, now)
                                .then(jobStore.finishAttempt(
                                        job.id(),
                                        message.attempt(),
                                        JobStatus.DLQ.name(),
                                        safeError,
                                        failureClass,
                                        now))
                                .then(jobStore.insertOutboxEvent(dlqOutbox)))
                .doOnSuccess(ignored -> meterRegistry.counter("jobqueue.worker.dlq", "topic", topic).increment());
    }

    private Mono<Void> rescheduleEarlyMessage(
            final JobQueueMessage message,
            final String topic,
            final Instant notBefore) {
        final Instant now = Instant.now(clock);
        final NewOutboxEvent event = new NewOutboxEvent(
                UUID.randomUUID(),
                message.jobId(),
                topic,
                message.idempotencyKey(),
                jsonSupport.write(message),
                "{}",
                notBefore,
                now);
        return jobStore.insertOutboxEvent(event)
                .doOnSuccess(ignored -> meterRegistry.counter("jobqueue.worker.early.rescheduled", "topic", topic).increment());
    }

    private String safeMessage(final Throwable error) {
        final String message = error.getMessage();
        final String value = message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
        return value.length() > 1_000 ? value.substring(0, 1_000) : value;
    }
}
