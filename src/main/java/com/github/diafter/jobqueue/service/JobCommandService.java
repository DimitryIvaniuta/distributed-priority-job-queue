package com.github.diafter.jobqueue.service;

import com.github.diafter.jobqueue.api.CreateJobRequest;
import com.github.diafter.jobqueue.api.CreateJobResult;
import com.github.diafter.jobqueue.api.JobResponse;
import com.github.diafter.jobqueue.config.JobQueueProperties;
import com.github.diafter.jobqueue.domain.JobPriority;
import com.github.diafter.jobqueue.exception.IdempotencyConflictException;
import com.github.diafter.jobqueue.exception.InvalidJobStateException;
import com.github.diafter.jobqueue.exception.JobNotFoundException;
import com.github.diafter.jobqueue.messaging.JobQueueMessage;
import com.github.diafter.jobqueue.persistence.JobRecord;
import com.github.diafter.jobqueue.persistence.JobStore;
import com.github.diafter.jobqueue.persistence.NewJob;
import com.github.diafter.jobqueue.persistence.NewOutboxEvent;
import com.github.diafter.jobqueue.util.HashSupport;
import com.github.diafter.jobqueue.util.JsonSupport;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/**
 * Application service for API-level job commands.
 */
@Service
@RequiredArgsConstructor
public class JobCommandService {

    private final Clock clock;
    private final HashSupport hashSupport;
    private final JobQueueProperties properties;
    private final JobStore jobStore;
    private final JsonSupport jsonSupport;
    private final MeterRegistry meterRegistry;
    private final TransactionalOperator transactionalOperator;

    /**
     * Creates a new job or returns the existing job for an idempotent duplicate request.
     *
     * @param request create-job request.
     * @return create result containing job response and created flag.
     */
    public Mono<CreateJobResult> create(final CreateJobRequest request) {
        final Instant now = Instant.now(clock);
        final String idempotencyKey = normalize(request.idempotencyKey());
        final String type = normalize(request.type()).toUpperCase();
        final String payloadJson = jsonSupport.canonical(request.payload());
        final String requestHash = requestHash(idempotencyKey, type, request.priority(), payloadJson);
        final int maxAttempts = request.maxAttempts() == null
                ? properties.getRetry().getDefaultMaxAttempts()
                : request.maxAttempts();
        final UUID jobId = UUID.randomUUID();
        final String traceId = UUID.randomUUID().toString();
        final NewJob job = new NewJob(
                jobId,
                idempotencyKey,
                requestHash,
                type,
                request.priority().name(),
                payloadJson,
                maxAttempts,
                traceId,
                now);
        final String topic = topicForPriority(request.priority());
        final JobQueueMessage message = new JobQueueMessage(jobId, idempotencyKey, 1, now, traceId);
        final NewOutboxEvent outbox = outbox(jobId, idempotencyKey, topic, jsonSupport.write(message), now);

        return transactionalOperator.transactional(
                jobStore.insertJobIfAbsent(job)
                        .flatMap(inserted -> inserted
                                ? jobStore.insertOutboxEvent(outbox)
                                        .then(jobStore.findJobById(jobId))
                                        .map(JobResponse::from)
                                        .map(response -> new CreateJobResult(response, true))
                                : jobStore.findJobByIdempotencyKey(idempotencyKey)
                                        .flatMap(existing -> ensureSameRequest(existing, requestHash))
                                        .map(JobResponse::from)
                                        .map(response -> new CreateJobResult(response, false))))
                .doOnNext(result -> meterRegistry.counter(
                        "jobqueue.jobs.created", "created", Boolean.toString(result.created())).increment());
    }

    /**
     * Returns a job by id.
     *
     * @param jobId job id.
     * @return job response.
     */
    public Mono<JobResponse> get(final UUID jobId) {
        return jobStore.findJobById(jobId)
                .switchIfEmpty(Mono.error(new JobNotFoundException(jobId)))
                .map(JobResponse::from);
    }

    /**
     * Returns a job by idempotency key.
     *
     * @param idempotencyKey idempotency key.
     * @return job response.
     */
    public Mono<JobResponse> getByIdempotencyKey(final String idempotencyKey) {
        return jobStore.findJobByIdempotencyKey(normalize(idempotencyKey))
                .switchIfEmpty(Mono.error(new InvalidJobStateException("Job not found for idempotency key")))
                .map(JobResponse::from);
    }

    /**
     * Replays a DLQ job by adding a new outbox event while preserving idempotency.
     *
     * @param jobId job id.
     * @param priority replay priority.
     * @return updated job response.
     */
    public Mono<JobResponse> replayDlq(final UUID jobId, final JobPriority priority) {
        final Instant now = Instant.now(clock);
        return transactionalOperator.transactional(
                jobStore.findJobById(jobId)
                        .switchIfEmpty(Mono.error(new JobNotFoundException(jobId)))
                        .flatMap(job -> {
                            if (!"DLQ".equals(job.status())) {
                                return Mono.error(new InvalidJobStateException("Only DLQ jobs can be replayed"));
                            }
                            final JobQueueMessage message = new JobQueueMessage(
                                    job.id(), job.idempotencyKey(), job.attempt() + 1, now, job.traceId());
                            final NewOutboxEvent event = outbox(
                                    job.id(),
                                    job.idempotencyKey(),
                                    topicForPriority(priority),
                                    jsonSupport.write(message),
                                    now);
                            return jobStore.requeueDlqJob(job.id(), priority.name(), now)
                                    .then(jobStore.insertOutboxEvent(event))
                                    .then(jobStore.findJobById(job.id()));
                        })
                        .map(JobResponse::from));
    }

    /**
     * Returns current job counters grouped by status.
     *
     * @return map of status to count.
     */
    public Mono<Map<String, Long>> stats() {
        return jobStore.countJobsByStatus();
    }

    private Mono<JobRecord> ensureSameRequest(final JobRecord existing, final String requestHash) {
        if (!existing.requestHash().equals(requestHash)) {
            return Mono.error(new IdempotencyConflictException(existing.idempotencyKey()));
        }
        return Mono.just(existing);
    }

    private NewOutboxEvent outbox(
            final UUID jobId,
            final String idempotencyKey,
            final String topic,
            final String payloadJson,
            final Instant dueAt) {
        return new NewOutboxEvent(
                UUID.randomUUID(),
                jobId,
                topic,
                idempotencyKey,
                payloadJson,
                "{}",
                dueAt,
                Instant.now(clock));
    }

    private String topicForPriority(final JobPriority priority) {
        return priority == JobPriority.HIGH ? properties.getTopics().getHigh() : properties.getTopics().getLow();
    }

    private String requestHash(
            final String idempotencyKey,
            final String type,
            final JobPriority priority,
            final String payloadJson) {
        return hashSupport.sha256(jsonSupport.write(Map.of(
                "idempotencyKey", idempotencyKey,
                "type", type,
                "priority", priority.name(),
                "payload", payloadJson)));
    }

    private String normalize(final String value) {
        return value == null ? "" : value.trim();
    }
}
