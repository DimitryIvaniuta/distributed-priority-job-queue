package com.github.diafter.jobqueue.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.diafter.jobqueue.domain.JobExecutionResult;
import com.github.diafter.jobqueue.exception.NonRetryableJobException;
import com.github.diafter.jobqueue.exception.RetryableJobException;
import com.github.diafter.jobqueue.persistence.JobRecord;
import com.github.diafter.jobqueue.persistence.JobStore;
import com.github.diafter.jobqueue.util.HashSupport;
import com.github.diafter.jobqueue.util.JsonSupport;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Demo-safe job executor that models a guarded banking side effect.
 *
 * <p>Real systems would delegate by job type to isolated handlers. This reference
 * implementation demonstrates the critical production pattern: create a unique
 * side-effect receipt before reporting success. If Kafka redelivers the message,
 * the receipt prevents duplicate business effects.</p>
 */
@Service
@RequiredArgsConstructor
public class JobExecutor {

    private final Clock clock;
    private final HashSupport hashSupport;
    private final JobStore jobStore;
    private final JsonSupport jsonSupport;

    /**
     * Executes the job and records an idempotent side-effect receipt.
     *
     * @param job persisted job.
     * @return execution result.
     */
    public Mono<JobExecutionResult> execute(final JobRecord job) {
        return Mono.defer(() -> {
            final JsonNode payload = jsonSupport.read(job.payloadJson(), JsonNode.class);
            validateFailureSimulation(payload);

            final String payloadHash = hashSupport.sha256(job.payloadJson());
            final String effectKey = job.type() + ":" + job.idempotencyKey();
            return jobStore.insertSideEffectReceipt(effectKey, job.id(), job.type(), payloadHash, Instant.now(clock))
                    .map(created -> {
                        final Map<String, Object> result = Map.of(
                                "jobId", job.id().toString(),
                                "type", job.type(),
                                "effectKey", effectKey,
                                "effectCreated", created,
                                "processedAt", Instant.now(clock).toString());
                        return new JobExecutionResult(jsonSupport.write(result), created);
                    });
        });
    }

    private void validateFailureSimulation(final JsonNode payload) {
        if (payload.path("simulatePermanentFailure").asBoolean(false)) {
            throw new NonRetryableJobException("Payload requested permanent failure simulation");
        }
        if (payload.path("simulateRetryableFailure").asBoolean(false)) {
            throw new RetryableJobException("Payload requested retryable failure simulation");
        }
    }
}
