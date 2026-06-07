package com.github.diafter.jobqueue.messaging;

import com.github.diafter.jobqueue.config.JobQueueProperties;
import com.github.diafter.jobqueue.persistence.JobStore;
import com.github.diafter.jobqueue.persistence.OutboxRecord;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Background publisher that drains due rows from the transactional outbox and sends
 * them to Kafka.
 *
 * <p>Rows are first claimed in PostgreSQL with {@code FOR UPDATE SKIP LOCKED}; this
 * lets many application instances publish concurrently without double-publishing the
 * same outbox row.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final Clock clock;
    private final JobQueueProperties properties;
    private final JobStore jobStore;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Publishes a batch of due outbox rows on a fixed schedule.
     */
    @Scheduled(fixedDelayString = "${app.job-queue.outbox.fixed-delay-ms:1000}")
    public void publishDueRows() {
        if (!properties.getOutbox().isEnabled() || !running.compareAndSet(false, true)) {
            return;
        }
        final Instant now = Instant.now(clock);
        jobStore.claimDueOutbox(now, properties.getOutbox().getBatchSize())
                .flatMap(this::publishOne, properties.getOutbox().getPublishConcurrency())
                .doFinally(signalType -> running.set(false))
                .subscribe(
                        ignored -> { },
                        error -> log.error("Outbox publisher batch failed", error));
    }

    private Mono<Void> publishOne(final OutboxRecord record) {
        return Mono.fromFuture(kafkaTemplate.send(record.topic(), record.messageKey(), record.payloadJson()))
                .flatMap(sendResult -> jobStore.markOutboxPublished(record.id(), Instant.now(clock)))
                .doOnSuccess(ignored -> meterRegistry.counter("jobqueue.outbox.published", "topic", record.topic()).increment())
                .doOnError(error -> meterRegistry.counter("jobqueue.outbox.publish.failed", "topic", record.topic()).increment())
                .onErrorResume(error -> jobStore.markOutboxFailed(record.id(), safeMessage(error), Instant.now(clock)))
                .then();
    }

    private String safeMessage(final Throwable error) {
        final String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return error.getClass().getSimpleName();
        }
        return message.length() > 1_000 ? message.substring(0, 1_000) : message;
    }
}
