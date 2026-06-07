package com.github.diafter.jobqueue.messaging;

import com.github.diafter.jobqueue.config.JobQueueProperties;
import com.github.diafter.jobqueue.service.JobWorkerService;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Kafka listener adapter that bridges Kafka's imperative listener API to the
 * reactive worker service.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaJobListener {

    private static final String TOPIC_HEADER = "kafka_receivedTopic";

    private final JobQueueProperties properties;
    private final JobWorkerService workerService;

    /**
     * Consumes high-priority jobs.
     *
     * @param payload Kafka payload.
     * @param topic source topic.
     * @param acknowledgment manual acknowledgment handle.
     */
    @KafkaListener(
            topics = "${app.job-queue.topics.high}",
            containerFactory = "highPriorityKafkaListenerContainerFactory")
    public void onHighPriorityJob(
            final String payload,
            @Header(name = TOPIC_HEADER, required = false) final String topic,
            final Acknowledgment acknowledgment) {
        handle(payload, topic == null ? properties.getTopics().getHigh() : topic, acknowledgment);
    }

    /**
     * Consumes low-priority jobs.
     *
     * @param payload Kafka payload.
     * @param topic source topic.
     * @param acknowledgment manual acknowledgment handle.
     */
    @KafkaListener(
            topics = "${app.job-queue.topics.low}",
            containerFactory = "lowPriorityKafkaListenerContainerFactory")
    public void onLowPriorityJob(
            final String payload,
            @Header(name = TOPIC_HEADER, required = false) final String topic,
            final Acknowledgment acknowledgment) {
        handle(payload, topic == null ? properties.getTopics().getLow() : topic, acknowledgment);
    }

    /**
     * Consumes retry topic jobs.
     *
     * @param payload Kafka payload.
     * @param topic source topic.
     * @param acknowledgment manual acknowledgment handle.
     */
    @KafkaListener(
            topics = {
                    "${app.job-queue.topics.retry-5s}",
                    "${app.job-queue.topics.retry-30s}",
                    "${app.job-queue.topics.retry-2m}"
            },
            containerFactory = "retryKafkaListenerContainerFactory")
    public void onRetryJob(
            final String payload,
            @Header(name = TOPIC_HEADER, required = false) final String topic,
            final Acknowledgment acknowledgment) {
        handle(payload, topic == null ? properties.getTopics().getRetry2m() : topic, acknowledgment);
    }

    private void handle(final String payload, final String topic, final Acknowledgment acknowledgment) {
        try {
            workerService.process(payload, topic)
                    .block(properties.getWorker().getProcessingTimeout());
            acknowledgment.acknowledge();
        } catch (RuntimeException exception) {
            log.error("Worker failed before making a durable state transition; message will be redelivered", exception);
            acknowledgment.nack(Duration.ofSeconds(5));
        }
    }
}
