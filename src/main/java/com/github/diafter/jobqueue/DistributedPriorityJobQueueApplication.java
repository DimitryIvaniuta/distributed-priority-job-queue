package com.github.diafter.jobqueue;

import com.github.diafter.jobqueue.config.JobQueueProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Application entry point for the distributed priority job queue service.
 *
 * <p>The application exposes a reactive HTTP API, persists job state in PostgreSQL,
 * publishes queue messages through a transactional outbox, and consumes Kafka topics
 * with manual acknowledgements to preserve at-least-once delivery semantics.</p>
 */
@EnableKafka
@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties(JobQueueProperties.class)
public class DistributedPriorityJobQueueApplication {

    /**
     * Starts the Spring Boot application.
     *
     * @param args command-line arguments passed by the runtime.
     */
    public static void main(final String[] args) {
        SpringApplication.run(DistributedPriorityJobQueueApplication.class, args);
    }
}
