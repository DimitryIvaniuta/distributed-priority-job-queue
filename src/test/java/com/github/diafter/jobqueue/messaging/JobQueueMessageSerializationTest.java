package com.github.diafter.jobqueue.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for Kafka message JSON compatibility.
 */
class JobQueueMessageSerializationTest {

    /**
     * Verifies the job queue message can round-trip through JSON.
     *
     * @throws Exception when JSON processing fails.
     */
    @Test
    void jobQueueMessageRoundTripsThroughJson() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        final JobQueueMessage message = new JobQueueMessage(
                UUID.randomUUID(), "key-1", 2, Instant.parse("2026-06-06T07:30:00Z"), "trace-1");

        final String json = objectMapper.writeValueAsString(message);
        final JobQueueMessage restored = objectMapper.readValue(json, JobQueueMessage.class);

        assertThat(restored).isEqualTo(message);
    }
}
