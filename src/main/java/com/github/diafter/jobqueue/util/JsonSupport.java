package com.github.diafter.jobqueue.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UncheckedIOException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Centralized JSON serialization helper.
 */
@Component
@RequiredArgsConstructor
public class JsonSupport {

    private final ObjectMapper objectMapper;

    /**
     * Serializes a value to compact JSON.
     *
     * @param value value to serialize.
     * @return serialized JSON string.
     */
    public String write(final Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    /**
     * Reads JSON as a typed value.
     *
     * @param json serialized JSON.
     * @param type target type.
     * @param <T> target type parameter.
     * @return parsed value.
     */
    public <T> T read(final String json, final Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    /**
     * Converts a JSON node into canonical compact JSON.
     *
     * @param node JSON node.
     * @return canonical JSON string.
     */
    public String canonical(final JsonNode node) {
        return write(node);
    }
}
