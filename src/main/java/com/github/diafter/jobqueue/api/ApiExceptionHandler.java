package com.github.diafter.jobqueue.api;

import com.github.diafter.jobqueue.exception.IdempotencyConflictException;
import com.github.diafter.jobqueue.exception.InvalidJobStateException;
import com.github.diafter.jobqueue.exception.JobNotFoundException;
import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebInputException;
import org.springframework.web.bind.support.WebExchangeBindException;

/**
 * Centralized API exception mapping that returns stable error codes.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    /**
     * Handles missing jobs.
     *
     * @param exception exception.
     * @return 404 response.
     */
    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(final JobNotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, "JOB_NOT_FOUND", exception.getMessage());
    }

    /**
     * Handles idempotency conflicts.
     *
     * @param exception exception.
     * @return 409 response.
     */
    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ErrorResponse> handleIdempotencyConflict(final IdempotencyConflictException exception) {
        return error(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT", exception.getMessage());
    }

    /**
     * Handles invalid state transitions.
     *
     * @param exception exception.
     * @return 409 response.
     */
    @ExceptionHandler(InvalidJobStateException.class)
    public ResponseEntity<ErrorResponse> handleInvalidState(final InvalidJobStateException exception) {
        return error(HttpStatus.CONFLICT, "INVALID_JOB_STATE", exception.getMessage());
    }

    /**
     * Handles WebFlux input binding errors.
     *
     * @param exception exception.
     * @return 400 response.
     */
    @ExceptionHandler({ServerWebInputException.class, MethodArgumentNotValidException.class, WebExchangeBindException.class})
    public ResponseEntity<ErrorResponse> handleBadRequest(final Exception exception) {
        return error(HttpStatus.BAD_REQUEST, "BAD_REQUEST", "Request body or parameters are invalid");
    }

    private ResponseEntity<ErrorResponse> error(final HttpStatus status, final String code, final String message) {
        return ResponseEntity.status(status).body(new ErrorResponse(code, message, Instant.now()));
    }
}
