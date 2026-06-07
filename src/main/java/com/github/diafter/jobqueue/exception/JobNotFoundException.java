package com.github.diafter.jobqueue.exception;

import java.util.UUID;

/**
 * Raised when an operator or API caller references an unknown job id.
 */
public class JobNotFoundException extends RuntimeException {

    /**
     * Creates an exception for the missing job id.
     *
     * @param jobId missing job id.
     */
    public JobNotFoundException(final UUID jobId) {
        super("Job not found: " + jobId);
    }
}
