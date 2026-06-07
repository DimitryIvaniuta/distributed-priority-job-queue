package com.github.diafter.jobqueue.api;

import java.util.Map;

/**
 * Aggregated job counts grouped by status.
 *
 * @param countsByStatus map where key is a durable job status and value is the number of jobs.
 */
public record JobStatsResponse(Map<String, Long> countsByStatus) {
}
