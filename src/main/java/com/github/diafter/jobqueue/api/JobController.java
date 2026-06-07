package com.github.diafter.jobqueue.api;

import com.github.diafter.jobqueue.domain.JobPriority;
import com.github.diafter.jobqueue.service.JobCommandService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Reactive HTTP API for creating, reading, replaying, and monitoring jobs.
 */
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobCommandService jobCommandService;

    /**
     * Creates a job or returns the existing idempotent job.
     *
     * @param request create request.
     * @return 201 for a new job, 200 for an idempotent duplicate.
     */
    @PostMapping
    public Mono<ResponseEntity<JobResponse>> create(@Valid @RequestBody final CreateJobRequest request) {
        return jobCommandService.create(request)
                .map(result -> result.created()
                        ? ResponseEntity.created(URI.create("/api/v1/jobs/" + result.response().id())).body(result.response())
                        : ResponseEntity.ok(result.response()));
    }

    /**
     * Reads a job by id.
     *
     * @param id job id.
     * @return job response.
     */
    @GetMapping("/{id}")
    public Mono<JobResponse> get(@PathVariable final UUID id) {
        return jobCommandService.get(id);
    }

    /**
     * Reads a job by idempotency key.
     *
     * @param key idempotency key.
     * @return job response.
     */
    @GetMapping("/idempotency/{key}")
    public Mono<JobResponse> getByIdempotencyKey(@PathVariable final String key) {
        return jobCommandService.getByIdempotencyKey(key);
    }

    /**
     * Replays a job currently in DLQ.
     *
     * @param id job id.
     * @param request replay request.
     * @return updated job response.
     */
    @PostMapping("/{id}/replay")
    public Mono<JobResponse> replay(@PathVariable final UUID id, @Valid @RequestBody final ReplayJobRequest request) {
        return jobCommandService.replayDlq(id, request.priority() == null ? JobPriority.LOW : request.priority());
    }

    /**
     * Returns job counters grouped by status.
     *
     * @return stats response.
     */
    @GetMapping("/stats")
    public Mono<JobStatsResponse> stats() {
        return jobCommandService.stats().map(JobStatsResponse::new);
    }
}
