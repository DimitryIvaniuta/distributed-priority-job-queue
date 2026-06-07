package com.github.diafter.jobqueue.service;

import java.time.Duration;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Redis-backed distributed lock helper used to prevent concurrent processing of
 * the same job by multiple worker instances.
 */
@Service
@RequiredArgsConstructor
public class DistributedLockService {

    private final ReactiveStringRedisTemplate redisTemplate;

    /**
     * Runs work only when a Redis lock is acquired.
     *
     * @param lockKey lock key.
     * @param ttl lock time to live.
     * @param work protected work.
     * @return true when work ran, false when another worker already owned the lock.
     */
    public Mono<Boolean> withLock(final String lockKey, final Duration ttl, final Mono<Void> work) {
        final String token = UUID.randomUUID().toString();
        return redisTemplate.opsForValue().setIfAbsent(lockKey, token, ttl)
                .flatMap(acquired -> {
                    if (!Boolean.TRUE.equals(acquired)) {
                        return Mono.just(false);
                    }
                    return work.thenReturn(true)
                            .flatMap(result -> release(lockKey, token).thenReturn(result))
                            .onErrorResume(error -> release(lockKey, token).then(Mono.<Boolean>error(error)));
                });
    }

    private Mono<Void> release(final String lockKey, final String token) {
        return redisTemplate.opsForValue().get(lockKey)
                .flatMap(value -> token.equals(value) ? redisTemplate.delete(lockKey).then() : Mono.empty())
                .then();
    }
}
