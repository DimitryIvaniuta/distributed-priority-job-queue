# Architecture Notes

## Why not rely on Kafka exactly-once semantics only?

Kafka's producer and transaction features can protect Kafka-to-Kafka flows, but most business side effects touch databases, payment providers, emails, ledgers, object storage, or external APIs. Those systems are outside the Kafka transaction. Therefore this service treats Kafka delivery as at-least-once and makes side effects idempotent.

## Retry design

The worker never sleeps on the hot topic. A failed job is persisted as `RETRY_SCHEDULED`, and a retry outbox row is inserted with `not_before`. The outbox publisher only publishes due rows to the configured retry topic. This gives the operational shape of retry topics without blocking consumer threads.

## Duplicate prevention layers

1. Unique `jobs.idempotency_key` prevents duplicate job creation.
2. `request_hash` detects incorrect key reuse.
3. Redis leases reduce concurrent processing.
4. Unique `side_effect_receipts.effect_key` prevents duplicate side effects.
5. Terminal states are checked before execution, so redelivery after success becomes a no-op.
