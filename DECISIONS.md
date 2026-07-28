# Architecture & Design Decisions

This document explains the *why* behind the implementation, not just the *what*.
It is meant to make the review (and a follow-up discussion) easy.

## 1. Transport: Redis Streams + Consumer Groups

**Decision:** publish payout events with `XADD` to a stream and consume them with
a Consumer Group (`XREADGROUP`).

**Why:**
- A **Consumer Group** guarantees that each message is delivered to exactly one
  consumer in the group — this directly satisfies the "multiple instances, each
  event processed once" requirement. Adding instances scales throughput linearly.
- Every unacknowledged message stays in the consumer's **Pending Entries List
  (PEL)**, so nothing is lost on a crash/restart — the requirement for reliability.
- Each consumer gets a unique id (`consumer-<uuid>`) so instances don't collide.

**Alternatives considered:** Redis Pub/Sub (fire-and-forget, no persistence, no
redelivery — rejected); a full broker like Kafka/RabbitMQ (overkill for the scope,
and Redis is already provisioned).

## 2. Delivery semantics: at-least-once + idempotency = effectively-once

`XREADGROUP` gives **at-least-once** delivery. The dangerous window is: the DB
transaction commits, then the process dies *before* `XACK`. On restart the event
is redelivered and would be credited **twice** — unacceptable for money.

**Decision:** make processing idempotent at the database level.
- `payout_ledgers.event_id` (the Redis stream message id) has a **UNIQUE**
  constraint.
- The credit + ledger insert happen in one transaction. A redelivered event hits
  the unique violation, which the service catches and treats as a no-op
  (`'duplicate'`), then acks.

Net effect: **effectively-once** crediting on top of at-least-once transport.
See `payouts.service.ts` and the idempotency e2e test.

## 3. Atomicity: single DB transaction + pessimistic lock

Crediting the balance and writing the audit row must be all-or-nothing.

**Decision:** `em.transactional(...)` wraps both writes. The `User` row is loaded
with `PESSIMISTIC_WRITE` (`SELECT ... FOR UPDATE`) so concurrent consumers
crediting the same user serialize instead of racing on a read-modify-write of the
balance.

## 4. Money math in integer cents

Floating point can't represent `0.1 + 0.2` exactly, which corrupts balances.

**Decision:** all arithmetic is done in integer cents (`Math.round(x * 100)`),
converted back to a 2-dp number only at the boundary. Column type stays
`DECIMAL(10,2)`. Driver-returned decimals (strings) are normalized via `Number()`.

## 5. Failure handling: retry then dead-letter

A message that always fails (e.g. a deleted user) must not block the stream or
loop forever.

**Decision:**
- A **transient** failure is not acked → it stays in the PEL and is retried.
- After `PAYOUT_MAX_RETRIES` deliveries (tracked via `XPENDING` delivery count),
  the message is moved to a **dead-letter stream** (`payout-events-dlq`) with a
  reason, then acked — the main stream keeps flowing and nothing is silently lost.
- **Schema-invalid** ("poison") messages can never succeed, so they go straight
  to the DLQ.

## 6. Recovering dead instances: XAUTOCLAIM

If an instance dies permanently, its pending messages would otherwise sit unowned.

**Decision:** before each read, the consumer runs `XAUTOCLAIM` to take over
messages idle longer than `PAYOUT_CLAIM_IDLE_MS` from any consumer, so a dead
worker's backlog is picked up by a live one.

## 7. API returns 202 Accepted

The endpoint only publishes to the stream and returns immediately — the heavy
work is offloaded to the background consumer. This is the fast, responsive-API
pattern the challenge asks for. Input is validated with a single **zod** schema
that is reused for both the HTTP body and the stream payload (one source of truth).

## 8. Operability

- `GET /health` pings Redis and Postgres (used for readiness/liveness probes).
- A global exception filter returns a uniform JSON error envelope.
- Tuning knobs (`BLOCK`, batch size, retries, claim idle) are env-configurable.
- `enableShutdownHooks()` lets the consumer drain cleanly on shutdown.

## Known trade-offs / what I'd do next in production

- **Schema management:** the challenge seeds via `db/seed.sql`. A real service
  would use MikroORM migrations instead of `seed.sql` + a fresh volume.
- **DLQ tooling:** there's no automated DLQ re-drive UI; that'd be the next step.
- **Metrics/tracing:** I'd add Prometheus counters (processed/failed/dlq) and
  OpenTelemetry spans around the consume→persist path.
- **Outbox on the publish side:** if the publisher also wrote to its own DB, a
  transactional outbox would close the gap between DB write and stream publish.
