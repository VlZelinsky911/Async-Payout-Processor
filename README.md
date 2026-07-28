# Asynchronous Payout Processor (Nest.js + Redis Streams + PostgreSQL)

A microservice that credits user payouts **asynchronously**. The HTTP endpoint only
publishes an event to a **Redis Stream** and immediately answers `202 Accepted`, while all
the heavy lifting (amount calculation, balance update, audit-log write) happens in a
**background worker** that reads the stream through a **Consumer Group**.

Key properties of the implementation:

- **exactly-once effect** (in reality *at-least-once transport + idempotency*),
- **atomic** balance and audit update inside a single DB transaction with a pessimistic lock,
- **durability**: no event is ever lost on restart or worker crash,
- **DLQ** (dead-letter stream) for "poison" and chronically failing events,
- **horizontal scaling** of workers with no duplicate processing.

> The rationale behind the architectural decisions is also summarised in [DECISIONS.md](./DECISIONS.md).

---

## Table of contents

1. [What the challenge required and how it is covered](#1-what-the-challenge-required-and-how-it-is-covered)
2. [Bird's-eye architecture](#2-birds-eye-architecture)
3. [Full event processing flow (step by step)](#3-full-event-processing-flow-step-by-step)
4. [Deep dive into the key decisions](#4-deep-dive-into-the-key-decisions)
5. [Code structure](#5-code-structure)
6. [API reference](#6-api-reference)
7. [Data model](#7-data-model)
8. [Configuration (env variables)](#8-configuration-env-variables)
9. [How to run](#9-how-to-run)
10. [Testing](#10-testing)
11. [Scaling](#11-scaling)
12. [Error handling and edge cases](#12-error-handling-and-edge-cases)
13. [Possible interview questions (Q&A)](#13-possible-interview-questions-qa)
14. [What I would improve for production](#14-what-i-would-improve-for-production)
15. [Detailed file-by-file walkthrough](#15-detailed-file-by-file-walkthrough)

---

## 1. What the challenge required and how it is covered

| # | Requirement | Implementation | File |
|---|-------------|----------------|------|
| 1 | **Publisher endpoint** that puts an event (`userId`, `offerId`, `basePayout`) into a Redis Stream | `POST /payouts` validates the body with zod and performs `XADD` into the `payout-events` stream | [payouts.controller.ts](src/payouts/payouts.controller.ts), [payouts.publisher.ts](src/payouts/payouts.publisher.ts) |
| 2 | **Background worker**; with several instances each event is processed by exactly one | Redis **Consumer Group** `payout-workers` + a unique `consumer-<uuid>` per instance | [payouts.consumer.ts](src/payouts/payouts.consumer.ts) |
| 3 | **Atomic** final payout calculation (user multiplier) + balance update + audit transaction record | A single `em.transactional(...)` with `SELECT … FOR UPDATE` and an `INSERT` into `payout_ledgers` | [payouts.service.ts](src/payouts/payouts.service.ts) |
| 4 | **Durability** (nothing is lost on restart) + unit and e2e tests | Unacked messages stay in the PEL and get redelivered; `XAUTOCLAIM` picks up the work of "dead" workers | [payouts.service.spec.ts](src/payouts/payouts.service.spec.ts), [payouts.e2e-spec.ts](test/payouts.e2e-spec.ts) |
| + | **No double crediting** | `payout_ledgers.event_id` is `UNIQUE` → idempotency (effectively-once) | [seed.sql](db/seed.sql), [payouts.service.ts](src/payouts/payouts.service.ts) |
| + | **Poison / chronically failing events** | Retry limit → **dead-letter stream** `payout-events-dlq` | [payouts.consumer.ts](src/payouts/payouts.consumer.ts) |
| + | **Money correctness** | All arithmetic done in integer cents | [payouts.service.ts](src/payouts/payouts.service.ts) |

---

## 2. Bird's-eye architecture

```
        HTTP (fast 202 response)                       Background processing (decoupled)
 ┌─────────────────────────────────┐        ┌────────────────────────────────────────────┐
 │                                 │        │                                            │
 │  POST /payouts                  │        │   PayoutsConsumer  (1 per instance)        │
 │        │                        │        │        │  loop():                          │
 │        ▼                        │        │        │   1. XAUTOCLAIM (pick up "dead")   │
 │  PayoutsController              │        │        │   2. XREADGROUP (new + own PEL)    │
 │   (zod validation)              │        │        ▼                                   │
 │        │                        │        │   PayoutsService.processPayout()           │
 │        ▼                        │        │        │  one DB transaction:              │
 │  PayoutsPublisher.publish()     │        │        │   • SELECT … FOR UPDATE user      │
 │        │  XADD                  │        │        │   • balance += base × multiplier  │
 │        ▼                        │        │        │   • INSERT payout_ledgers         │
 │   ┌──────────────────┐          │        │        │        (event_id UNIQUE)          │
 │   │  Redis Stream    │◄─────────┼────────┼── reads│                                   │
 │   │  payout-events   │          │        │        ▼                                   │
 │   └──────────────────┘          │        │   XACK  ─── or ───►  payout-events-dlq     │
 │                                 │        │                      (after N retries)     │
 └─────────────────────────────────┘        └────────────────────────────────────────────┘
                                                          │
                                                          ▼
                                                    PostgreSQL
                                              users / payout_ledgers
```

**The core idea:** publisher and consumer are decoupled through the Redis Stream. The
endpoint does not wait for the business logic to finish — it responds right after `XADD`.
This is exactly the "fast, responsive API" pattern with the work offloaded to the background.

### Components

| Component | Role |
|-----------|------|
| `PayoutsController` | Accepts `POST /payouts`, validates the body, returns `202` |
| `PayoutsPublisher` | A wrapper around `XADD` — publishes the event into the stream |
| `PayoutsConsumer` | Background worker: `XAUTOCLAIM` → `XREADGROUP` → processing → `XACK`/DLQ. Started in `onModuleInit`, stopped in `onModuleDestroy` |
| `PayoutsService` | Pure business logic: transaction, lock, calculation, idempotency |
| `HealthController` | `GET /health` — pings Redis and Postgres for liveness/readiness |
| `AllExceptionsFilter` | A single JSON envelope for every error |
| `RedisModule` | DI provider for the `ioredis` client |

---

## 3. Full event processing flow (step by step)

**Publishing (the synchronous part):**

1. The client sends `POST /payouts` with the body `{ userId, offerId, basePayout }`.
2. `PayoutsController` parses the body via `payoutEventSchema.safeParse`. An invalid body
   → `400` with the list of zod issues.
3. `PayoutsPublisher.publish()` executes `XADD payout-events * userId … offerId … basePayout …`.
   Redis returns the **message id** (e.g. `1719839239-0`), which becomes the future `event_id`.
4. The controller returns `202 Accepted` `{ status: 'accepted', id }`. **The HTTP request ends here.**

**Processing (the asynchronous part, in the background):**

5. `PayoutsConsumer.onModuleInit()`:
   - `ensureGroup()` — `XGROUP CREATE payout-events payout-workers $ MKSTREAM`
     (ignores the `BUSYGROUP` error if the group already exists; `MKSTREAM` creates the
     stream if it does not exist yet);
   - duplicates the Redis connection (`this.redis.duplicate()`) — a **separate socket for
     the blocking `XREADGROUP`**, so the main client is never blocked;
   - starts the infinite `loop()`.
6. On every `loop()` iteration:
   - **`claimStale()`** → `XAUTOCLAIM` takes over messages that have been idle for longer
     than `PAYOUT_CLAIM_IDLE_MS` in *any* consumer (i.e. in a dead instance) and processes
     them immediately;
   - **`XREADGROUP GROUP payout-workers <consumerId> COUNT <batch> BLOCK <ms> STREAMS payout-events <cursor>`**.
     Cursor `0` reads the **own PEL** (already delivered to this consumer but not acked);
     once the PEL is empty it switches to `>` (new messages not yet delivered to anyone).
7. For every message `handle(id, fields)` is called (wrapped in `@CreateRequestContext()` —
   a MikroORM context per processed message):
   - `parse(fields)` folds the flat array `[k, v, k, v, …]` into an object and validates it with zod;
   - if **invalid** → straight to the DLQ (`invalid-schema`) and `XACK` (retrying makes no sense);
   - if valid → `PayoutsService.processPayout(id, event)`.
8. `processPayout()` inside **a single transaction**:
   - `findOne(User, …, { lockMode: PESSIMISTIC_WRITE })` → `SELECT … FOR UPDATE`;
   - no such user → `NotFoundException` (the transaction is rolled back);
   - `amount = round(basePayout × multiplier × 100) / 100`;
   - `user.balance += amount` (arithmetic in cents);
   - `em.create(PayoutLedger, { user, eventId, offerId, payoutAmount, createdAt })`.
   - Returns `'processed'`. If the `INSERT` fails on `UNIQUE(event_id)` →
     `UniqueConstraintViolationException` is caught and `'duplicate'` is returned.
9. Back in the consumer:
   - success (`processed`/`duplicate`) → `XACK payout-events payout-workers <id>` (removes it from the PEL);
   - exception → `handleFailure()`: reads the delivery count via `XPENDING`
     (`deliveryCount`). If `>= PAYOUT_MAX_RETRIES` → DLQ + `XACK`; otherwise **no ack**
     → the message stays in the PEL and will be redelivered on the next iteration.

---

## 4. Deep dive into the key decisions

### 4.1. Transport: Redis Streams + Consumer Groups

**Why Streams and not Pub/Sub?** Pub/Sub is fire-and-forget: if no subscriber is present at
publish time the message is lost; there is neither persistence nor redelivery. Streams store
messages in an append-only log and provide an acknowledgement (ACK) mechanism — exactly what
"no event is ever lost" requires.

**Why a Consumer Group and not plain `XREAD`?** A Consumer Group guarantees that every
message is handed to **exactly one** consumer within the group. That directly satisfies the
"several instances, each event processed by one" requirement. Adding instances scales throughput.

**Why not Kafka/RabbitMQ?** Overkill for the scale of this task, and Redis is already in the stack.

### 4.2. Delivery semantics: at-least-once + idempotency = effectively-once

`XREADGROUP` provides **at-least-once**. The dangerous window: the DB transaction is
committed but the process crashes **before** `XACK`. After a restart the event is redelivered
and would be credited **twice** — unacceptable for money.

**The solution — idempotency at the DB level:** `payout_ledgers.event_id` (= the stream
message id) is `UNIQUE`. The balance credit and the ledger insert happen in one transaction.
A redelivered event hits the `UNIQUE` violation, the service catches it as a no-op
(`'duplicate'`) and calls `XACK`. Net result: **effectively-once** on top of an
at-least-once transport.

### 4.3. Atomicity: one transaction + a pessimistic lock

The balance credit and the audit record must be all-or-nothing — otherwise you can end up
with a balance that has no trace in the ledger (or the other way around).

`em.transactional(...)` wraps both writes. The user is loaded with `PESSIMISTIC_WRITE` →
`SELECT … FOR UPDATE`. Without it, two consumers crediting the same user concurrently would
produce a classic **lost update** (a read-modify-write race on the balance). The lock
serialises them.

### 4.4. Money in integer cents

`0.1 + 0.2 !== 0.3` in doubles — the accumulated error corrupts balances.

All arithmetic goes through integer cents: `Math.round(x * 100)`, converted back to a
2-decimal number only at the boundary. The driver returns `DECIMAL` as a **string**, so
everything is normalised via `Number()` before the computation (see the test
"handles string decimals returned by the driver").

### 4.5. Failure handling: retry, then dead-letter

A message that always fails (e.g. a deleted user) must neither block the stream nor spin forever.

- **Transient** failure → not acked → stays in the PEL → retried.
- After `PAYOUT_MAX_RETRIES` deliveries (the counter comes from `XPENDING`) → moved to the
  **dead-letter stream** `payout-events-dlq` with a `reason` field, then `XACK` — the main
  stream keeps flowing and nothing is lost silently.
- **Schema-invalid** ("poison") messages will never pass → straight to the DLQ.

### 4.6. Recovering dead instances: `XAUTOCLAIM`

If an instance dies permanently, its pending messages would be left orphaned in its PEL.
Before every read the consumer runs `XAUTOCLAIM` and takes over messages idle for longer
than `PAYOUT_CLAIM_IDLE_MS` in any consumer.

> **A subtlety worth mentioning in an interview:** `consumerId` is regenerated
> (`consumer-<uuid>`) on every process start. Therefore after a restart cursor `0` reads the
> PEL of the **new** id (empty), and the stuck work of the old id is picked up precisely by
> `XAUTOCLAIM`. Cursor `0` saves you within the lifetime of a single process;
> cross-restart recovery rests on `XAUTOCLAIM`.

### 4.7. Resilience of the loop itself

- `NOGROUP` (the group/stream was dropped externally) → `ensureGroup()` and the cursor resets to `0`.
- Any other loop error → exponential **backoff** (500ms → … → 10s cap), so the CPU is not
  burned in a tight loop while Redis is unavailable.
- `onModuleDestroy` sets `running = false` and closes the blocking socket — thanks to
  `enableShutdownHooks()` the worker drains cleanly on shutdown.

### 4.8. One zod schema — one source of truth

`payoutEventSchema` is used **both** for the HTTP body in the controller **and** for parsing
the stream payload in the consumer. One schema means no validation drift between the entry
point and the processing. `z.coerce` is appropriate here because everything arrives from the
stream as strings.

---

## 5. Code structure

```
src/
├── main.ts                        # bootstrap; global filter; shutdown hooks; WORKER_ONLY mode
├── app.module.ts                  # root module: Config, MikroORM, Redis, Common, Payouts
├── mikro-orm.config.ts            # Postgres driver config (host/port/creds from env)
│
├── common/
│   ├── common.module.ts
│   ├── health.controller.ts       # GET /health — pings Redis + Postgres
│   └── http-exception.filter.ts   # single JSON error envelope
│
├── redis/
│   └── redis.module.ts            # DI provider for ioredis (REDIS_CLIENT)
│
├── users/
│   └── user.entity.ts             # User entity (balance, multiplier)
│
└── payouts/
    ├── payouts.module.ts
    ├── payouts.constants.ts       # stream / group / DLQ names
    ├── payouts.schema.ts          # zod event schema (single source of truth)
    ├── payouts.controller.ts      # POST /payouts → publisher
    ├── payouts.publisher.ts       # XADD into the stream
    ├── payouts.consumer.ts        # background worker (Consumer Group, DLQ, XAUTOCLAIM)
    ├── payouts.service.ts         # transaction + lock + calculation + idempotency
    ├── payout-ledger.entity.ts    # PayoutLedger entity (UNIQUE event_id)
    └── payouts.service.spec.ts    # unit tests for the business logic

test/
├── payouts.e2e-spec.ts            # e2e: health, full flow, idempotency
└── jest-e2e.json

db/
└── seed.sql                       # schema + 3 seed users (executed on the first Postgres start)
```

---

## 6. API reference

### `POST /payouts`

Publishes a payout event into the stream. Returns immediately, without waiting for processing.

**Body:**
```json
{ "userId": 1, "offerId": "offer-1", "basePayout": 10 }
```

| Field | Rule (zod) |
|-------|------------|
| `userId` | integer, positive (`z.coerce.number().int().positive()`) |
| `offerId` | string, min. 1 character |
| `basePayout` | number, positive |

**Responses:**
- `202 Accepted` → `{ "status": "accepted", "id": "1719839239123-0" }` (`id` is the stream message id / the future `event_id`);
- `400 Bad Request` → `{ statusCode, path, timestamp, error: [ …zod issues… ] }`.

**Example:**
```bash
curl -i -X POST http://localhost:3000/payouts \
  -H 'Content-Type: application/json' \
  -d '{"userId":1,"offerId":"offer-1","basePayout":10}'
```

Seed users: `1` (×1.2, balance 100), `2` (×1.0, balance 50), `3` (×1.5, balance 200).
So `basePayout: 10` for user `1` → a credit of `12.00`, new balance `112.00`.

### `GET /health`

Pings both dependencies in parallel.

- `200 OK` → `{ "status": "ok", "redis": true, "postgres": true }`;
- `503 Service Unavailable` → `{ "status": "degraded", "redis": …, "postgres": … }`
  (a `ServiceUnavailableException` is thrown if at least one dependency is unreachable).

---

## 7. Data model

**`users`**

| Column | Type | Note |
|--------|------|------|
| `id` | `SERIAL PK` | |
| `name` | `VARCHAR(255)` | |
| `balance` | `DECIMAL(10,2)` | the accumulated balance |
| `multiplier` | `DECIMAL(5,2)` | personal payout multiplier |

**`payout_ledgers`** (audit record of every transaction)

| Column | Type | Note |
|--------|------|------|
| `id` | `SERIAL PK` | |
| `user_id` | `INTEGER FK → users.id` | |
| `event_id` | `VARCHAR(255)` **UNIQUE** | stream message id → the idempotency key |
| `offer_id` | `VARCHAR(255)` | |
| `payout_amount` | `DECIMAL(10,2)` | the amount actually credited (base × multiplier) |
| `created_at` | `TIMESTAMPTZ` | `DEFAULT CURRENT_TIMESTAMP` |

`UNIQUE(event_id)` is the heart of the double-crediting protection.

---

## 8. Configuration (env variables)

Copy `.env.example` → `.env`.

| Variable | Default | Purpose |
|----------|---------|---------|
| `PORT` | `3000` | HTTP port |
| `WORKER_ONLY` | `false` | `true` → the instance starts as a consumer only, with no HTTP listener |
| `POSTGRES_HOST` / `PORT` / `USER` / `PASSWORD` / `DB` | — | Postgres connection |
| `REDIS_HOST` / `REDIS_PORT` | — | Redis connection |
| `PAYOUT_BLOCK_MS` | `5000` | How long `XREADGROUP` blocks waiting for new events |
| `PAYOUT_BATCH_SIZE` | `10` | Maximum number of messages per read |
| `PAYOUT_MAX_RETRIES` | `3` | Number of deliveries before the message goes to the DLQ |
| `PAYOUT_CLAIM_IDLE_MS` | `30000` | Idle time after which `XAUTOCLAIM` takes over another consumer's message |

> The Redis client is created with `maxRetriesPerRequest: null` and `enableReadyCheck: false` —
> this is an `ioredis` requirement for a correct blocking `XREADGROUP` (otherwise the command
> with `BLOCK` would be aborted by the internal retry limit).

---

## 9. How to run

### Prerequisites
- Docker + Docker Compose
- Node.js 18+
- npm

### Local development (service on the host, dependencies in Docker)

```bash
cp .env.example .env       # POSTGRES_HOST/REDIS_HOST=localhost for a local run
npm install
docker-compose up -d postgres redis   # bring up only the DB and Redis
npm run start:dev          # Nest in watch mode on http://localhost:3000
```

> The DB schema is created from `db/seed.sql` on the **first** start of the Postgres container.
> If you change the schema, recreate the volume:
> `docker-compose down -v && docker-compose up -d postgres redis`.

### Fully in Docker

```bash
docker-compose up -d --build    # app + postgres + redis
```

---

## 10. Testing

```bash
npm run test        # unit tests (business logic, no DB/Redis)
npm run test:e2e    # e2e (requires running Postgres + Redis)
npm run test:cov    # coverage
```

**Unit ([payouts.service.spec.ts](src/payouts/payouts.service.spec.ts))** — the
`EntityManager` is mocked, the tests verify the pure logic:
- multiplier applied + `eventId`/`offerId`/`payoutAmount` recorded;
- integer-cent arithmetic (`0.1 × 1.1 → 0.11`, no drift);
- normalisation of the string decimals returned by the driver;
- duplicate event → `'duplicate'` (the mock throws `UniqueConstraintViolationException`);
- missing user → `NotFoundException`, no ledger row created.

**E2E ([payouts.e2e-spec.ts](test/payouts.e2e-spec.ts))** — boots the whole `AppModule`,
creates a test user (×2) and verifies:
- `GET /health` → both dependencies `true`;
- **the full flow**: `POST /payouts` → `202` → the worker processed it → balance and ledger
  updated (polling with `waitFor` until the result appears);
- **idempotency**: two `processPayout` calls with the same `eventId` → the first returns
  `processed`, the second `duplicate`, the balance is credited once and the ledger has a single row.

---

## 11. Scaling

The consumer is a background service inside the application (not a separate process). To add
workers you scale the application — the Consumer Group hands every event to exactly one instance:

```bash
docker compose up -d --scale app=3
```

With `WORKER_ONLY=true` an instance comes up as a consumer **without an HTTP listener**, so
API nodes and worker nodes can be scaled independently from a single image (e.g. 2 API +
5 workers).

---

## 12. Error handling and edge cases

| Scenario | Behaviour |
|----------|-----------|
| Invalid HTTP body | `400` with the list of zod issues; nothing is written to the stream |
| Invalid payload in the stream ("poison") | straight to the DLQ (`invalid-schema`) + `XACK` — retrying is pointless |
| User does not exist | `NotFoundException`, transaction rolled back; retries up to the limit → DLQ |
| Transient DB failure | not acked → stays in the PEL → redelivered |
| Process crashed after commit, before `XACK` | the event is redelivered, `UNIQUE(event_id)` makes it a no-op → `XACK` |
| Race between two workers on the same user | `SELECT … FOR UPDATE` serialises them → no lost update |
| An instance died permanently with pending messages | `XAUTOCLAIM` hands them to a live worker after `PAYOUT_CLAIM_IDLE_MS` |
| Redis unavailable inside the loop | exponential backoff (up to 10s), no tight loop |
| Group/stream dropped externally (`NOGROUP`) | `ensureGroup()` recreates it, the cursor resets |
| Internal 5xx error | logged; the client gets the single JSON envelope from `AllExceptionsFilter` |

---

## 13. Possible interview questions (Q&A)

**Q: Why 202 and not 200/201?**
`202 Accepted` semantically means "the request has been accepted, processing will happen
later". We only put the event into the stream and do not know (and do not wait for) the
crediting result — that is the honest response for an asynchronous pattern.

**Q: `XREADGROUP` gives at-least-once. How do you avoid double crediting?**
Idempotency at the DB level: `event_id` (the stream message id) with a `UNIQUE` constraint.
The credit and the ledger insert live in one transaction, so a duplicate hits `UNIQUE`, is
caught as a no-op and gets acked. At-least-once transport + idempotency = effectively-once.

**Q: Why a pessimistic lock? Why not an optimistic one?**
The balance is a read-modify-write under concurrent workers; an optimistic lock (versioning)
would produce a lot of conflicts and retries on "hot" users here. `SELECT … FOR UPDATE`
serialises the competitors deterministically. Optimistic locking would fit low contention;
here we expect conflicts, hence pessimistic.

**Q: What happens if the process crashes exactly between the commit and `XACK`?**
The message stays in the PEL and is redelivered after the restart. The transaction is already
committed, so the ledger `INSERT` fails on `UNIQUE`, `duplicate` is returned and we simply
ack. The balance is unaffected.

**Q: How does work recover after an instance crash?**
Unacked messages stay in the dead consumer's PEL. Live workers run `XAUTOCLAIM` before every
read and take over whatever has been idle for longer than `PAYOUT_CLAIM_IDLE_MS`. Since
`consumerId` is new on every start, it is `XAUTOCLAIM` (not cursor `0`) that is responsible
for cross-restart recovery.

**Q: What is cursor `0` vs `>` in `XREADGROUP`?**
`0` (in general — any id) reads the consumer's **own PEL** — messages already delivered to it
but not acked. `>` reads **new** messages not yet handed to anyone in the group. The code
starts with `0` (to drain its own PEL) and switches to `>` once that is empty.

**Q: Why a separate (duplicated) Redis connection for the consumer?**
`XREADGROUP … BLOCK` blocks the connection for the duration of the wait. On a shared client
it would also block `XADD`/`XACK`/health pings. Hence `this.redis.duplicate()`.

**Q: Why money in integer cents and not `DECIMAL` in JS?**
JS has no native decimal; a number is a double, where `0.1 + 0.2 ≠ 0.3`. Multiplying by 100
plus `Math.round` keeps precision down to the cent. In the DB the type stays `DECIMAL(10,2)`.

**Q: What does the DLQ do and why is it needed?**
It isolates "poison" (schema-invalid) and chronically failing messages so they neither block
the stream nor spin forever. After `PAYOUT_MAX_RETRIES` the event goes to
`payout-events-dlq` with a reason and is acked in the main stream — nothing is lost silently.

**Q: How is the attempt count computed?**
From `XPENDING` for the specific id — the fourth field of the reply is the delivery count.
That is Redis's own source of truth, not a custom counter (which would not survive every restart).

**Q: Weak spots / what to improve?** — see the next section.

---

## 14. What I would improve for production

- **Migrations instead of `seed.sql`.** Right now the schema is created by the seed on the
  first container start. In production — versioned MikroORM migrations.
- **A transactional outbox on the publisher side.** If the publisher also writes to its own
  DB, there is an inconsistency window between the DB write and `XADD`; an outbox closes it.
- **DLQ tooling.** There is no auto-redrive UI/job to reprocess from the DLQ — that is the
  next step.
- **Metrics and tracing.** Prometheus counters (processed/failed/dlq) and OpenTelemetry
  spans around the consume→persist path.
- **Stream trimming.** `XADD … MAXLEN` / a periodic `XTRIM` so the log does not grow unbounded.
- **Structured logging + correlation id** end-to-end from publish to persist.

---

## 15. Detailed file-by-file walkthrough

This section is a "walk the project out loud": for every file it explains **what it does, how
exactly, and why that way**, so there is a ready answer for any question about any line.

### `src/main.ts` — the entry point

```ts
const app = await NestFactory.create(AppModule);
app.useGlobalFilters(new AllExceptionsFilter());   // one error format for the whole app
app.enableShutdownHooks();                          // Nest catches SIGTERM/SIGINT → calls onModuleDestroy
if (process.env.WORKER_ONLY === 'true') {
  await app.init();                                 // brings up the DI graph and the consumer, but NO HTTP server
  return;
}
await app.listen(port);                             // normal mode: API + consumer together
```

- **`enableShutdownHooks()`** — without it the consumer's `onModuleDestroy` would not be
  called on shutdown and the worker would not drain cleanly (the `XREADGROUP BLOCK` socket
  would be left hanging).
- **`WORKER_ONLY`** — `app.init()` brings up the entire DI graph (and therefore
  `PayoutsConsumer` through `onModuleInit`) but does not open the HTTP port. This lets a
  **single image** run either as an API node or as a pure worker node. Trick question: "does
  the consumer start in both modes?" — **yes**, because it lives in the module lifecycle
  hooks, not in the HTTP layer.

### `src/app.module.ts` — the root module

Assembles the application: `ConfigModule.forRoot({ isGlobal: true })` (env available
everywhere without re-importing), `MikroOrmModule.forRoot(config)` (a global EM),
`RedisModule`, `CommonModule` (health + filter), `PayoutsModule` (all the business logic).
There is no logic here — only composition. Question: "why is Config global?" — so it does not
have to be imported into every module.

### `src/mikro-orm.config.ts` — the ORM config

Reads the connection from env (host/port/user/password/db) with defaults. `dotenv.config()`
is there so the config also works outside Nest (e.g. for CLI migrations in the future).
`debug: NODE_ENV !== 'production'` — SQL logs in dev mode. `entities: [User, PayoutLedger]` —
an explicit list (no glob scanning). Question: "how would you change it for production?" — a
winston logger instead of debug, pool tuning, SSL, separate read replicas.

### `src/redis/redis.module.ts` — the Redis provider

Creates an `ioredis` client through a factory with the DI token `REDIS_CLIENT` and **exports**
it, so other modules inject the same singleton.

```ts
new Redis({
  host, port,
  enableReadyCheck: false,
  maxRetriesPerRequest: null,   // ← critical for blocking commands
});
```

- **`maxRetriesPerRequest: null`** — by default `ioredis` throws on a command after N retries;
  but `XREADGROUP ... BLOCK 5000` deliberately "hangs" for 5s — the standard limit would kill
  it. `null` = no limit. This is a frequent "why these two options?" question.
- **`enableReadyCheck: false`** — skips the `INFO` readiness check, which simplifies starting
  the blocking client.

### `src/payouts/payouts.constants.ts`

Three string constants: `PAYOUT_STREAM='payout-events'`, `PAYOUT_GROUP='payout-workers'`,
`PAYOUT_DLQ='payout-events-dlq'`. Extracted separately because they are used **both** in the
publisher **and** in the consumer **and** in the tests — a single source of truth for the names.

### `src/payouts/payouts.schema.ts` — the zod schema (single validation source)

```ts
z.object({
  userId: z.coerce.number().int().positive(),
  offerId: z.string().min(1),
  basePayout: z.coerce.number().positive(),
})
```

- **`z.coerce`** — because the data arrives as strings both from the HTTP JSON (numbers are
  fine) and from the **stream** (everything is a string there). Coercion makes one schema
  usable for both sources.
- **`.int().positive()` / `.min(1)`** — no negative amounts, no zero/empty offerId.
  `PayoutEvent = z.infer<...>` — the type is derived from the schema (no "schema + type" duplication).
- Question: "why zod and not class-validator?" — lightweight, functional, and one schema
  object is reused outside DI (the consumer has no decorator-based DTO class).

### `src/payouts/payouts.controller.ts` — the HTTP entry point

```ts
@Post() @HttpCode(202)
async publish(@Body() body: unknown) {
  const result = payoutEventSchema.safeParse(body);
  if (!result.success) throw new BadRequestException(result.error.issues);
  const id = await this.publisher.publish(result.data);
  return { status: 'accepted', id };
}
```

- **`@Body() body: unknown`** — deliberately `unknown`, because validation is done by zod, not
  by a Nest pipe.
- **`safeParse`** (not `parse`) — does not throw, returns `{success, data|error}`; on failure
  → `400` with the list of issues (machine-readable).
- **`@HttpCode(202)`** — the "accepted, will process later" semantics. The controller does
  **not** wait for processing — it only publishes. That is the "fast API" part.
- Returns the stream message id — the client can track the event with it (this is the future `event_id`).

### `src/payouts/payouts.publisher.ts` — publishing into the stream

```ts
this.redis.xadd(PAYOUT_STREAM, '*', 'userId', String(...), 'offerId', ..., 'basePayout', String(...))
```

- **`XADD stream * field val ...`** — `*` tells Redis to generate a monotonic id
  (`<ms>-<seq>`). Stream fields are flat key/value pairs, hence everything is stringified.
- A subtle but deliberate point: the publisher writes **only** the three business fields. The
  `event_id` used for idempotency is the **message id itself**, returned by `XADD`, not a
  separate field. Question: "where does event_id come from?" — from the stream message id,
  not from the payload.

### `src/payouts/payouts.consumer.ts` — the background worker (the most important file)

**Class fields:** `consumerId = consumer-<uuid>` (unique per process), tuning from env
(`blockMs`, `batchSize`, `maxRetries`, `claimIdleMs`), `blockingRedis` (a separate
connection), `running` (the loop flag), `errorBackoffMs`.

**`onModuleInit()`:** `ensureGroup()` → `this.redis.duplicate()` (a separate socket for BLOCK)
→ `running = true` → `void this.loop()` (started without await so it does not block Nest startup).

**`ensureGroup()`:**
```ts
XGROUP CREATE payout-events payout-workers $ MKSTREAM
// catch: if the error contains 'BUSYGROUP' the group already exists, ignore it; otherwise rethrow
```
- `$` = "new messages from this moment on" (the group's starting position). `MKSTREAM` creates
  the stream if it does not exist yet. `BUSYGROUP` is a normal state on a repeated start.

**`loop()`** — the heart of the worker:
```ts
let cursor = '0';
while (running) {
  try {
    await claimStale();                        // 1) pick up the work of dead workers
    const reply = await blockingRedis.xreadgroup(
      'GROUP', group, consumerId,
      'COUNT', batchSize, 'BLOCK', blockMs,
      'STREAMS', stream, cursor);              // 2) read
    const messages = reply?.[0]?.[1] ?? [];
    if (messages.length === 0) { cursor = '>'; continue; }  // PEL drained → read new ones
    for (const [id, fields] of messages) await handle(id, fields);
    errorBackoffMs = 0;                        // success → reset the backoff
  } catch (err) {
    if (!running) break;                       // stop cleanly
    if (String(err).includes('NOGROUP')) { await ensureGroup(); cursor = '0'; continue; }
    await backoff(err);                        // other error → exponential pause
  }
}
```
- **Cursor `0` → `>`:** starting at `0` drains the **own PEL** (whatever was already delivered
  to this consumerId but not acked — e.g. after a crash within the process). Once the PEL is
  empty, switch to `>` (new messages of the group). This is the canonical Redis pattern.
- **`NOGROUP`** — if the group/stream was dropped externally, recreate it and reset the cursor.
- **`errorBackoffMs = 0` after a success** — a reset, so a single failure does not leave the
  backoff elevated forever.

**`claimStale()`** — `XAUTOCLAIM stream group consumerId <idleMs> 0 COUNT batch`:
takes over messages idle for more than `claimIdleMs` in **any** consumer (i.e. in a dead one)
and processes them immediately. This is the recovery after an instance death. The important
point: because `consumerId` is new on every start, it is `XAUTOCLAIM` (not cursor `0`) that
recovers work **after a process restart**.

**`handle(id, fields)`** (wrapped in `@CreateRequestContext()`):
```ts
const event = parse(fields);
if (!event) { deadLetter(id, fields, 'invalid-schema'); return; }   // poison → straight to DLQ
try {
  const result = await service.processPayout(id, event);
  await redis.xack(stream, group, id);         // success (processed | duplicate) → ACK
} catch (err) {
  await handleFailure(id, fields, err);        // failure → retry or DLQ
}
```
- **`@CreateRequestContext()`** — creates a separate MikroORM Identity Map / EM context per
  processed message (outside an HTTP request there is no context of its own). Without it you
  get the "global context" error. A frequent question: "isn't that a memory leak?" — no, the
  context only lives for the duration of processing one message.
- **`XACK`** for both `processed` and `duplicate` — both mean "no need to deliver this again".

**`handleFailure()`** — reads `deliveryCount(id)`; if `>= maxRetries` → DLQ, otherwise
**no ack** (it stays in the PEL → retry).

**`deadLetter()`** — `XADD dlq * reason <reason> ...fields` (copies the original fields plus
the reason), then `XACK` in the main stream. Nothing is lost silently.

**`deliveryCount()`** — `XPENDING stream group IDLE 0 <id> <id> 1`; returns the 4th field
(`pending[0][3]`) — **Redis's own** delivery counter. Defaults to `?? 1`. Question: "why not
a custom counter?" — because the Redis PEL survives a restart, an in-process counter does not.

**`parse(fields)`** — flat `[k,v,k,v]` → object → `safeParse`; returns `data` or `null`.

**`backoff(err)`** — an exponential pause `500ms → ×2 → 10s cap`, so an unavailable Redis does
not burn CPU in a tight loop.

**`onModuleDestroy()`** — `running = false` + `blockingRedis.quit()`: the loop finishes and
the socket closes. Graceful shutdown.

### `src/payouts/payouts.service.ts` — the business logic (transaction + money)

```ts
async processPayout(eventId, event): Promise<'processed' | 'duplicate'> {
  try {
    await this.em.transactional(async (em) => {
      const user = await em.findOne(User, { id: event.userId },
        { lockMode: LockMode.PESSIMISTIC_WRITE });      // SELECT ... FOR UPDATE
      if (!user) throw new NotFoundException(...);       // rolls the transaction back
      const amount = calculatePayout(event.basePayout, user.multiplier);
      user.balance = addMoney(user.balance, amount);     // dirty tracking → UPDATE on commit
      em.create(PayoutLedger, { user, eventId, offerId, payoutAmount: amount, createdAt });
    });
    return 'processed';
  } catch (err) {
    if (err instanceof UniqueConstraintViolationException) return 'duplicate';  // idempotency
    throw err;                                           // anything else → up, the consumer decides retry/DLQ
  }
}
```

- **`em.transactional`** — everything inside is either committed together or rolled back. The
  balance and the ledger cannot diverge.
- **`PESSIMISTIC_WRITE`** → `SELECT ... FOR UPDATE`: two consumers on the same user are
  serialised, there is no lost update on the read-modify-write of the balance.
- **Idempotency:** a unique `event_id` → a repeated insert throws
  `UniqueConstraintViolationException`, which we catch and return `'duplicate'` (a no-op).
  Important: the transaction is fully rolled back in that case, so the balance is **not**
  changed a second time.
- **`calculatePayout`** — `Math.round(base * Number(multiplier) * 100) / 100`. The multiplier
  goes through `Number()` because the driver returns decimals as strings.
- **`addMoney`** — both operands are converted to integer cents, added, and divided by 100.
  This avoids the `0.1 + 0.2` drift. The DB column stays `DECIMAL(10,2)`.
- Question: "why return two different strings?" — the consumer logs `processed` while
  `duplicate` is acked quietly; both lead to an ACK, but the semantics and metrics differ.

### `src/payouts/payout-ledger.entity.ts` — the audit entity

`@Unique() eventId` is the DB-level protection against double crediting (mirroring the
`UNIQUE` in `seed.sql`). `payoutAmount` is `decimal(10,2)`. `@ManyToOne(() => User)` +
`Rel<User>` — the relation to the user. Every successful credit = one immutable row
(append-only audit).

### `src/users/user.entity.ts` — the user

`balance` (`decimal(10,2)`, default 0) and `multiplier` (`decimal(5,2)`, default 1) — the
personal payout multiplier. `@OneToMany` to the ledgers (the inverse side of the relation).
Question: "why is the balance not integer cents in the DB?" — the challenge schema was given
as `DECIMAL`; I keep the precision in the code and leave the type compatible. In production I
would consider a `BIGINT` of cents.

### `src/common/health.controller.ts` — `GET /health`

Pings Redis (`PING`) and Postgres (`SELECT 1`) in parallel (`Promise.all`). Each ping is
wrapped in `ping()`, which catches the error and returns a `boolean`. `status = ok` only if
**both** are alive; otherwise it throws `ServiceUnavailableException` (503) with the details.
This is a readiness probe for k8s: a pod with a dead dependency is removed from load balancing.

### `src/common/http-exception.filter.ts` — the global error filter

`@Catch()` with no arguments = catches **everything**. An `HttpException` → its status/message;
anything else → `500` + "Internal server error" (internal details do not leak). Only `>= 500`
is logged. Returns a single envelope `{ statusCode, path, timestamp, error }`. Question: "why
unify it?" — a predictable error contract for clients and easier debugging.

### `src/common/common.module.ts` and `src/redis/redis.module.ts` (modules)

Thin composition modules; `CommonModule` imports `RedisModule` (health needs Redis), and no
logic is exported. They demonstrate Nest's module isolation.

### `db/seed.sql` — schema + seed

Creates `users` and `payout_ledgers` with `UNIQUE(event_id)` and the FK. Inserts 3 users
(×1.2/100, ×1.0/50, ×1.5/200). It is executed by the Postgres container **only on the first
start** (via `/docker-entrypoint-initdb.d`). Question: "how do you update the schema?" —
recreate the volume (`down -v`), and in production use migrations.

### Tests

- **`payouts.service.spec.ts`** — unit tests with a **mocked** `EntityManager` (no DB):
  multiplier, integer cents, string decimals, duplicate → `duplicate`, missing user →
  `NotFoundException`. `em.transactional` is mocked so that it simply invokes the callback
  with a fake `txEm`.
- **`payouts.e2e-spec.ts`** — boots the whole `AppModule`, clears the stream, creates a test
  user (×2), and verifies: `/health`, the full flow (POST → the worker processed it →
  balance + ledger, with `waitFor` polling), idempotency (`processPayout` twice with the same
  id → a single row). `afterAll` cleans up the data and destroys the group.

### Infrastructure

- **`Dockerfile`** — multi-stage: `builder` (npm install + `nest build`) → a thin runtime
  (only `node_modules` + `dist`), `CMD node dist/main`. Question: "why multi-stage?" — a
  smaller image, with no dev dependencies or sources in production.
- **`docker-compose.yml`** — `app` (build + a volume on `./src` for hot reload), `postgres:15`
  (with `seed.sql` mounted), `redis:7-alpine`. Named volumes for persistence.

---

## Stack

Nest.js 11 · TypeScript 5 · MikroORM 6 (PostgreSQL) · ioredis 5 (Redis Streams) ·
zod 4 · Jest · Docker Compose.
