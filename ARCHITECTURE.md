# SMS Gateway — Architecture

This document describes the system design of the SMS Gateway built for the
ArvanCloud software developer challenge: a REST API that lets any number of
clients top up a balance, send SMS (bulk or express), and fetch delivery
reports, at a scale of ~100M messages/day with highly uneven traffic across
clients.

## 1. Goals driving the design

| Requirement from the spec | Design response |
|---|---|
| ~100M SMS/day, unpredictable spikes | API never talks to the telecom directly; it only validates + queues. Kafka absorbs bursts, workers drain at their own pace. |
| Traffic is very uneven across clients (a few clients send huge volumes, most send very little) | Kafka messages are **not** partitioned by `user_id`. A `RoundRobin` balancer spreads every client's messages evenly across all partitions, so one noisy client can't starve a worker while others sit idle. |
| Express SMS (e.g. OTP codes) need a delivery-time guarantee | Express and bulk traffic live on **separate Kafka topics**, each with their own partitions and worker pool, so a flood of bulk marketing SMS can never delay an OTP. |
| A client must be able to use 100% of their balance, and must never be able to send after it hits zero | Two-layer balance guard: Redis for a sub-millisecond pre-check, MySQL as the transactional source of truth that can never go negative. |
| Clients must be able to fetch delivery reports | Every SMS is persisted with a status (`PENDING` / `DELIVERED` / `FAILED`) and is queryable per user. |
| REST only, no UI, no auth | Plain JSON HTTP API (`gin`), no session/auth layer. |
| 7-day budget | Go, because concurrency (goroutines/channels) maps directly onto "one goroutine per Kafka partition," which is the whole scaling story. |

## 2. High-level component diagram

```mermaid
flowchart LR
    subgraph Clients
        C1["Client A - low volume"]
        C2["Client B - high volume"]
    end

    C1 -->|REST| API
    C2 -->|REST| API

    subgraph API Service
        API["Gin HTTP API"]
    end

    API <-->|"Lua script: atomic check-and-decrement"| Redis[("Redis<br/>cached balance")]
    API -->|"reserve credit in Redis, then produce message"| Kafka

    subgraph Kafka
        direction TB
        TE["Topic: sms_express<br/>N partitions"]
        TB2["Topic: sms_bulk<br/>M partitions"]
    end

    Kafka --> TE
    Kafka --> TB2

    TE --> WE1["worker-express #1..k"]
    TB2 --> WB1["worker-bulk #1..k"]

    WE1 -->|"debit + insert, one SQL transaction"| MySQL[("MySQL<br/>source of truth")]
    WB1 -->|"debit + insert, one SQL transaction"| MySQL

    WE1 -->|SendSMS| OP["Mock Telecom Operator"]
    WB1 -->|SendSMS| OP

    WE1 -.->|"refund / invalidate on failure"| Redis
    WB1 -.->|"refund / invalidate on failure"| Redis

    API -->|GetReports| MySQL
```

Two binaries share the same domain/repository code (`cmd/api`, `cmd/worker`):

- **API** (`cmd/api`) — stateless, horizontally scalable. Owns `POST /users/charge`,
  `POST /sms/send`, `GET /users/:id/report`.
- **Worker** (`cmd/worker`) — one process, run in two deployment flavors
  (`WORKER_TYPE=express` / `bulk`), each consuming its own topic. This is the
  component that actually talks to MySQL for debits and to the (mock) telecom
  operator.

Both binaries only depend on `internal/domain` interfaces (`UserRepository`,
`SMSRepository`, `CreditRepository`, `CacheRepository`, `MessageProducer`,
`SMSOperator`, `TransactionManager`) — this is the Clean/Hexagonal split:

```mermaid
flowchart TB
    subgraph cmd
        A["cmd/api"]
        W["cmd/worker"]
    end
    subgraph internal_domain ["internal/domain"]
        I["Interfaces + Models + Errors"]
    end
    subgraph adapters ["Adapters"]
        D["internal/delivery<br/>HTTP handlers"]
        K["internal/kafka<br/>Producer / Consumer"]
        R["internal/repository<br/>MySQL / Redis"]
        O["internal/operator<br/>mock telecom"]
    end
    A --> D
    A --> K
    A --> R
    W --> K
    W --> R
    W --> O
    D --> I
    K --> I
    R --> I
    O --> I
```

`internal/delivery` (the HTTP layer) and `internal/kafka` (the consumer) never
import `database/sql` or a MySQL driver directly — they only see the
interfaces in `internal/domain`. This is what makes both fully unit-testable
with in-memory fakes (see `internal/delivery/http_test.go` and
`internal/kafka/consumer_test.go`), with no real database or broker needed.

## 3. Request flow: sending an SMS

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Redis
    participant MySQL
    participant Kafka
    participant Worker
    participant Operator as Telecom Operator (mock)

    Client->>API: POST /api/v1/sms/send
    API->>Redis: EVAL deduct(cost) via Lua script
    alt cache hit, sufficient balance
        Redis-->>API: 1 (deducted)
    else cache miss
        API->>MySQL: SELECT balance
        API->>Redis: SETNX balance (init cache)
        API->>Redis: EVAL deduct(cost) again
    else insufficient balance
        Redis-->>API: 0
        API-->>Client: 402 Payment Required
    end
    API->>Kafka: produce SMS (sms_express or sms_bulk)
    alt Kafka publish fails
        API->>Redis: refund reserved credit
        API-->>Client: 500
    else Kafka publish succeeds
        API-->>Client: 200 "queued", sms id
    end

    Kafka-->>Worker: FetchMessage
    Worker->>MySQL: TX: DeductBalance (guarded, WHERE balance >= cost) + INSERT sms_records (PENDING) + INSERT credit
    alt insufficient in MySQL (Redis was stale)
        Worker->>MySQL: INSERT sms_records (FAILED)
        Worker->>Redis: InvalidateBalance (drop stale cache key)
    else debit succeeded
        Worker->>Operator: SendSMS(to, text)
        alt operator success
            Worker->>MySQL: UPDATE status PENDING -> DELIVERED
        else operator failure
            Worker->>MySQL: TX: UPDATE status PENDING -> FAILED + AddBalance(refund) + INSERT credit(REFUND)
            Worker->>Redis: AddBalance(refund)
        end
    end
    Worker->>Kafka: CommitMessages (only after the above completes)
```

Key point: the API's Redis check is only a **fast-path reservation**, never
the final word. The worker's MySQL transaction is the actual charge, guarded
by `UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?`. If
Redis and MySQL ever disagree (Redis restarted, evicted a key, or was never
populated), MySQL wins and the worker invalidates the Redis key so the next
request reloads the true balance. This is what makes "balance can never go
negative" and "no free SMS" hold even under cache failures, not just under
the happy path.

## 4. Redis: persistence, and what happens if it crashes or loses data

### What's actually configured

`deployments/docker-compose.yml` starts Redis with `--appendonly yes --appendfsync everysec`. That turns on **AOF (Append Only File)** persistence: every write command Redis executes is appended to a log file on the `redis_data` volume, and on restart Redis replays that log to rebuild the dataset. `appendfsync everysec` means the OS buffer is flushed to physical disk once a second (not on every single write) — so a **crash** (not a clean shutdown) can lose at most the last ~1 second of writes; a clean restart loses nothing, since Redis flushes before exiting.

The compose file never passes `--save ""`, so Redis's **built-in default RDB snapshot schedule is also still active** underneath the explicit AOF setting (e.g. snapshot if 10000+ keys changed in 60s, 100+ in 300s, or 1+ in 3600s). In practice this stack is running the hybrid RDB+AOF setup, not AOF alone, even though only AOF is mentioned explicitly.

### What persistence does *not* cover

AOF/RDB only reduce data loss in the *ordinary* crash/restart case. They do nothing for a genuine full wipe — the `redis_data` volume being deleted, a manual `FLUSHALL`, or the AOF file itself getting corrupted — because in that case there's no log left to replay. That's the scenario worth designing for on its own merits, and the application doesn't rely on Redis persistence to stay correct in it.

### How the application handles a Redis crash / full data loss

Redis in this system is a **fast-path cache, never the source of truth** — the balance guarantee comes entirely from MySQL's guarded `UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?` (see section 3), which doesn't care whether Redis has any data at all. Concretely, if Redis loses everything (a crash with no AOF to replay, or a full wipe) while, say, 1,000 SMS are already sitting in Kafka:

- **The 1,000 already-queued messages are unaffected.** Redis's state played no further part in them — each is debited by the worker straight against MySQL when it's actually processed, using the guard above. Any that don't fit the real balance are marked `FAILED` with no charge; none can ever be sent for free.
- **New requests arriving during the outage hit a cache miss.** `internal/delivery/http.go`'s `SendSMS` sees `DeductBalance` return `-1` (key doesn't exist in Redis), reads the real balance from MySQL (`GetBalance`), reseeds Redis with `SETNX` (`InitBalance`), and deducts from that freshly seeded key. Because MySQL hasn't yet processed the 1,000 messages still in Kafka at that moment, the number it reads is temporarily higher than the user's true remaining balance — so a few *new* requests may get accepted ("200 queued") that ultimately can't be paid for.
- **Those over-accepted requests still can't cause harm.** When each one reaches a worker, it goes through the exact same MySQL guard as every other message. Whichever of them the real balance can't cover come back `FAILED` with a refund of nothing (they were never charged), and the worker calls `InvalidateBalance` for that user so the next request reloads a correct number from MySQL.

```mermaid
flowchart TD
    A["Redis crashes / loses all data"] --> B["Next request for user X arrives"]
    B --> C["Redis DeductBalance -> -1 (cache miss)"]
    C --> D["API reads real balance from MySQL"]
    D --> E["API reseeds Redis: SETNX (InitBalance)"]
    E --> F["API deducts from the freshly seeded key"]
    F --> G["SMS produced to Kafka"]
    G --> H["Worker consumes the message"]
    H --> I{"MySQL guarded UPDATE:<br/>balance >= cost?"}
    I -->|yes| J["Debit commits, SMS sent"]
    I -->|no| K["SMS marked FAILED, no charge,<br/>Redis key invalidated"]
```

Net effect: the two guarantees that matter — **balance never goes negative** and **no SMS is ever sent for free** — hold exactly as well with Redis completely empty as they do with it fully warm, because neither guarantee is implemented in Redis. The only real cost is UX-level and temporary: during the recovery window, some sends that the API accepted with a 200 can end up `FAILED` a little later instead of being rejected with a 402 immediately, and (since partitioning is round-robin, not per-user) which specific messages fail isn't strictly FIFO — though the total count of successes/failures still lines up with the user's real balance. This window closes on its own as soon as the Kafka backlog drains and MySQL catches up.

## 5. Idempotency and at-least-once delivery

Kafka consumers in this system are **at-least-once**: a crash between
"process message" and "commit offset" causes the same message to be
redelivered. The design handles every redelivery case explicitly instead of
assuming it won't happen:

- **SMS id is the Kafka message key and the MySQL primary key.** A redelivered
  "debit + insert" transaction hits a duplicate-key error (`ErrDuplicateRecord`),
  which the worker treats as "already handled" rather than double-charging.
- **Crash between debit and insert:** on redelivery, the debit fails (already
  done) but the SMS row doesn't exist yet — the worker detects this and
  resumes from "send to operator" without charging again.
- **Crash between insert and status finalize:** `UpdateStatusFrom(id, PENDING, DELIVERED/FAILED)`
  is a conditional `UPDATE ... WHERE status = 'PENDING'`. A second delivery
  attempt sees the row is no longer `PENDING` and treats the transition as a
  no-op instead of re-sending or re-refunding.
- **A message is never `continue`-skipped on a transient error.** Because
  `kafka-go` commits are cumulative (committing offset N implicitly commits
  every offset before it), skipping message N would silently drop it forever,
  along with the user's paid credit. Instead, `processMessage` retries in
  place with backoff until it succeeds or the process is shutting down; on
  shutdown, the offset is deliberately left uncommitted so the next consumer
  instance picks the message back up.

## 6. Avoiding InnoDB deadlocks under concurrent workers

The worker's "debit + insert SMS + insert credit" unit of work (see section 3)
touches the same user row from three statements in one transaction. `sms_records`
and `credits` both carry a foreign key to `users`, so MySQL's InnoDB engine
needs *some* lock on the user row to satisfy each `INSERT`, and a stronger
lock to run the `balance` update. If two workers happened to process messages
for the **same** user at the same time and issued their statements in
different orders, InnoDB can deadlock (Error 1213): worker A holds a Shared
lock (from an `INSERT`'s FK check) while waiting to escalate to Exclusive for
the balance update, and worker B is doing the mirror image — each waiting on
the other.

The fix is write ordering, not a different isolation level: every transaction
in this codebase runs `UPDATE users SET balance = balance ± ?` **first** —
`DeductBalance` before `SMSRepository.Create`/`CreditRepository.Create` in
`internal/kafka/consumer.go`'s debit path, and the same order on the refund
path. Acquiring the Exclusive lock up front means a second worker touching the
same user simply queues behind the first one instead of racing it for lock
escalation, which removes the deadlock without weakening consistency.

## 7. Scaling to ~100M messages/day with uneven clients

- 100M/day ≈ 1,160 msg/sec average, with real spikes far above that — Kafka
  is the shock absorber between the bursty API traffic and the
  rate-limited-by-nature worker/telecom side.
- Partitioning uses `kafka.RoundRobin`, **not** a hash of `user_id`. If
  partitioning were keyed by client, one client sending 50,000 SMS in a burst
  would pin all of them to a single partition/consumer, while eight other
  workers sat idle — exactly the "unequal distribution" problem called out in
  the spec. Round-robin spreads every client's messages across all partitions,
  so throughput scales with total partition/worker count regardless of which
  client is generating the load.
- Express vs. bulk are physically separate topics (`sms_express`,
  `sms_bulk`), each independently scalable via `KAFKA_PARTITIONS_EXPRESS` /
  `KAFKA_PARTITIONS_BULK` and `WORKER_EXPRESS_REPLICAS` / `WORKER_BULK_REPLICAS`
  in `deployments/.env`. A backlog of bulk marketing traffic can never delay
  an express OTP, because they're different consumer groups on different
  topics entirely.
- Scaling model is "one goroutine per partition, one partition per worker
  slot" — no internal goroutine pools inside a single consumer. This was a
  deliberate choice: committing offsets concurrently from multiple goroutines
  inside one consumer risks committing offset N+1 before offset N is durably
  processed, which can silently lose a message on a crash. Instead, throughput
  is scaled the same way Kafka is designed to be scaled: add partitions, add
  worker replicas.
- MySQL connection pools are sized per role and documented in `.env`
  (`API_DATABASE_MAX_OPEN_CONNECTIONS`, `WORKER_DATABASE_MAX_OPEN_CONNECTIONS`)
  with the constraint `api_conns + (express_replicas + bulk_replicas) * worker_conns < MYSQL_MAX_CONNECTIONS`
  spelled out right next to the values, so scaling replica counts doesn't
  silently exhaust MySQL's connection limit.
- `sms_records(user_id, created_at DESC)` is a composite index, so
  `GetReports` stays O(log n) instead of a filesort even once a user has
  millions of rows.

## 8. Data model

```mermaid
erDiagram
    USERS ||--o{ SMS_RECORDS : sends
    USERS ||--o{ CREDITS : "has ledger entries"

    USERS {
        int id PK
        bigint balance "UNSIGNED, never negative"
        timestamp created_at
    }
    SMS_RECORDS {
        varchar id PK "UUID, also the Kafka message key"
        int user_id FK
        varchar to_number
        text text
        enum status "PENDING / DELIVERED / FAILED"
        bool is_express
        timestamp created_at
        timestamp updated_at
    }
    CREDITS {
        varchar id PK "UUID"
        int user_id FK
        bigint amount
        varchar type "TOPUP / SMS_SENT / REFUND"
        timestamp created_at
    }
```

`balance` is `BIGINT UNSIGNED` and MySQL runs with `sql-mode=STRICT_ALL_TABLES`,
so the database engine itself rejects any operation that would drive it
negative — this is a second, storage-level backstop underneath the
application-level `WHERE balance >= cost` guard. `credits` is an audit trail
(top-up / charge / refund) rather than something the business logic reads
back from, which keeps the balance check itself a single indexed column read.

## 9. Deployment topology

`deployments/docker-compose.yml` runs: `mysql`, `redis`, `zookeeper` + `kafka`,
a one-shot `init-kafka` job that creates `sms_express`/`sms_bulk` with their
configured partition counts, one `api` service, and two independently-scaled
worker services (`worker-express`, `worker-bulk`) built from the same image
via `WORKER_TYPE`. All tunables (ports, replica counts, partition counts,
connection-pool sizes, `SMS_COST`) live in `deployments/.env`
(`deployments/.env.example` is the checked-in template; `.env` itself is
git-ignored since it holds credentials).

## 10. Known simplifications (in scope for this challenge)

- The mock operator (`internal/operator/mock.go`) simulates 10–100ms latency
  and a 90% success rate; a real integration would replace `SMSOperator` with
  an adapter that calls the actual telecom API — no other layer would need to
  change, since the worker only depends on the `SMSOperator` interface.
- No authentication/authorization layer, no multi-page/concatenated SMS
  handling, and a single flat price regardless of language — all per the
  challenge's explicit assumptions.
- Kafka replication factor is 1 in the local compose stack (single broker);
  a production deployment would run a multi-broker cluster with a replication
  factor ≥ 3 for durability, and remove `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`'s
  single point of setup (`init-kafka`) in favor of Terraform/GitOps-managed
  topics.
