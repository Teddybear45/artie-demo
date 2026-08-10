# Frankenstein Queue

An HTTP message queue where FIFO/LIFO, priority, and [SQS-style delay](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html)
are **configuration of one engine, not separate implementations** — so a single
queue can be a delayed priority LIFO, a priority FIFO, or any other combination.
Pure Go stdlib, no external dependencies.

## Quick start

```bash
go run ./cmd/queued            # starts the server on :8080, persists to ./data/
go run ./cmd/demo              # in another terminal: runs all four demo scenarios
go test ./...                  # unit tests (ordering, delay, crash-replay)
```

The demo exercises priority FIFO, priority LIFO, delay, and 4 concurrent
producers + 4 concurrent consumers racing over 100 messages, verifying every
message is delivered exactly once.

**Durability check:** enqueue some messages (including a delayed one), `kill -9`
the server, restart it — everything is still there, and the delay is still
honored, because visibility is stored as an absolute timestamp.

## API

| Endpoint | Body | Behavior |
|---|---|---|
| `POST /queues` | `{"name": "jobs", "order": "fifo"\|"lifo"}` | Create a queue; ordering is fixed at creation (like SQS) |
| `POST /queues/{name}/messages` | `{"body": "...", "priority": 9, "delay_seconds": 30}` | Enqueue; `priority`/`delay_seconds` optional (default 0) |
| `POST /queues/{name}/dequeue` | — | Pop the best visible message; `204` if none is ready |
| `GET /queues/{name}` | — | Stats: ready count, delayed count, config |

## Design

### One heap, two knobs

The insight that keeps this small: the three features are one ordered structure
plus one visibility rule.

- Every message gets a monotonically increasing **sequence number** at enqueue.
- **Ordering** is just a comparator: higher `priority` first, ties broken by
  sequence — ascending for FIFO, descending for LIFO. A plain FIFO/LIFO queue
  is the degenerate case where all priorities are equal.
- **Delay** is not ordering at all — it's *visibility*. A delayed message gets
  `visible_at = now + delay` and sits in a separate min-heap keyed by
  `visible_at`; it is promoted into the ready heap (lazily, on dequeue/stats)
  once its time passes. Until then it cannot be delivered, regardless of
  priority — matching SQS semantics, where a delayed message effectively isn't
  in the queue yet.

So "delayed priority LIFO" is not a code path; it's a queue created with
`order=lifo` receiving messages that happen to carry `priority` and
`delay_seconds`.

### Durability: append-only operation log

Storage cannot be delegated to a database, so the service writes its own
**write-ahead log** (`data/queue.log`): every state change — queue creation,
enqueue, dequeue — is one JSON line, appended and `fsync`'d *before* the HTTP
response is sent. That ordering is what makes an acknowledged operation
durable. On startup the log is replayed: enqueues insert, dequeues delete, and
the surviving set is exactly the pre-crash state. A crash mid-write can leave
one torn final line; replay detects it and stops there, which is correct
because that operation was never acknowledged.

Alternatives considered:

- **Snapshot-per-change** (rewrite full state + atomic rename): less code but
  O(n) disk work per operation — collapses under load as the queue grows.
- **WAL + periodic snapshots** (the Redis RDB+AOF hybrid): bounds log size and
  restart time, but roughly doubles the persistence code. This is the natural
  next step here, along with **compaction** (rewrite the log keeping only live
  messages) — both deliberately described rather than built.
- **Embedded SQLite/bbolt**: ruled out as delegating storage to a database.

### Concurrency

Go's `net/http` runs every request in its own goroutine, so the server is
concurrent by construction; correctness comes from one mutex per queue guarding
the tiny critical section (heap operation + log append). Two consumers racing
for the same message serialize on that lock, so each message is delivered
exactly once — which the demo's scenario 4 verifies empirically. A coarse
per-queue mutex is the deliberate choice at this scale: the critical section is
dominated by the fsync anyway, so finer-grained locking would add risk without
adding throughput.

### Deliberate simplifications

- **Dequeue is a destructive pop** — no ack/visibility-timeout lease. If a
  consumer crashes after dequeuing, that message is gone. The SQS-style fix
  (dequeue hides the message for a lease period; ack deletes it; expiry makes
  it visible again) slots cleanly into the existing visibility mechanism — the
  leased message would re-enter the delayed heap with `visible_at = now +
  lease` — and is the first thing to add next.
- **No long-polling** — an empty dequeue returns `204` immediately; clients
  poll.
- **Single node** — replication/partitioning is out of scope; the log design
  is the piece a replicated version would build on.

## Layout

```
cmd/queued/           HTTP server entrypoint
cmd/demo/             producer/consumer demo client (doubles as e2e test)
internal/queue/       engine: heaps + comparator + mutex (queue.go),
                      log append/replay + registry (store.go), tests
internal/api/         the four HTTP handlers
```
