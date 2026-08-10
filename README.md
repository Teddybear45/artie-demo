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
- **WAL + periodic snapshots**: the production-grade hybrid — designed below,
  deliberately not built.
- **Embedded SQLite/bbolt**: ruled out as delegating storage to a database.

### Periodic snapshots — the designed-but-not-built half of the hybrid

The WAL alone has two costs that grow with *history* rather than with *live
data*: the log file never shrinks, and restart replay touches every operation
ever performed — including the millions of messages that were long since
consumed. Periodic snapshots (the same idea as Redis's RDB+AOF pairing or
Postgres checkpoints) bound both. The design that would slot into this
codebase:

1. **Trigger**: after every K log records (or B bytes appended), a snapshot
   runs — under the queue locks, or from a copy taken under the locks so the
   pause is one memory copy, not one disk write.
2. **Rotate first**: start appending to a fresh log file (`queue.log.2`).
   From this instant, the old log is immutable and the snapshot has a clean
   cut point.
3. **Write the snapshot**: serialize all live messages (both heaps, plus each
   queue's config and sequence counter) to `snapshot.tmp`, `fsync` it, then
   atomically `rename()` to `snapshot.json`. The rename is the commit point —
   readers can never observe a half-written snapshot.
4. **Delete the old log** — only after the rename lands. Recovery is now:
   load `snapshot.json`, then replay only the short `queue.log.2` tail.

Crash safety falls out of the ordering: a crash before the rename leaves the
old snapshot + both logs (full replay still works); a crash after the rename
but before the delete leaves a stale log that recovery can identify and skip
via the sequence counter stored in the snapshot. Every state on disk is
recoverable; nothing depends on two files changing together.

**Why it's documented rather than implemented:** it roughly doubles the
persistence code and adds the subtlest failure windows in the system (the
rotate/rename/delete ordering above), while at demo scale replay is
milliseconds. The WAL was built to make acknowledged writes durable — the
requirement; snapshots make *restarts fast and disk bounded* — an optimization
with a clear design ready when the log gets long.

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
