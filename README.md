# Job Queue ![visitors](https://visitor-badge.laobi.icu/badge?page_id=harshalvk.job-queue&left_text=visitors&left_color=%234f4f4f&right_color=%23c48312)

<img width="1498" height="500" alt="image" src="https://github.com/user-attachments/assets/f6fffc1f-224b-4634-8d32-567b9abcc60e" />


A distributed job queue built from scratch in Go — a mini Sidekiq/Celery, without reaching for an off-the-shelf framework. The goal is to actually understand the primitives (worker pools, retries, dead-lettering, backoff) rather than just importing a library that hides them.

Backed by Redis for the hot queue, with Postgres planned for durable job history.

## Why build this instead of using an existing library?

Libraries like Sidekiq, Celery, or Asynq solve this problem well — but using them skips past the actual mechanics: 
- how does a worker pool avoid spawning unbounded goroutines?
- How do retries avoid hammering a failing dependency?
- How do you not lose jobs when a process crashes mid-retry?

This project builds each of those pieces manually, one at a time, with the reasoning behind each design decision documented alongside the code.

## Project structure

```
jobqueue/
├── go.mod
├── job.go              # core Job struct and constructor (package jobqueue)
├── queue.go            # Redis-backed queue: enqueue/dequeue/dead-letter ops
├── worker.go           # worker pool, retry logic, backoff
├── cmd/
│   ├── producer/       # CLI to enqueue test jobs
│   ├── worker/         # runs the worker pool, processes jobs
│   └── deadletter/     # CLI to list/requeue/purge dead-lettered jobs
└── README.md
```

## Supported features

### 1. Core job model

UUID-based `Job` struct with `Type`, `Payload` (raw JSON, so the queue stays agnostic to job contents), `Status`, `Attempts`/`MaxAttempts`, timestamps, and `LastError`. This struct is the shared contract every other component (queue, worker pool, retries, dead-letter) builds against, so they never drift out of sync with each other.

**Why UUIDs:** no coordination needed between multiple producers or worker nodes — any process can generate a valid job ID locally.

### 2. Redis-backed queue

`Enqueue`/`Dequeue` implemented using Redis `LPUSH`/`BRPOP` against a list. `BRPOP` blocks until a job is available — no polling loop burning CPU while the queue is empty.

**Why a list and not Streams (yet):** starting with the simplest primitive that works means understanding exactly what's traded away before reaching for something more complex like Redis Streams (which offer consumer groups and replay — relevant once this goes multi-node).

### 3. Worker pool

A fixed number of goroutines (`concurrency`), each looping forever, pulling jobs and routing them to a registered `Handler` based on `job.Type`. Uses `context.Context` + `sync.WaitGroup` for coordinated shutdown, and a 5-second `BRPOP` timeout (rather than blocking forever) so workers can periodically check if they've been asked to stop.

**Why a fixed pool and not one goroutine per job:** caps how much work happens in parallel, protecting downstream resources (databases, external APIs) from being overwhelmed if a burst of jobs land at once.

### 4. Retries with exponential backoff

On handler failure, `Attempts` increments and the job is either requeued after a backoff delay (`2^attempts` seconds, capped at 30s) or moved to the dead-letter queue if `MaxAttempts` is exhausted.

**Known limitation (called out intentionally):** the current retry delay uses an in-memory goroutine (`time.After`), which means a crashed worker process loses any pending retries. This gets fixed properly using a Redis sorted set for durable, crash-safe scheduling (see roadmap).

### 5. Dead-letter queue

Permanently-failed jobs move to a separate Redis list (`jobqueue:dead_letter`) instead of just being logged and dropped. Includes `ListDeadLetter` (inspect without removing), `RequeueDeadLetter` (reset attempts and retry), and `PurgeDeadLetter` (delete all), plus a small CLI (`cmd/deadletter`) to drive these manually.

**Why a separate list:** keeps failed jobs out of the pending queue so workers never waste cycles retrying something already known to be permanently broken; separates "peek" operations from "mutate" operations so debugging never has side effects.

## Running it locally

```bash
# start redis
docker run -d -p 6379:6379 redis

# terminal 1: start the worker pool
go run ./cmd/worker

# terminal 2: enqueue a test job
go run ./cmd/producer

# inspect dead-lettered jobs
go run ./cmd/deadletter -action=list
go run ./cmd/deadletter -action=requeue -id=<job-uuid>
go run ./cmd/deadletter -action=purge
```

## Requirements

- Go 1.21+
- Redis (local or Docker)
- `github.com/redis/go-redis/v9`
- `github.com/google/uuid`

> README.md is ai-generated
