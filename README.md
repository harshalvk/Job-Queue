# Kairos

![visitors](https://visitor-badge.laobi.icu/badge?page_id=harshalvk.job-queue&left_text=visitors&left_color=%234f4f4f&right_color=%23c48312)
[![CI](https://github.com/harshalvk/kairos/actions/workflows/ci.yml/badge.svg)](https://github.com/harshalvk/Job-Queue/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/harshalvk/kairos)](https://goreportcard.com/report/github.com/harshalvk/kairos)
[![Go Reference](https://pkg.go.dev/badge/github.com/harshalvk/kairos.svg)](https://pkg.go.dev/github.com/harshalvk/kairos)
<img width="1498" height="288" alt="image" src="https://github.com/user-attachments/assets/1622967b-2453-490e-ae40-432183abacda" />

A distributed job queue built from scratch in Go — a mini Sidekiq/Celery, without reaching for an off-the-shelf framework. The goal is to actually understand the primitives (worker pools, retries, dead-lettering, priority queues, dependency graphs, idempotency) rather than just importing a library that hides them.

## Why "Kairos"

In ancient Greek, *chronos* is clock time — sequential, measured. *Kairos* is the right, opportune moment for something to happen. That's really what this queue is about: not running things fast, but running each job at the moment it's actually meant to run — after dependencies resolve, once backoff has passed, ahead of lower-priority work when it matters. Kairos felt like the right name for a system whose whole job is figuring out the right moment.

## Why build this instead of using an existing library?

Libraries like Sidekiq, Celery, or Asynq solve this problem well — but using them skips past the actual mechanics:

- how does a worker pool avoid spawning unbounded goroutines?
- How do retries avoid hammering a failing dependency?
- How do you not lose jobs when a process crashes mid-retry?

This project builds each of those pieces manually, one at a time, with the reasoning behind each design decision documented alongside the code.

## Project structure

```
kairos/
├── go.mod
├── job.go              # core Job struct and constructor (package kairos)
├── queue.go            # Redis-backed queue: enqueue/dequeue/dead-letter ops
├── worker.go           # worker pool, retry logic, backoff
├── cmd/
│   ├── producer/       # CLI to enqueue test jobs
│   ├── worker/         # runs the worker pool, processes jobs
│   └── deadletter/     # CLI to list/requeue/purge dead-lettered jobs
│   └── scheduler/      # reshedules a dead-lettered job
└── README.md
```

## Supported features

- **Core job model** — UUID-based `Job` struct (`Type`, raw JSON `Payload`, `Status`, `Attempts`/`MaxAttempts`, timestamps, `LastError`) that every other component builds against.
- **Redis-backed queue** — `Enqueue`/`Dequeue` via `LPUSH`/`BRPOP`. Blocking pop means no polling loop burning CPU.
- **Worker pool** — fixed number of goroutines pulling and routing jobs to registered `Handler`s by `job.Type`, capping parallelism to protect downstream resources.
- **Retries with exponential backoff** — failed jobs are requeued with `2^attempts` backoff (capped at 30s) or dead-lettered once `MaxAttempts` is hit.
- **Dead-letter queue** — permanently-failed jobs land in a separate Redis list, inspectable/requeueable/purgeable via `cmd/deadletter`.
- **Durable delayed jobs** — retries and scheduled jobs live in a Redis sorted set (score = run-at timestamp), promoted by a standalone `cmd/scheduler` process. Survives worker restarts, unlike an in-memory timer.
- **Postgres persistence** — every job's lifecycle is written to a `job_history` table for durable, queryable audit history alongside Redis's live queue state.
- **Metrics** — Prometheus counters/histogram/gauge on `/metrics`: jobs processed (by type + outcome), handler duration, pending queue depth.
- **Graceful shutdown** — workers stop picking up new jobs on SIGTERM/SIGINT but let an in-flight job finish, bounded by a shutdown timeout.
- **Multi-node ready** — `BRPOP` already distributes work safely across multiple worker processes with no extra code; workers are tagged with a `nodeID` for log attribution across machines. Leader election and queue sharding are known, intentionally unbuilt next steps.

## Running it locally

```bash
# start redis + postgres
docker compose up -d

# run schema against postgres (copy + exec avoids psql needing to be installed locally)
docker cp schema.sql kairos-postgres:/schema.sql
docker exec -it kairos-postgres psql -U kairos -d kairos -f /schema.sql

# terminal 1: start the worker pool (serves metrics on :2112/metrics)
go run ./cmd/worker

# terminal 2: start the scheduler (promotes due delayed/retry jobs)
go run ./cmd/scheduler

# terminal 3: enqueue a test job
go run ./cmd/producer

# inspect dead-lettered jobs
go run ./cmd/deadletter -action=list
go run ./cmd/deadletter -action=requeue -id=<job-uuid>
go run ./cmd/deadletter -action=purge

# stop everything (keeps data)
docker compose down

# stop and wipe all data
docker compose down -v
```

## Requirements

- Go 1.21+
- Docker + Docker Compose (Redis + Postgres)
- github.com/redis/go-redis/v9
- github.com/google/uuid
- github.com/jackc/pgx/v5/pgxpool
- github.com/prometheus/client_golang
  > README.md is ai-generated

## Documentation

- [Contributing guide](CONTRIBUTING.md) — setup, commands, commit conventions
- [Architecture Decision Records](docs/adr/README.md) — the reasoning behind major design choices
