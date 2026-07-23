# Contributing

## Prerequisites

- Go 1.22+
- Docker + Docker Compose
- [`golangci-lint`](https://golangci-lint.run/welcome/install/)
- [`lefthook`](https://github.com/evilmartians/lefthook)

## Setup

```bash
git clone https://github.com/harshalvk/kairos.git
cd kairos
lefthook install
make docker-up
make migrate
```

## Common commands

Run `make help` for the full list. The ones you'll use most:

| Command              | What it does                                  |
| -------------------- | --------------------------------------------- |
| `make run-worker`    | Start the worker pool                         |
| `make run-producer`  | Enqueue a test job                            |
| `make run-scheduler` | Start the delayed-job scheduler               |
| `make lint`          | Run golangci-lint                             |
| `make fmt`           | Format with goimports                         |
| `make test`          | Run tests (unit + testcontainers integration) |
| `make vuln`          | Check dependencies for known CVEs             |
| `make sec`           | Run gosec security scan                       |

## Before committing

`lefthook` runs `fmt`/`lint`/`vet` automatically on commit, and `test`/`vuln` on push. If a hook fails, fix the issue and re-stage — don't bypass with `--no-verify` unless you have a specific reason (and explain it in the commit message if you do).

## Commit messages

This repo follows [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): description`. Common types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `build`, `ci`. Scope is usually the package touched (`worker`, `queue`, `store`) or `dev`/`db`/`build` for tooling.

## Architectural decisions

Significant design decisions are recorded in [`docs/adr/`](docs/adr/README.md) as ADRs (Context → Decision → Consequences). If you're proposing a change that reverses or significantly alters an existing decision, add a new ADR referencing the one it supersedes rather than just changing code silently.

## Package layout

- `internal/job` — core Job domain model, no dependencies on other packages
- `internal/queue` — Redis-backed queue (pending, dead-letter, delayed)
- `internal/store` — Postgres job history persistence
- `internal/metrics` — Prometheus metrics definitions
- `internal/worker` — worker pool, retry/backoff, dead-letter logic
- `cmd/*` — entrypoints; thin wiring only, no business logic
