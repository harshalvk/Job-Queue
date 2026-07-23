# ADR 0008: Priority queues via multiple Redis lists, checked in BRPOP order

## Status
Accepted

## Context
All jobs previously shared one pending queue, so urgent jobs could be
stuck behind a backlog of low-priority ones with no way to jump ahead.

## Decision
Use three separate Redis lists (`pending:high`, `pending:default`,
`pending:low`), and a single BRPOP call listing all three keys in
priority order. Redis's multi-key BRPOP checks keys left-to-right and
returns from the first non-empty one atomically.

## Consequences
- Priority is enforced without polling or races — a single atomic Redis
  operation, not three separate dequeue attempts.
- Adding a new priority level (e.g. "critical") is a small, contained
  change: one more key, one more entry in `dequeueOrder`.
- Trade-off: a sustained flood of high-priority jobs can starve
  default/low priority jobs indefinitely, since high is always checked
  first with no fairness mechanism. Acceptable for now; a weighted or
  round-robin dequeue would be the fix if starvation becomes a real
  problem.
