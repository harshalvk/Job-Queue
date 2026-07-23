# ADR 0009: Job dependencies via a waiting hash + reverse-dependency index

## Status

Accepted

## Context

Some jobs should only run after other specific jobs succeed (e.g.
resize an image, then notify). The queue had no concept of ordering
or relationships between jobs.

## Decision

Jobs with dependencies are held in a Redis hash (`kairos:waiting`)
instead of the pending queue, with a per-job outstanding-dependency
counter and a reverse index (`kairos:dependents:<id>`) mapping each
dependency to the jobs waiting on it. On successful completion,
ResolveDependents decrements counters for dependents and enqueues any
that reach zero. On permanent failure (dead-letter), CascadeFailDependents
walks the reverse index and dead-letters all transitive dependents too.

## Consequences

- Dependency resolution on completion is O(direct dependents), not a
  scan of all waiting jobs — the reverse index makes "who's waiting on
  me" a direct lookup.
- A permanently failed dependency doesn't leave its dependents stuck
  forever — they're explicitly cascaded to dead-letter with a traceable
  reason (`upstream dependency X failed permanently`).
- This only supports a simple DAG via flat dependsOn lists — no cycle
  detection is implemented. Enqueuing a job with a circular dependency
  (A depends on B, B depends on A) would leave both stuck in the
  waiting hash forever with no error raised. Worth flagging as a known
  gap; a production version would validate the dependency graph is
  acyclic at enqueue time.
- Multi-key writes (hash + counter + set) use TxPipeline for atomicity,
  but ResolveDependents itself is not fully atomic across dependents —
  if the process crashes mid-loop, some dependents may be resolved and
  others not, requiring nothing worse than re-running resolution
  (idempotent per dependent, since HGet returning redis.Nil is treated
  as "already handled").
