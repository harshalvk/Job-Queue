# ADR 0004: Dead-lettered jobs live in a separate Redis list

## Status

Accepted

## Context

Jobs that exhaust all retry attempts need to go somewhere other than
being silently dropped, but they also shouldn't keep being picked up by
workers as if they were still runnable.

## Decision

Move permanently-failed jobs to a separate Redis list
(`kairos:dead_letter`), distinct from the pending queue, with explicit
list/requeue/purge operations (`cmd/deadletter`).

## Consequences

- Workers never waste cycles retrying something already known to be
  permanently broken, since dead-lettered jobs are structurally outside
  the pending queue.
- Inspecting the dead-letter queue is a "peek" (LRANGE) with no side
  effects, kept separate from mutating operations (LREM on requeue,
  DEL on purge) — makes debugging safe by construction.
- `RequeueDeadLetter` resets `Attempts` to 0 on the assumption that a
  human manually replaying a job wants a fresh set of retries, not an
  immediate re-failure from an already-exhausted counter.
