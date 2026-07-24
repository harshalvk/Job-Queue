# ADR 0013: Per-job-type circuit breaker with closed/open/half-open states

## Status
Accepted

## Context
Retries with exponential backoff (ADR 0003) handle transient failures
well, but do nothing to stop a job type from repeatedly attempting
against a dependency that is genuinely down for an extended period —
every job still burns through its full retry budget before
dead-lettering, adding continued load to an already-struggling
dependency.

## Decision
Add a circuitbreaker package tracking consecutive failures per job
type. After failureThreshold consecutive failures, the circuit opens
and jobs of that type are deferred (rescheduled via the existing
delayed-job mechanism, not counted against MaxAttempts) without being
attempted. After a cooldown, exactly one half-open trial job is
allowed through; success closes the circuit, failure reopens it.

## Consequences
- Reduces load on a struggling dependency during an outage — jobs
  don't hammer it while it's known to be down.
- Jobs skipped due to an open circuit don't consume a retry attempt,
  since the job itself never actually ran — only genuine execution
  failures count against MaxAttempts.
- State is in-memory and per-process, same limitation as ratelimit
  (ADR 0011) — multiple worker nodes each track circuit state
  independently, so one node's circuit opening doesn't stop other
  nodes from continuing to attempt that job type. A shared,
  Redis-backed circuit state would close this gap but wasn't
  implemented, matching the same reasoning as ADR 0011.
- Half-open explicitly allows only one trial at a time (via state
  transition, not a separate lock) — tested explicitly since this is
  the most timing-sensitive part of the implementation.
