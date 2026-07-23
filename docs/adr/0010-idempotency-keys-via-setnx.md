# ADR 0010: Idempotency keys via Redis SETNX, scoped by job type, with a TTL

## Status
Accepted

## Context
A producer retrying Enqueue after a network failure or timeout (without
knowing whether the first attempt actually succeeded) could create
duplicate jobs — problematic for anything with real-world side effects
(charging a card, sending an email).

## Decision
Add an opt-in IdempotencyKey field on Job. EnqueueIdempotent uses
Redis's atomic SETNX to claim a key (scoped by job type) before
enqueuing; a duplicate claim attempt is silently skipped rather than
enqueued again. Claims expire after a caller-supplied TTL.

## Consequences
- SETNX is atomic, so concurrent producers racing to enqueue the same
  key cannot both succeed — exactly one wins, closing the race window
  a check-then-set approach would have.
- Scoping by job type means the same key can be reused for legitimately
  different work (e.g. "order-42" for both a charge job and a
  notification job) without one blocking the other.
- The TTL bounds this to "protects against near-term retries," not
  permanent global uniqueness — a genuinely permanent dedup guarantee
  would need a different mechanism (e.g. a unique constraint in
  Postgres).
- If Enqueue fails after the SETNX claim succeeds, the claim is
  released so a legitimate retry isn't permanently blocked by our own
  transient failure — but this release-on-failure is not itself atomic
  with the claim; a crash between claiming and releasing would leave a
  key claimed with no job actually enqueued, blocking retries until
  the TTL expires. Accepted as a rare edge case, not fully closed.
