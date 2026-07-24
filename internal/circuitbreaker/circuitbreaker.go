// Package circuitbreaker implements a per-job-type circuit breaker that
// temporarily stops attempting a job type after repeadted failures,
// given a struggling downstream dependency room to recover
package circuitbreaker

import (
	"sync"
	"time"
)

// State represents the circurit breaker's current state
type State int

// Possible circuit breaker states
const (
	// StateClosed: normal operations, job are attempted
	StateClosed State = iota
	// StateOpen: failures exceeded the threshold, jobs are rejected
	// without attempting them until the cooldown elapses
	StateOpen
	// StateHalfOpen: cooldown elapsed, allowring one trial job through to
	// test whether the dependency has recovered
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type breakerEntry struct {
	state            State
	consecutiveFails int
	openedAt         time.Time
}

// Breaker manages one circuit breaker per job type
type Breaker struct {
	mu               sync.Mutex
	entries          map[string]*breakerEntry
	failureThreshold int
	cooldown         time.Duration
}

// New creates a Breaker.
// - failureThreshold is the number of consecutive
// failures (for a given job type) that trips the circuit open.
// - cooldown is how long the circuit stays open before allowing a half-open trial
func New(failureThreshold int, cooldown time.Duration) *Breaker {
	return &Breaker{
		entries:          make(map[string]*breakerEntry),
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
	}
}

func (b *Breaker) entryFor(jobType string) *breakerEntry {
	e, ok := b.entries[jobType]
	if !ok {
		e = &breakerEntry{state: StateClosed}
		b.entries[jobType] = e
	}
	return e
}

// Allow reports whether a job of jobType should be attempted right now
// if the circuit is opne and the cooldown has elapsed, it transitions to
// half-open and allows exactly one trial job through
func (b *Breaker) Allow(jobType string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.entryFor(jobType)

	switch e.state {
	case StateClosed:
		return true
	case StateHalfOpen:
		return false // a trial is already in flight, block others until it resolves
	case StateOpen:
		if time.Since(e.openedAt) >= b.cooldown {
			e.state = StateHalfOpen
			return true // this call gets the trial attempt
		}
		return false
	default:
		return true
	}
}

// RecordSuccess reports that a job of jobType completed successfully,
// closing the circuit and resetting is failure count
func (b *Breaker) RecordSuccess(jobType string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.entryFor(jobType)
	e.state = StateClosed
	e.consecutiveFails = 0
}

// RecordFailure reports that a job of jobType failed.
// if this pushes the consecutive failure count to the threshold (or the half-open trial
// itself failed), the circuit opens
func (b *Breaker) RecordFailure(jobType string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.entryFor(jobType)

	if e.state == StateHalfOpen {
		// the trial failed - dependency still not recovered, reopen
		e.state = StateOpen
		e.openedAt = time.Now()
		e.consecutiveFails = b.failureThreshold
		return
	}

	e.consecutiveFails++
	if e.consecutiveFails >= b.failureThreshold {
		e.state = StateOpen
		e.openedAt = time.Now()
	}
}

// StateOf returns the current state of the circuit for jobType, mainly
// useful for metrics/observability
func (b *Breaker) StateOf(jobType string) State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entryFor(jobType).state
}
