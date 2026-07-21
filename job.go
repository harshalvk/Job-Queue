// Package jobqueue implements a distributed job queue backed by Redis,
// with worker pools, retries, dead-lettering, and Postgres-backed history.
package jobqueue

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// JobStatus represents the current lifecycle state of a Job.
type JobStatus string

// Possible values for JobStatus.
const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusDeadLetter JobStatus = "dead_letter"
)

// Job represents a single unit of work to be processed by a worker.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      JobStatus       `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	RunAt       time.Time       `json:"run_at"`
	LastError   string          `json:"last_error,omitempty"`
}

// NewJob creates a new Job with a generated UUID and pending status.
func NewJob(jobType string, payload json.RawMessage, maxAttempts int) *Job {
	return &Job{
		ID:          uuid.NewString(),
		Type:        jobType,
		Payload:     payload,
		Status:      StatusPending,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
		RunAt:       time.Now(),
	}
}
