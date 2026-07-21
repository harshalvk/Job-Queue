// Package job defines the core Job domain model shared across the queue,
// worker, and store packages.
package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status represents the current lifecycle state of a Job.
type Status string

// Possible values for JobStatus.
const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
)

// Job represents a single unit of work to be processed by a worker.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      Status          `json:"status"`
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
