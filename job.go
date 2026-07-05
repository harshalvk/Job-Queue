package jobqueue

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusDeadLetter JobStatus = "dead_letter"
)

type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      JobStatus       `json:"status"`
	Attempts    int             `json:"attempts"` 
	MaxAttempts int             `json:"max_attempts`
	CreatedAt   time.Time       `json:"created_at"`
	RunAt       time.Time       `json:"run_at"`
	LastError   string	        `json:"last_error,omitempty"`
}

func NewJob(jobType string, payload json.RawMessage, maxAttempts int) * Job {
	return &Job{
		ID: uuid.NewString(),
		Type: jobType,
		Payload: payload,
		Status: StatusPending,
		Attempts: 0,
		MaxAttempts: maxAttempts,
		CreatedAt: time.Now(),
		RunAt: time.Now(),
	}
} 