// Package store persists job lifecycle history to Postgres.
package store

import (
	"context"
	"fmt"

	"github.com/harshalvk/jobqueue/internal/job"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store persists job lifecycle history to Postgres. This is separate from
// Queue (Redis) on purpose — Queue answers "what needs to run next", Store
// answers "what happened, historically".
type Store struct {
	db *pgxpool.Pool
}

// NewStore creates a Store backed by the given Postgres connection pool.
func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// RecordCreated inserts a new row when a job is first created.
func (s *Store) RecordCreated(ctx context.Context, j *job.Job) error {
	_, err := s.db.Exec(ctx, `
		 INSERT INTO job_history (id, type, payload, status, attempts, max_attempts, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (id) DO NOTHING
	`, j.ID, j.Type, j.Payload, j.Status, j.Attempts, j.MaxAttempts, j.CreatedAt)

	if err != nil {
		return fmt.Errorf("record created %w", err)
	}

	return nil
}

// RecordStatus updates a job's status, attempts, and last error — called
// after every completion, failure, retry, or dead-letter.
func (s *Store) RecordStatus(ctx context.Context, j *job.Job) error {
	_, err := s.db.Exec(ctx, `
		 UPDATE job_history
		 SET status = $2, attempts = $3, last_error = $4, updated_at = now()
		 WHERE id = $1
	`, j.ID, j.Status, j.Attempts, j.LastError)

	if err != nil {
		return fmt.Errorf("record status: %w", err)
	}

	return nil
}
