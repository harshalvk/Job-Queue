// Package worker implements a concurrent worker pool that dequeues and
// processes jobs, handling retries, dead-lettering, and metrics.
package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/metrics"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/store"
)

// Handler processes a single job. Returning an error means the job failed.
type Handler func(ctx context.Context, j *job.Job) error

// Pool pulls jobs from a Queue and dispatches them to registered
// Handlers, with a fixed number of concurrent workers.
type Pool struct {
	queue       *queue.Queue
	store       *store.Store
	handlers    map[string]Handler
	concurrency int
	nodeID      string
}

// NewWorkerPool creates a WorkerPool with the given concurrency and node
// identifier (used for log attribution across multiple worker processes).
func NewWorkerPool(queue *queue.Queue, store *store.Store, concurrency int, nodeID string) *Pool {
	return &Pool{
		queue:       queue,
		store:       store,
		handlers:    make(map[string]Handler),
		concurrency: concurrency,
		nodeID:      nodeID,
	}
}

// RegisterHandler tells the pool which function handles a given job Type.
func (wp *Pool) RegisterHandler(jobType string, h Handler) {
	wp.handlers[jobType] = h
}

// Start launches concurrency goroutines, each pulling jobs in a loop. When
// ctx is cancelled, workers finish their current job (if any) and exit —
// they do not pick up new jobs. Start blocks until every worker has exited
// or shutdownTimeout elapses, whichever comes first.
func (wp *Pool) Start(ctx context.Context, shutdownTimeout time.Duration) {
	var wg sync.WaitGroup
	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		workerID := i
		go wp.runWorker(ctx, workerID, &wg)
	}

	// wait for either all workers to finish cleanly, or the timeout to expire
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("all workers exited cleanly")
	case <-time.After(shutdownTimeout):
		log.Printf("shutdown timeout (%s) exceeded, some workers may still be mid-job", shutdownTimeout)
	}
}

func (wp *Pool) runWorker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d: shutdown signal received, no longer picking up new jobs", id)
			return
		default:
		}

		job, err := wp.queue.Dequeue(ctx, 5*time.Second)
		if err != nil {
			continue
		}

		wp.process(ctx, id, job)
	}
}

func (wp *Pool) process(ctx context.Context, workerID int, j *job.Job) {
	handler, ok := wp.handlers[j.Type]
	if !ok {
		log.Printf("worker %d: no handler for job type %q, skipping", workerID, j.Type)
		return
	}

	log.Printf("[%s] worker %d: processing job %s (%s), attempt %d/%d", wp.nodeID, workerID, j.ID, j.Type, j.Attempts+1, j.MaxAttempts)

	start := time.Now()
	handleErr := handler(ctx, j)
	metrics.JobDuration.WithLabelValues(j.Type).Observe(time.Since(start).Seconds())

	if handleErr == nil {
		j.Status = job.StatusCompleted
		metrics.JobsProcessed.WithLabelValues(j.Type, "completed").Inc()
		if err := wp.store.RecordStatus(ctx, j); err != nil {
			log.Printf("job %s: failed to recrod completed status: %v", j.ID, err)
		}
		if err := wp.queue.ResolveDependents(ctx, j.ID); err != nil {
			log.Printf("job %s: failed to resolve dependents: %v", j.ID, err)
		}

		log.Printf("[%s] worker %d: job %s completed", wp.nodeID, workerID, j.ID)
		return
	}

	j.Attempts++
	j.LastError = handleErr.Error()
	j.Status = job.StatusFailed
	metrics.JobsProcessed.WithLabelValues(j.Type, "failed").Inc()
	if recError := wp.store.RecordCreated(ctx, j); recError != nil {
		log.Printf("job %s: failed to record failed status: %v", j.ID, recError)
	}
	log.Printf("worker %d: job %s failed: %v", workerID, j.ID, handleErr)

	if j.Attempts >= j.MaxAttempts {
		wp.moveToDeadLetter(ctx, j)
		return
	}

	wp.scheduleRetry(ctx, j)
}

func (wp *Pool) scheduleRetry(ctx context.Context, j *job.Job) {
	delay := backoffDuration(j.Attempts)
	runAt := time.Now().Add(delay)
	j.Status = job.StatusPending

	log.Printf("job %s: scheduling retry at %s (in %s)", j.ID, runAt.Format(time.RFC3339), delay)

	if err := wp.queue.EnqueueDelayed(ctx, j, runAt); err != nil {
		log.Printf("job %s: failed to schedule retry: %v", j.ID, err)
	}
}

func backoffDuration(attempt int) time.Duration {
	base := time.Second
	const maxBackoff = 30 * time.Second

	d := base * time.Duration(1<<uint(attempt-1))
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

func (wp *Pool) moveToDeadLetter(ctx context.Context, j *job.Job) {
	j.Status = job.StatusDeadLetter

	if err := wp.queue.MoveToDeadLetter(ctx, j); err != nil {
		log.Printf("job %s: failed to move to dead letter: %v", j.ID, err)
		return
	}
	metrics.JobsProcessed.WithLabelValues(j.Type, "dead_letter").Inc()
	if err := wp.store.RecordStatus(ctx, j); err != nil {
		log.Printf("job %s: failed to record dead-letter status: %v", j.ID, err)
	}
	if err := wp.queue.CascadeFailDependents(ctx, j.ID); err != nil {
		log.Printf("job %s: failed to cascade-fail dependents: %v", j.ID, err)
	}

	log.Printf("job %s: moved to dead-letter queue after %d attempts", j.ID, j.Attempts)
}
