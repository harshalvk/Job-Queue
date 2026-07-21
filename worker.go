package jobqueue

import (
	"context"
	"log"
	"sync"
	"time"
)

// Handler processes a single job. Returning an error means the job failed.
type Handler func(ctx context.Context, job *Job) error

// WorkerPool pulls jobs from a Queue and dispatches them to registered
// Handlers, with a fixed number of concurrent workers.
type WorkerPool struct {
	queue       *Queue
	store       *Store
	handlers    map[string]Handler
	concurrency int
	nodeID      string
}

// NewWorkerPool creates a WorkerPool with the given concurrency and node
// identifier (used for log attribution across multiple worker processes).
func NewWorkerPool(queue *Queue, store *Store, concurrency int, nodeID string) *WorkerPool {
	return &WorkerPool{
		queue:       queue,
		store:       store,
		handlers:    make(map[string]Handler),
		concurrency: concurrency,
		nodeID:      nodeID,
	}
}

// RegisterHandler tells the pool which function handles a given job Type.
func (wp *WorkerPool) RegisterHandler(jobType string, h Handler) {
	wp.handlers[jobType] = h
}

// Start launches concurrency goroutines, each pulling jobs in a loop. When
// ctx is cancelled, workers finish their current job (if any) and exit —
// they do not pick up new jobs. Start blocks until every worker has exited
// or shutdownTimeout elapses, whichever comes first.
func (wp *WorkerPool) Start(ctx context.Context, shutdownTimeout time.Duration) {
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

func (wp *WorkerPool) runWorker(ctx context.Context, id int, wg *sync.WaitGroup) {
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

func (wp *WorkerPool) process(ctx context.Context, workerID int, job *Job) {
	handler, ok := wp.handlers[job.Type]
	if !ok {
		log.Printf("worker %d: no handler for job type %q, skipping", workerID, job.Type)
		return
	}

	log.Printf("[%s] worker %d: processing job %s (%s), attempt %d/%d", wp.nodeID, workerID, job.ID, job.Type, job.Attempts+1, job.MaxAttempts)

	start := time.Now()
	handleErr := handler(ctx, job)
	JobDuration.WithLabelValues(job.Type).Observe(time.Since(start).Seconds())

	if handleErr == nil {
		job.Status = StatusCompleted
		JobsProcessed.WithLabelValues(job.Type, "completed").Inc()
		if err := wp.store.RecordStatus(ctx, job); err != nil {
			log.Printf("job %s: failed to recrod completed status: %v", job.ID, err)
		}

		log.Printf("worker %d: job %s completed", workerID, job.ID)
		return
	}

	job.Attempts++
	job.LastError = handleErr.Error()
	job.Status = StatusFailed
	JobsProcessed.WithLabelValues(job.Type, "failed").Inc()
	if recError := wp.store.RecordCreated(ctx, job); recError != nil {
		log.Printf("job %s: failed to record failed status: %v", job.ID, recError)
	}
	log.Printf("worker %d: job %s failed: %v", workerID, job.ID, handleErr)

	if job.Attempts >= job.MaxAttempts {
		wp.moveToDeadLetter(ctx, job)
		return
	}

	wp.scheduleRetry(ctx, job)
}

func (wp *WorkerPool) scheduleRetry(ctx context.Context, job *Job) {
	delay := backoffDuration(job.Attempts)
	runAt := time.Now().Add(delay)
	job.Status = StatusPending

	log.Printf("job %s: scheduling retry at %s (in %s)", job.ID, runAt.Format(time.RFC3339), delay)

	if err := wp.queue.EnqueueDelayed(ctx, job, runAt); err != nil {
		log.Printf("job %s: failed to schedule retry: %v", job.ID, err)
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

func (wp *WorkerPool) moveToDeadLetter(ctx context.Context, job *Job) {
	job.Status = StatusDeadLetter

	if err := wp.queue.MoveToDeadLetter(ctx, job); err != nil {
		log.Printf("job %s: failed to move to dead letter: %v", job.ID, err)
		return
	}
	JobsProcessed.WithLabelValues(job.Type, "dead_letter").Inc()
	if err := wp.store.RecordStatus(ctx, job); err != nil {
		log.Printf("job %s: failed to record dead-letter status: %v", job.ID, err)
	}

	log.Printf("job %s: moved to dead-letter queue after %d attempts", job.ID, job.Attempts)
}
