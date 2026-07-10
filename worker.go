package jobqueue

import (
	"context"
	"log"
	"sync"
	"time"
)

type Handler func(ctx context.Context, job *Job) error

type WorkerPool struct {
	queue *Queue
	store *Store
	handlers map[string]Handler
	concurrency int
	nodeID string
}

func NewWorkerPool(queue *Queue, store *Store, concurrency int, nodeID string) *WorkerPool {
	return &WorkerPool{
		queue: queue,
		store: store,
		handlers: make(map[string]Handler),
		concurrency: concurrency,
		nodeID: nodeID,
	}
}

func (wp *WorkerPool) RegisterHandler(jobType string, h Handler){
	wp.handlers[jobType] = h
}

func (wp *WorkerPool) Start(ctx context.Context, shutdownTimeout time.Duration){
	var wg sync.WaitGroup
	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		workerID := i
		go wp.runWorker(ctx, workerID, &wg)
	}

	// wait for either all workers to finish cleanly, or the timeout to expire
	done := make(chan struct{})
	go func ()  {	
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

		job, error := wp.queue.Dequeue(ctx, 5*time.Second)
		if error != nil {
			continue
		}

		wp.process(ctx, id, job)
	}
}

func (wp *WorkerPool) process(ctx context.Context, workerID int, job *Job){
	handler, ok := wp.handlers[job.Type]
	if !ok {
		log.Printf("worker %d: no handler for job type %q, skipping", workerID, job.Type)
		return
	}

	log.Printf("[%s] worker %d: processing job %s (%s), attempt %d/%d", wp.nodeID, workerID, job.ID, job.Type, job.Attempts+1, job.MaxAttempts)

	start := time.Now()
	err := handler(ctx, job)
	JobDuration.WithLabelValues(job.Type).Observe(time.Since(start).Seconds())

	if err == nil {
		job.Status = StatusCompleted
		JobsProcessed.WithLabelValues(job.Type, "completed").Inc()
		if err := wp.store.RecordStatus(ctx, job); err != nil {
			log.Printf("job %s: failed to recrod completed status: %v", job.ID, err)
		}

		log.Printf("worker %d: job %s completed", workerID, job.ID)
		return
	}

	job.Attempts++
	job.LastError = err.Error()
	job.Status = StatusFailed
	JobsProcessed.WithLabelValues(job.Type, "failed").Inc()
	if recError := wp.store.RecordCreated(ctx, job); recError != nil {
		log.Printf("job %s: failed to record failed status: %v", job.ID, recError)
	}
	log.Printf("worker %d: job %s failed: %v", workerID, job.ID, err)

	if job.Attempts >= job.MaxAttempts {
		wp.moveToDeadLetter(ctx, job)
		return
	}

	wp.scheduleRetry(ctx, job)
}

func (wp *WorkerPool) scheduleRetry(ctx context.Context, job *Job){
	delay := backoffDuration(job.Attempts)
	runAt := time.Now().Add(delay)
	job.Status = StatusPending

	log.Printf("job %s: scheduling retry at %s (in %s)", job.ID, runAt.Format(time.RFC3339), delay)

	if error := wp.queue.EnqueueDelayed(ctx, job, runAt); error != nil {
		log.Printf("job %s: failed to schedule retry: %v", job.ID, error)
	}
}

func backoffDuration(attempt int) time.Duration{
	base := time.Second
	max := 30 * time.Second

	d := base * time.Duration(1<<uint(attempt-1))
	if d > max {
		d = max
	}
	return d
}

func (wp *WorkerPool) moveToDeadLetter(ctx context.Context, job *Job){
	job.Status = StatusDeadLetter

	if error := wp.queue.MoveToDeadLetter(ctx, job); error != nil {
		log.Printf("job %s: failed to move to dead letter: %v", job.ID, error)
		return
	}
	JobsProcessed.WithLabelValues(job.Type, "dead_letter").Inc()
	if error := wp.store.RecordStatus(ctx, job); error != nil {
		log.Printf("job %s: failed to record dead-letter status: %v", job.ID, error)
	}

	log.Printf("job %s: moved to dead-letter queue after %d attempts", job.ID, job.Attempts)
}

