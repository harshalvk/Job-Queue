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
	handlers map[string]Handler
	concurrency int
}

func NewWorkerPool(queue *Queue, concurrency int) *WorkerPool {
	return &WorkerPool{
		queue: queue,
		handlers: make(map[string]Handler),
		concurrency: concurrency,
	}
}

func (wp *WorkerPool) RegisterHandler(jobType string, h Handler){
	wp.handlers[jobType] = h
}

func (wp *WorkerPool) Start(ctx context.Context){
	var wg sync.WaitGroup
	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		workerID := i
		go wp.runWorker(ctx, workerID, &wg)
	}
	wg.Wait()
}

func (wp *WorkerPool) runWorker(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d: shutting down", id)
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

	log.Printf("worker %d: processing job %s (%s)", workerID, job.ID, job.Type)

	error := handler(ctx, job)

	if error == nil {
		log.Printf("worker %d: job %s completed", workerID, job.ID)
		return
	}

	job.Attempts++
	job.LastError = error.Error()
	log.Printf("worker %d: job %s failed: %v", workerID, job.ID, error)

	if job.Attempts >= job.MaxAttempts {
		wp.moveToDeadLetter(ctx, job)
		return
	}

	wp.scheduleRetry(ctx, job)
}

func (wp *WorkerPool) scheduleRetry(ctx context.Context, job *Job){
	delay := backoffDuration(job.Attempts)
	log.Printf("job %s: retrying in %s", job.ID, &delay)

	go func(){
		select {
		case <-time.After(delay):
			job.Status = StatusPending
			if error := wp.queue.Enqueue(ctx, job); error != nil {
				log.Printf("job %s: failed to requeue: %v", job.ID, error)
			}
		case <-ctx.Done():
			log.Printf("job %s: retry cancelled due to shutdown", job.ID)
		}
	}()
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

	log.Printf("job %s: moved to dead-letter queue after %d attempts", job.ID, job.Attempts)
}