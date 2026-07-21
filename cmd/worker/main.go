// Command worker runs the worker pool, processing jobs and serving
// Prometheus metrics.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/harshalvk/jobqueue/internal/job"
	"github.com/harshalvk/jobqueue/internal/metrics"
	"github.com/harshalvk/jobqueue/internal/queue"
	"github.com/harshalvk/jobqueue/internal/store"
	"github.com/harshalvk/jobqueue/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// func sendEmailHandler(ctx context.Context, job *jobqueue.Job) error {
// 	var payload struct {
// 		To string `json:"to"`
// 	}
// 	if err := json.Unmarshal(job.Payload, &payload); err != nil {
// 		return err
// 	}
// 	fmt.Printf("sending email to %s (job %s)\n", payload.To, job.ID)
// 	return nil
// }

// simulated version to fail a job
func sendEmailHandler(_ context.Context, j *job.Job) error {
	time.Sleep(5 * time.Second)
	fmt.Printf("email send for job %s\n", j.ID)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	queue := queue.NewQueue(rdb)

	db, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := store.NewStore(db)

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		nodeID = hostname
	}

	pool := worker.NewWorkerPool(queue, store, 5, nodeID) // 5 concurrent workers
	pool.RegisterHandler("send_email", sendEmailHandler)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("metrics server listening on :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("metrics server error: %v", err)
		}
	}()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				depth, err := queue.Depth(ctx)
				if err != nil {
					continue
				}
				metrics.QueueDepth.Set(float64(depth))
			}
		}
	}()

	fmt.Println("worker pool started, waiting for jobs...")
	pool.Start(ctx, 30*time.Second)
	fmt.Println("worker pool stopped")
}
