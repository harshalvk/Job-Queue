// Command cron runs recurring job defintions on their configured cron
// schedules, enqueuing a fresh job instance each time on fires
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/robfig/cron/v3"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	q := queue.New(rdb)

	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		pgDSN = "postgres://kairos:kairos@localhost:5432/kairos"
	}
	db, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := scheduler.NewStore(db)

	recurringJobs, err := store.ListEnabled(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("loaded %d enabled recurring job(s)", len(recurringJobs))

	c := cron.New(cron.WithSeconds())

	for _, rj := range recurringJobs {
		rj := rj // capture loop varialbe for the closure below
		_, err := c.AddFunc(rj.CronExpr, func() {
			j := job.New(rj.JobType, rj.Payload, rj.MaxAttempts)
			if err := q.Enqueue(ctx, j); err != nil {
				log.Printf("recurring job %q: failed to enqueue: %v", rj.Name, err)
				return
			}
			if err := store.RecordRun(ctx, rj.ID, j.CreatedAt); err != nil {
				log.Printf("recurring job %q: failed to record run: %v", rj.Name, err)
			}
			log.Printf("recurring job %q fired: enqueued job %s", rj.Name, j.ID)
		})
		if err != nil {
			log.Printf("recurring job %q: invalid cron expression %q: %v", rj.Name, rj.CronExpr, err)
			continue
		}
	}

	c.Start()
	log.Printf("cron scheduler started")

	<-ctx.Done()
	log.Printf("shutting down cron scheduler...")
	stopCtx := c.Stop() // stops accepting new triggers, waits for running jobs
	<-stopCtx.Done()
	log.Printf("cron scheduler stopped")
}
