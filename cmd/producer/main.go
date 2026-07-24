// Command producer enqueues a test job onto the queue.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/harshalvk/kairos/internal/job"
	"github.com/harshalvk/kairos/internal/queue"
	"github.com/harshalvk/kairos/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	q := queue.New(rdb)
	ctx := context.Background()

	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN == "" {
		pgDSN = "postgres://kairos:kairos@localhost:5432/kairos"
	}
	db, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := store.NewStore(db)

	payload, err := json.Marshal(map[string]string{"to": "devwork2004@gmail.com"})
	if err != nil {
		log.Fatalf("failed to marshal payload: %v", err)
	}
	job := job.NewWithPriority("send_email", payload, 3, job.PriorityHigh)

	if err := q.Enqueue(ctx, job); err != nil {
		panic(err)
	}
	if err := store.RecordCreated(ctx, job); err != nil {
		panic(err)
	}
	fmt.Println("enqueued:", job.ID)
}
