// Command producer enqueues a test job onto the queue.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/harshalvk/jobqueue"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := jobqueue.NewQueue(rdb)
	ctx := context.Background()

	db, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := jobqueue.NewStore(db)

	payload, err := json.Marshal(map[string]string{"to": "devwork2004@gmail.com"})
	if err != nil {
		log.Fatalf("failed to marshal payload: %v", err)
	}
	job := jobqueue.NewJob("send_email", payload, 3)

	if err := q.Enqueue(ctx, job); err != nil {
		panic(err)
	}
	if err := store.RecordCreated(ctx, job); err != nil {
		panic(err)
	}
	fmt.Println("enqueued:", job.ID)
}
