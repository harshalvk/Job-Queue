package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/harshalvk/jobqueue"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := jobqueue.NewQueue(rdb)
	ctx := context.Background()

	payload, _ := json.Marshal(map[string]string{"to": "test@example.com"})
	job := jobqueue.NewJob("send_email", payload, 3)

	if err := q.Enqueue(ctx, job); err != nil {
		panic(err)
	}
	fmt.Println("enqueued:", job.ID)
}