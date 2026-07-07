package main

import (
	"context"
	"log"
	"time"

	"github.com/harshalvk/jobqueue"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := jobqueue.NewQueue(rdb)
	ctx := context.Background()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("scheduler started, checking for due jobs every 1s")
	for range ticker.C {
		n, error := q.PromoteDueJobs(ctx)
		if error != nil {
			log.Printf("promote due jobs: %v",error)
			continue
		}
		if n > 0{
			log.Printf("promoted %d due job(s) to pending queue", n)
		}
	}
}