// Command scheduler polls the delayed queue and promotes due jobs to
// the pending queue.
package main

import (
	"context"
	"log"
	"time"

	"github.com/harshalvk/jobqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := queue.NewQueue(rdb)
	ctx := context.Background()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("scheduler started, checking for due jobs every 1s")
	for range ticker.C {
		n, err := q.PromoteDueJobs(ctx)
		if err != nil {
			log.Printf("promote due jobs: %v", err)
			continue
		}
		if n > 0 {
			log.Printf("promoted %d due job(s) to pending queue", n)
		}
	}
}
