// Command scheduler polls the delayed queue and promotes due jobs to
// the pending queue.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/harshalvk/kairos/internal/queue"
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
