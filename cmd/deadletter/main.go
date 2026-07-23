// Command producer enqueues a test job onto the queue.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/harshalvk/jobqueue/internal/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	action := flag.String("action", "list", "list | requeue | purge")
	jobID := flag.String("id", "", "job ID (required for requeue)")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := queue.New(rdb)
	ctx := context.Background()

	switch *action {
	case "list":
		jobs, err := q.ListDeadLetter(ctx, 50)
		if err != nil {
			log.Fatal(err)
		}
		for _, job := range jobs {
			fmt.Printf(("id=%s type=%s attempts=%d error=%q\n"), job.ID, job.Type, job.Attempts, job.LastError)
		}
	case "requeue":
		if *jobID == "" {
			log.Fatal("--id required for requeue")
		}
		if err := q.RequeueDeadLetter(ctx, *jobID); err != nil {
			log.Fatal(err)
		}
		fmt.Println("requeued: ", *jobID)
	case "purge":
		if err := q.PurgeDeadLetter(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Println("dead letter queue purged")
	}
}
