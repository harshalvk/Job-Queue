package jobqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const queueKey = "jobqueue:pending"

type Queue struct {
	rdb *redis.Client
}

func NewQueue(rdb *redis.Client) * Queue {
	return &Queue{rdb}
}

func (q *Queue) Enqueue(ctx context.Context, job *Job) error {
	data, error := json.Marshal(job)

	if error != nil {
		return fmt.Errorf("marshal job: %w", error)
	}

	return q.rdb.LPush(ctx, queueKey, data).Err() 
} 

func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*Job, error) {
	result, error := q.rdb.BRPop(ctx, timeout, queueKey).Result()

	if error != nil {
		return nil, error
	}

	var job Job
	if error := json.Unmarshal([]byte(result[1]), &job); error != nil {
		return nil, fmt.Errorf("unmarshal job: %w", error)
	}

	return &job, nil
}