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

const deadLetterKey = "jobqueue:dead_letter"

// this function stores a permanently-faiiled job in the dead-letter list
func (q *Queue) MoveToDeadLetter(ctx context.Context, job *Job) error {
	data, error := json.Marshal(job)

	if error != nil {
		return fmt.Errorf("marshal job: %w", error)
	}

	return q.rdb.LPush(ctx, deadLetterKey, data).Err()
}

// this function returns up to `limit` dead-lettered jobs without removing them
func (q *Queue) ListDeadLetter(ctx context.Context, limit int64) ([]*Job, error){
	stop := limit - 1
	if limit < 0 {
		stop = -1
	}

	raw, error := q.rdb.LRange(ctx, deadLetterKey, 0, stop).Result()

	if error != nil {
		return nil, fmt.Errorf("lrange dead letter: %w", error)
	}

	jobs := make([]*Job, 0, len(raw))

	for _, item := range raw {
		var job Job
		
		if error := json.Unmarshal([]byte(item), &job); error != nil {
			return nil, fmt.Errorf("unmarshal dead letter job: %w", error)
		}

		jobs = append(jobs, &job)
	}

	return jobs, nil
}

func (q *Queue) RequeueDeadLetter(ctx context.Context, jobID string) error {
	jobs, error := q.ListDeadLetter(ctx, -1) // -1 -> all

	if error != nil  {
		return error
	}

	for _, job := range jobs {
		if job.ID != jobID {
			continue
		}

		// remove the specific job from the dead-letter list
		data, _ := json.Marshal(job)

		if error := q.rdb.LRem(ctx, deadLetterKey, 1, data).Err(); error != nil {
			return fmt.Errorf("remove from dead letter: %w", error)
		}

		job.Attempts = 0
		job.Status = StatusPending
		job.LastError = ""

		return q.Enqueue(ctx, job)
	}

	return fmt.Errorf("job %s not found in dead letter queue", jobID)
}

// this deletes all dead-lettered jobs permanently
func (q *Queue) PurgeDeadLetter(ctx context.Context) error {
	return q.rdb.Del(ctx, deadLetterKey).Err()
}