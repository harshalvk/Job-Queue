package jobqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const queueKey = "jobqueue:pending"
const delayedKey = "jobqueue:delayed"

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

// this function schedules a job to become available at runAt
// stored in a redis sorted set, score = unix timestamp, so that it survives
// proces restarts (unlike an in-memory goroutine timer)
func (q *Queue) EnqueueDelayed(ctx context.Context, job *Job, runAt time.Time) error {
	job.RunAt = runAt
	data, error := json.Marshal(job)
	if error != nil {
		return fmt.Errorf("marshal job: %w", error)
	}

	return q.rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score: float64(runAt.Unix()),
		Member: data,
	}).Err()
}

// PromoteDueJobs finds jobs in the delayed set whose runAt has passed,
// moves them into the pending queue, and removes them from the delayed set
// returns how many jobs were promoted
func (q *Queue) PromoteDueJobs(ctx context.Context) (int, error){
	now := float64(time.Now().Unix())

	// fetches all delayed jobs with the socre <= now (i.e due to run)
	due, error := q.rdb.ZRangeByScore(ctx, delayedKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
	}).Result()

	if error != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", error)
	}

	for _, data := range due {
		// move atomatially (in a i think semi-automatic-way): push to pending, then remove from delayed
		if error := q.rdb.LPush(ctx, queueKey, data).Err(); error != nil {
			return 0, fmt.Errorf("push promoted job: %w", error)
		}
		if error := q.rdb.ZRem(ctx, delayedKey, data).Err(); error != nil {
			return 0, fmt.Errorf("remove promoted job: %w", error)
		}
	}

	return len(due), nil
}

func (q *Queue) Depth(ctx context.Context) (int64, error){
	return q.rdb.LLen(ctx, queueKey).Result()
}