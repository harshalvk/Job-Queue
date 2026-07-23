// Package queue implements a Redis-backed job queue: pending, dead-letter,
// and delayed job storage.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/harshalvk/jobqueue/internal/job"
	"github.com/redis/go-redis/v9"
)

const (
	queueKey      = "jobqueue:pending"
	deadLetterKey = "jobqueue:dead_letter"
	delayedKey    = "jobqueue:delayed"
)

var pendingKeys = map[job.Priority]string{
	job.PriorityHigh:    "jobqueue:pending:high",
	job.PriorityDefault: "jobqueue:pending:default",
	job.PriorityLow:     "jobqueue:pending:low",
}

// dequeueOrder defines the prioiryt check order - high checked firs,
// then low last
var dequeueOrder = []job.Priority{job.PriorityHigh, job.PriorityDefault, job.PriorityLow}

func keyFor(p job.Priority) string {
	if key, ok := pendingKeys[p]; ok {
		return key
	}
	return pendingKeys[job.PriorityDefault] // unknown priority falls back to default
}

// Queue wraps a Redis client to provide job enqueue/dequeue operations.
type Queue struct {
	rdb *redis.Client
}

// New creates a Queue backed by the given Redis client.
func New(rdb *redis.Client) *Queue {
	return &Queue{rdb}
}

// Enqueue pushes a job onto the pending queue.
func (q *Queue) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)

	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.rdb.LPush(ctx, keyFor(j.Priority), data).Err()
}

// Dequeue blocks until a job is available, then returns it.
// A timeout of 0 means block forever.
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*job.Job, error) {
	keys := make([]string, len(dequeueOrder))
	for i, p := range dequeueOrder {
		keys[i] = pendingKeys[p]
	}

	result, err := q.rdb.BRPop(ctx, timeout, keys...).Result()

	if err != nil {
		return nil, err
	}

	var j job.Job
	if err := json.Unmarshal([]byte(result[1]), &j); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &j, nil
}

// MoveToDeadLetter stores a permanently-failed job in the dead-letter list.
func (q *Queue) MoveToDeadLetter(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)

	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.rdb.LPush(ctx, deadLetterKey, data).Err()
}

// ListDeadLetter returns up to limit dead-lettered jobs without removing
// them. Pass limit = -1 to return all jobs.
func (q *Queue) ListDeadLetter(ctx context.Context, limit int64) ([]*job.Job, error) {
	stop := limit - 1
	if limit < 0 {
		stop = -1
	}

	raw, err := q.rdb.LRange(ctx, deadLetterKey, 0, stop).Result()

	if err != nil {
		return nil, fmt.Errorf("lrange dead letter: %w", err)
	}

	jobs := make([]*job.Job, 0, len(raw))

	for _, item := range raw {
		var j job.Job

		if err := json.Unmarshal([]byte(item), &j); err != nil {
			return nil, fmt.Errorf("unmarshal dead letter job: %w", err)
		}

		jobs = append(jobs, &j)
	}

	return jobs, nil
}

// RequeueDeadLetter pulls one job off the dead-letter list and re-enqueues
// it, resetting its attempt count so it gets a fresh set of retries.
func (q *Queue) RequeueDeadLetter(ctx context.Context, jobID string) error {
	jobs, err := q.ListDeadLetter(ctx, -1) // -1 -> all

	if err != nil {
		return err
	}

	for _, j := range jobs {
		if j.ID != jobID {
			continue
		}

		// remove the specific job from the dead-letter list
		data, err := json.Marshal(j)
		if err != nil {
			return fmt.Errorf("marshal job for dead-letter removal: %w", err)
		}

		if err := q.rdb.LRem(ctx, deadLetterKey, 1, data).Err(); err != nil {
			return fmt.Errorf("remove from dead letter: %w", err)
		}

		j.Attempts = 0
		j.Status = job.StatusPending
		j.LastError = ""

		return q.Enqueue(ctx, j)
	}

	return fmt.Errorf("job %s not found in dead letter queue", jobID)
}

// PurgeDeadLetter deletes all dead-lettered jobs permanently.
func (q *Queue) PurgeDeadLetter(ctx context.Context) error {
	return q.rdb.Del(ctx, deadLetterKey).Err()
}

// EnqueueDelayed schedules a job to become available at runAt, stored in a
// Redis sorted set keyed by Unix timestamp so it survives process restarts.
func (q *Queue) EnqueueDelayed(ctx context.Context, j *job.Job, runAt time.Time) error {
	j.RunAt = runAt
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	return q.rdb.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(runAt.Unix()),
		Member: data,
	}).Err()
}

// PromoteDueJobs finds jobs in the delayed set whose runAt has passed,
// moves them into the pending queue, and removes them from the delayed
// set. Returns how many jobs were promoted.
func (q *Queue) PromoteDueJobs(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())

	// fetches all delayed jobs with the socre <= now (i.e due to run)
	due, err := q.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     delayedKey,
		Start:   "-inf",
		Stop:    fmt.Sprintf("%f", now),
		ByScore: true,
	}).Result()

	if err != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", err)
	}

	for _, data := range due {
		var j job.Job
		if err := json.Unmarshal([]byte(data), &j); err != nil {
			return 0, fmt.Errorf("unmarshal due job: %w", err)
		}
		if err := q.rdb.LPush(ctx, keyFor(j.Priority), data).Err(); err != nil {
			return 0, fmt.Errorf("push promoted job: %w", err)
		}
		if err := q.rdb.ZRem(ctx, delayedKey, data).Err(); err != nil {
			return 0, fmt.Errorf("remove promoted job: %w", err)
		}
	}

	return len(due), nil
}

// Depth returns the current number of pending jobs.
func (q *Queue) Depth(ctx context.Context, p job.Priority) (int64, error) {
	return q.rdb.LLen(ctx, keyFor(p)).Result()
}

// TotalDepth retuns the sum of pending jobs across all priority levels
func (q *Queue) TotalDepth(ctx context.Context) (int64, error) {
	var total int64
	for _, key := range pendingKeys {
		n, err := q.rdb.LLen(ctx, key).Result()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}
