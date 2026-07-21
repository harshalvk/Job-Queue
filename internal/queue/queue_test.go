package queue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/harshalvk/jobqueue/internal/job"
	"github.com/harshalvk/jobqueue/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// setupRedis starts a real Redis container for the duration of the test
// and returns a connected client. testContainers handles teardown via
// t.Cleanup, so test never leak containers even on failure
func setupRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	opts, err := redis.ParseURL(connStr)
	require.NoError(t, err)

	return redis.NewClient(opts)
}

func TestEnqueueDequeue(t *testing.T) {
	rdb := setupRedis(t)
	q := queue.NewQueue(rdb)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.NewJob("send_email", payload, 3)

	require.NoError(t, q.Enqueue(ctx, j))

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)

	assert.Equal(t, j.ID, got.ID)
	assert.Equal(t, j.Type, got.Type)
	assert.Equal(t, job.StatusPending, got.Status)
}

func TestDequeue_TimesOutWhenEmpty(t *testing.T) {
	rdb := setupRedis(t)
	q := queue.NewQueue(rdb)
	ctx := context.Background()

	_, err := q.Dequeue(ctx, 1*time.Second)
	assert.ErrorIs(t, err, redis.Nil)
}

func TestDeadLetter_MoveListRequeue(t *testing.T) {
	rdb := setupRedis(t)
	q := queue.NewQueue(rdb)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)
	j := job.NewJob("send_email", payload, 3)
	j.Attempts = 3
	j.LastError = "simulated failure"

	require.NoError(t, q.MoveToDeadLetter(ctx, j))

	jobs, err := q.ListDeadLetter(ctx, 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, j.ID, jobs[0].ID)

	require.NoError(t, q.RequeueDeadLetter(ctx, j.ID))

	// after requeue, dead letter should be empty and pending should have it
	jobs, err = q.ListDeadLetter(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, jobs)

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
	assert.Equal(t, 0, got.Attempts) // confirms attempts was reset on requeue
}

func TestDelayedJobs_PromoteDueJobs(t *testing.T) {
	rdb := setupRedis(t)
	q := queue.NewQueue(rdb)
	ctx := context.Background()

	payload, err := json.Marshal(map[string]string{"to": "test@example.com"})
	require.NoError(t, err)

	dueJob := job.NewJob("send_email", payload, 3)
	futureJob := job.NewJob("send_email", payload, 3)

	// one job due in the past, one due far in the future
	require.NoError(t, q.EnqueueDelayed(ctx, dueJob, time.Now().Add(-1*time.Second)))
	require.NoError(t, q.EnqueueDelayed(ctx, futureJob, time.Now().Add(1*time.Hour)))

	promoted, err := q.PromoteDueJobs(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, promoted)

	got, err := q.Dequeue(ctx, 2*time.Second)
	require.NoError(t, err)
	assert.Equal(t, dueJob.ID, got.ID)

	// future job should still not be in pending
	_, err = q.Dequeue(ctx, 1*time.Second)
	assert.ErrorIs(t, err, redis.Nil)
}
