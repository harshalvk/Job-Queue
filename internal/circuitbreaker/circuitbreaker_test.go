package circuitbreaker_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/harshalvk/kairos/internal/circuitbreaker"
)

func TestBreaker_OpensAfterThreshold(t *testing.T) {
	b := circuitbreaker.New(3, time.Second)

	assert.True(t, b.Allow("send_email"))
	b.RecordFailure("send_email")
	b.RecordFailure("send_email")
	assert.Equal(t, circuitbreaker.StateClosed, b.StateOf("send_email"))

	b.RecordFailure("send_email") // 3rd failure trips it
	assert.Equal(t, circuitbreaker.StateOpen, b.StateOf("send_email"))
	assert.False(t, b.Allow("send_email"))
}

func TestBreaker_HalfOpenAfterCooldownAllowsOneTrial(t *testing.T) {
	b := circuitbreaker.New(1, 50*time.Millisecond)

	b.RecordFailure("send_email") // trips immediately (threshold=1)
	assert.False(t, b.Allow("send_email"))

	time.Sleep(60 * time.Millisecond)

	assert.True(t, b.Allow("send_email")) // cooldown elapsed, trial allowed
	assert.Equal(t, circuitbreaker.StateHalfOpen, b.StateOf("send_email"))

	assert.False(t, b.Allow("send_email")) // a second concurrent trial is blocked
}

func TestBreaker_SuccessInHalfOpenCloses(t *testing.T) {
	b := circuitbreaker.New(1, 10*time.Millisecond)

	b.RecordFailure("send_email")
	time.Sleep(20 * time.Millisecond)
	assert.True(t, b.Allow("send_email")) // enters half-open

	b.RecordSuccess("send_email")
	assert.Equal(t, circuitbreaker.StateClosed, b.StateOf("send_email"))
	assert.True(t, b.Allow("send_email"))
}

func TestBreaker_FailureInHalfOpenReopens(t *testing.T) {
	b := circuitbreaker.New(1, 10*time.Millisecond)

	b.RecordFailure("send_email")
	time.Sleep(20 * time.Millisecond)
	assert.True(t, b.Allow("send_email")) // enters half-open

	b.RecordFailure("send_email") // trial failed
	assert.Equal(t, circuitbreaker.StateOpen, b.StateOf("send_email"))
	assert.False(t, b.Allow("send_email"))
}

func TestBreaker_JobTypesAreIndependent(t *testing.T) {
	b := circuitbreaker.New(1, time.Hour)

	b.RecordFailure("send_email")
	assert.Equal(t, circuitbreaker.StateOpen, b.StateOf("send_email"))
	assert.Equal(t, circuitbreaker.StateClosed, b.StateOf("resize_image"))
	assert.True(t, b.Allow("resize_image"))
}
