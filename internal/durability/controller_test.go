package durability

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-store/internal/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTracker struct {
	mu         sync.Mutex
	dirty      bool
	dirtyCount int
	cleanCount int
}

func (m *mockTracker) OnOpen(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dirty, nil
}

func (m *mockTracker) MarkDirty() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty = true
	m.dirtyCount++
}

func (m *mockTracker) MarkClean() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty = false
	m.cleanCount++
}

func (m *mockTracker) Close() error {
	return nil
}

func TestController_OnOpen(t *testing.T) {
	tracker := &mockTracker{dirty: true}
	controller := NewController(Options{
		Sentinel: tracker,
	})

	dirty, err := controller.OnOpen(context.Background())
	require.NoError(t, err)
	assert.True(t, dirty)
}

func TestController_StartStop(t *testing.T) {
	tracker := &mockTracker{}
	flushCalled := false

	controller := NewController(Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 100 * time.Millisecond,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCalled = true
			return nil
		},
	})

	ctx := context.Background()

	err := controller.Start(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, tracker.dirtyCount)

	err = controller.Start(ctx)
	assert.Error(t, err)

	err = controller.Stop()
	require.NoError(t, err)
	assert.False(t, flushCalled)
	assert.Equal(t, 1, tracker.cleanCount)

	err = controller.Stop()
	assert.NoError(t, err)
}

func TestController_IdleFlush(t *testing.T) {
	tracker := &mockTracker{}
	flushCount := 0

	controller := NewController(Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 100 * time.Millisecond,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount++
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.Event{
		Type: driver.EventReleaseWriteWithChanges,
		When: time.Now(),
	})

	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, 1, flushCount)
	assert.Equal(t, 1, tracker.cleanCount)
}

func TestController_RaceConditionWriteDuringFlush(t *testing.T) {
	// Tests that when a write happens while flush is acquiring the connection,
	// the flush is skipped because the system is no longer idle
	tracker := &mockTracker{}
	flushCount := 0
	writeConnAcquired := make(chan struct{}, 1)
	writeConnReleased := make(chan struct{})
	flushAttempted := false

	controller := NewController(Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 200 * time.Millisecond,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			select {
			case writeConnAcquired <- struct{}{}:
				<-writeConnReleased
			default:
			}
			flushAttempted = true
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount++
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)

	initialWriteTime := time.Now().Add(-300 * time.Millisecond)
	controller.OnWriteEvent(driver.Event{
		Type: driver.EventReleaseWriteWithChanges,
		When: initialWriteTime,
	})

	<-writeConnAcquired

	newWriteTime := time.Now().Add(-50 * time.Millisecond)
	controller.OnWriteEvent(driver.Event{
		Type: driver.EventReleaseWriteWithChanges,
		When: newWriteTime,
	})

	close(writeConnReleased)
	time.Sleep(50 * time.Millisecond)

	assert.True(t, flushAttempted, "Flush should have been attempted")
	assert.Equal(t, 0, flushCount, "Flush should be skipped when write happens during acquire")
	assert.Equal(t, 0, tracker.cleanCount, "Should not mark clean when flush is skipped")

	controller.Stop()
}

func TestController_FlushAfterWriteDelay(t *testing.T) {
	// Tests that flush is skipped when a write happens during acquire,
	// but succeeds on the next attempt when system becomes idle
	tracker := &mockTracker{}
	flushCount := 0
	acquireStarted := make(chan struct{})
	proceedWithAcquire := make(chan struct{})

	controller := NewController(Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 200 * time.Millisecond,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			select {
			case acquireStarted <- struct{}{}:
			default:
			}
			select {
			case <-proceedWithAcquire:
			case <-ctx.Done():
				return ctx.Err()
			}
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount++
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.Event{
		Type: driver.EventReleaseWriteWithChanges,
		When: time.Now().Add(-300 * time.Millisecond),
	})

	<-acquireStarted

	controller.OnWriteEvent(driver.Event{
		Type: driver.EventReleaseWriteWithChanges,
		When: time.Now().Add(-50 * time.Millisecond),
	})

	close(proceedWithAcquire)
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 0, flushCount, "Flush should be skipped when write happens during acquire")
	assert.Equal(t, 0, tracker.cleanCount, "Should not mark clean when flush is skipped")

	time.Sleep(250 * time.Millisecond)

	assert.Equal(t, 1, flushCount, "Flush should eventually succeed when truly idle")
	assert.Equal(t, 1, tracker.cleanCount, "Should mark clean after successful flush")
}

func TestController_MultipleWritesDuringFlush(t *testing.T) {
	tracker := &mockTracker{}
	flushCount := 0
	writeConnAcquired := make(chan struct{}, 1)
	writeConnReleased := make(chan struct{})

	controller := NewController(Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 100 * time.Millisecond,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			select {
			case writeConnAcquired <- struct{}{}:
				<-writeConnReleased
			default:
			}
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount++
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.Event{
		Type: driver.EventReleaseWriteWithChanges,
		When: time.Now().Add(-200 * time.Millisecond),
	})

	<-writeConnAcquired

	// Simulate multiple writes with decreasing age (20ms, 18ms, 16ms, 14ms, 12ms ago)
	// to ensure at least one write is recent enough to skip the flush
	for i := 0; i < 5; i++ {
		controller.OnWriteEvent(driver.Event{
			Type: driver.EventReleaseWriteWithChanges,
			When: time.Now().Add(-time.Duration(20-i*2) * time.Millisecond),
		})
		time.Sleep(2 * time.Millisecond)
	}

	close(writeConnReleased)
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 0, flushCount, "Flush should be skipped due to recent writes")
	assert.Equal(t, 0, tracker.cleanCount, "Should not mark clean when flush is skipped")
}

func TestController_FlushDoesNotResetWriteTime(t *testing.T) {
	// Tests that flush operations do not trigger write events,
	// preventing an infinite loop where flush retries would keep resetting lastWriteTime
	tracker := &mockTracker{}
	flushCount := 0
	acquireCount := 0

	var controller *Controller

	opts := Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 10 * time.Second,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			acquireCount++
			err := fn(nil)
			// Only trigger write events for non-silent acquires (simulates real behavior)
			if controller != nil {
				controller.OnWriteEvent(driver.Event{
					Type: driver.EventReleaseWriteWithoutChanges,
					When: time.Now(),
				})
			}
			return err
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount++
			return nil
		},
	}

	controller = NewController(opts)

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.Event{
		Type: driver.EventReleaseWriteWithChanges,
		When: time.Now(),
	})

	time.Sleep(60 * time.Millisecond)

	flushed, err := controller.performFlushInternal(ctx, 50*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, flushed, "Should have flushed")
	assert.Equal(t, 1, flushCount, "Should have flushed once")
	assert.Equal(t, 1, acquireCount, "Should have acquired once")
}
