package durability

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/internal/driver"
)

type mockTracker struct {
	mu         sync.Mutex
	dirty      atomic.Bool
	dirtyCount atomic.Int32
	cleanCount atomic.Int32
}

func (m *mockTracker) OnOpen(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dirty.Load(), nil
}

func (m *mockTracker) MarkDirty() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty.Store(true)
	m.dirtyCount.Add(1)
}

func (m *mockTracker) MarkClean() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty.Store(false)
	m.cleanCount.Add(1)
}

func (m *mockTracker) Close() error {
	return nil
}

func TestController_OnOpen(t *testing.T) {
	tracker := &mockTracker{}
	tracker.dirty.Store(true)
	controller := NewController(Options{
		Sentinel: tracker,
	})

	dirty, err := controller.OnOpen(context.Background())
	require.NoError(t, err)
	assert.True(t, dirty)
}

func TestController_StartStop(t *testing.T) {
	tracker := &mockTracker{}
	var flushCalled atomic.Bool

	controller := NewController(Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 100 * time.Millisecond,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCalled.Store(true)
			return nil
		},
	})

	ctx := context.Background()

	err := controller.Start(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(0), tracker.dirtyCount.Load(), "Should not mark dirty on Start()")

	err = controller.Start(ctx)
	assert.Error(t, err)

	err = controller.Stop()
	require.NoError(t, err)
	assert.False(t, flushCalled.Load())
	assert.Equal(t, int32(1), tracker.cleanCount.Load())

	err = controller.Stop()
	assert.NoError(t, err)
}

func TestController_IdleFlush(t *testing.T) {
	tracker := &mockTracker{}
	var flushCount atomic.Int32

	controller := NewController(Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 100 * time.Millisecond,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount.Add(1)
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)

	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, int32(1), flushCount.Load())
	assert.Equal(t, int32(1), tracker.cleanCount.Load())
}

func TestController_RaceConditionWriteDuringFlush(t *testing.T) {
	// Tests that when a write happens while flush is acquiring the connection,
	// the flush is skipped because the system is no longer idle
	tracker := &mockTracker{}
	var flushCount atomic.Int32
	writeConnAcquired := make(chan struct{}, 1)
	writeConnReleased := make(chan struct{})
	var flushAttempted atomic.Bool

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
			flushAttempted.Store(true)
			return fn(nil)
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount.Add(1)
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)

	//initialWriteTime := time.Now().Add(-300 * time.Millisecond)
	controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)

	<-writeConnAcquired

	//newWriteTime := time.Now().Add(-50 * time.Millisecond)
	controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)

	close(writeConnReleased)
	time.Sleep(50 * time.Millisecond)

	assert.True(t, flushAttempted.Load(), "Flush should have been attempted")
	assert.Equal(t, int32(0), flushCount.Load(), "Flush should be skipped when write happens during acquire")
	assert.Equal(t, int32(0), tracker.cleanCount.Load(), "Should not mark clean when flush is skipped")

	controller.Stop()
}

func TestController_FlushAfterWriteDelay(t *testing.T) {
	// Tests that flush is skipped when a write happens during acquire,
	// but succeeds on the next attempt when system becomes idle
	tracker := &mockTracker{}
	var flushCount atomic.Int32
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
			flushCount.Add(1)
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)

	<-acquireStarted

	controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)

	close(proceedWithAcquire)
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int32(0), flushCount.Load(), "Flush should be skipped when write happens during acquire")
	assert.Equal(t, int32(0), tracker.cleanCount.Load(), "Should not mark clean when flush is skipped")

	time.Sleep(250 * time.Millisecond)

	assert.Equal(t, int32(1), flushCount.Load(), "Flush should eventually succeed when truly idle")
	assert.Equal(t, int32(1), tracker.cleanCount.Load(), "Should mark clean after successful flush")
}

func TestController_MultipleWritesDuringFlush(t *testing.T) {
	tracker := &mockTracker{}
	var flushCount atomic.Int32
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
			flushCount.Add(1)
			return nil
		},
	})

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)

	<-writeConnAcquired

	// Simulate multiple writes with decreasing age (20ms, 18ms, 16ms, 14ms, 12ms ago)
	// to ensure at least one write is recent enough to skip the flush
	for i := 0; i < 5; i++ {
		controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)
		time.Sleep(2 * time.Millisecond)
	}

	close(writeConnReleased)
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int32(0), flushCount.Load(), "Flush should be skipped due to recent writes")
	assert.Equal(t, int32(0), tracker.cleanCount.Load(), "Should not mark clean when flush is skipped")
}

func TestController_FlushDoesNotResetWriteTime(t *testing.T) {
	// Tests that flush operations do not trigger write events,
	// preventing an infinite loop where flush retries would keep resetting lastWriteTime
	tracker := &mockTracker{}
	var flushCount atomic.Int32
	var acquireCount atomic.Int32

	var controller *Controller

	opts := Options{
		AutoFlushEnable:    true,
		AutoFlushIdleAfter: 10 * time.Second,
		Sentinel:           tracker,
		AcquireWrite: func(ctx context.Context, fn func(conn *driver.Conn) error) error {
			acquireCount.Add(1)
			err := fn(nil)
			// Only trigger write events for non-silent acquires (simulates real behavior)
			if controller != nil {
				controller.OnWriteEvent(driver.EventReleaseWriteWithoutChanges)
			}
			return err
		},
		AutoFlushFunc: func(ctx context.Context, conn *driver.Conn) error {
			flushCount.Add(1)
			return nil
		},
	}

	controller = NewController(opts)

	ctx := context.Background()
	err := controller.Start(ctx)
	require.NoError(t, err)
	defer controller.Stop()

	controller.OnWriteEvent(driver.EventReleaseWriteWithChanges)

	time.Sleep(60 * time.Millisecond)

	flushed, err := controller.performFlushInternal(ctx, 50*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, flushed, "Should have flushed")
	assert.Equal(t, int32(1), flushCount.Load(), "Should have flushed once")
	assert.Equal(t, int32(1), acquireCount.Load(), "Should have acquired once")
}
