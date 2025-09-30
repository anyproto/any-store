package durability

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/internal/driver"
)

type Options struct {
	AutoFlushEnable    bool
	AutoFlushIdleAfter time.Duration
	AutoFlushFunc      func(ctx context.Context, conn *driver.Conn) error

	// AcquireWrite acquires write connection. If silent is true, won't trigger write events on release
	AcquireWrite func(ctx context.Context, silent bool, fn func(conn *driver.Conn) error) error
	Sentinel     Sentinel
	Logger       *log.Logger
}

type Sentinel interface {
	OnOpen(ctx context.Context) (dirty bool, err error)
	MarkDirty()
	MarkClean()
}

type Controller struct {
	opts            Options
	timer           *time.Timer
	timerMu         sync.Mutex
	running         atomic.Bool
	autoFlushCtx    context.Context
	autoFlushCancel context.CancelFunc
	autoFlushWG     sync.WaitGroup
	lastWriteTime   atomic.Value
}

func NewController(opts Options) *Controller {
	if opts.AutoFlushIdleAfter <= 0 {
		opts.AutoFlushIdleAfter = 20 * time.Second
	}

	if opts.AutoFlushFunc == nil {
		// This shouldn't happen in production, but provide a default for safety
		flush, _ := NewFlushFunc(FlushModeCheckpointPassive)
		opts.AutoFlushFunc = flush
	}

	c := &Controller{
		opts: opts,
	}
	if opts.AutoFlushEnable {
		// Create a stopped timer upfront - avoids all nil checks and races
		c.timer = time.NewTimer(opts.AutoFlushIdleAfter)
		if !c.timer.Stop() {
			<-c.timer.C
		}
	}

	return c
}

func (c *Controller) OnOpen(ctx context.Context) (dirty bool, err error) {
	if c.opts.Sentinel != nil {
		dirty, err = c.opts.Sentinel.OnOpen(ctx)
	}

	return dirty, nil
}

func (c *Controller) Start(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return fmt.Errorf("controller already running")
	}

	c.autoFlushCtx, c.autoFlushCancel = context.WithCancel(ctx)

	if c.opts.AutoFlushEnable {
		c.autoFlushWG.Add(1)
		go c.autoFlushLoop()
	}

	if c.opts.Sentinel != nil {
		c.opts.Sentinel.MarkDirty()
	}

	return nil
}

func (c *Controller) Stop() error {
	if !c.running.CompareAndSwap(true, false) {
		return nil
	}

	if c.opts.AutoFlushEnable {
		c.autoFlushCancel()
		c.timerMu.Lock()
		c.timer.Stop()
		c.timerMu.Unlock()
		c.autoFlushWG.Wait()
	}

	if c.opts.Sentinel != nil {
		c.opts.Sentinel.MarkClean()
	}

	return nil
}

func (c *Controller) OnWriteEvent(event driver.Event) {
	if event.Type == driver.EventReleaseWrite && c.running.Load() {
		c.lastWriteTime.Store(event.When)

		if !c.opts.AutoFlushEnable {
			return
		}
		c.timerMu.Lock()
		defer c.timerMu.Unlock()

		// Reset the timer
		if !c.timer.Stop() {
			// Drain the channel if timer already fired
			select {
			case <-c.timer.C:
			default:
			}
		}
		c.timer.Reset(c.opts.AutoFlushIdleAfter)
	}
}

func (c *Controller) autoFlushLoop() {
	defer c.autoFlushWG.Done()

	for {
		select {
		case <-c.autoFlushCtx.Done():
			return
		case <-c.timer.C:
			lastWriteTime, ok := c.lastWriteTime.Load().(time.Time)
			idleTime := time.Since(lastWriteTime)

			if !ok || idleTime >= c.opts.AutoFlushIdleAfter {
				flushed, err := c.performFlush(c.autoFlushCtx)
				if err != nil {
					if c.opts.Logger != nil {
						c.opts.Logger.Printf("Idle flush failed: %v", err)
					}
					// Re-arm timer for retry on error
					c.timer.Reset(c.opts.AutoFlushIdleAfter)
				} else if !flushed {
					// We didn't flush because we're not idle anymore
					// Re-arm timer to check again later
					c.timer.Reset(c.opts.AutoFlushIdleAfter)
				}
				// If flushed successfully, don't re-arm - wait for next write event
			} else {
				// Not idle yet, re-arm timer
				c.timer.Reset(c.opts.AutoFlushIdleAfter)
			}
		}
	}
}

func (c *Controller) performFlush(ctx context.Context) (bool, error) {
	return c.performFlushInternal(ctx, c.opts.AutoFlushIdleAfter)
}

func (c *Controller) performFlushInternal(ctx context.Context, idleAfter time.Duration) (bool, error) {
	return c.performFlushInternalWithFunc(ctx, idleAfter, c.opts.AutoFlushFunc)
}

func (c *Controller) performFlushInternalWithFunc(ctx context.Context, idleAfter time.Duration, flushFunc func(context.Context, *driver.Conn) error) (bool, error) {
	var flushed bool

	// Use silent acquire to avoid triggering write events during flush operations
	err := c.opts.AcquireWrite(ctx, true, func(conn *driver.Conn) error {
		if idleAfter > 0 {
			// Re-check if we're still idle after acquiring the connection
			// Someone might have done writes while we were waiting
			lastWriteTime, ok := c.lastWriteTime.Load().(time.Time)
			if ok {
				idleTime := time.Since(lastWriteTime)
				if idleTime < idleAfter {
					// Not idle enough, skip flush
					return nil
				}
			}
		}

		flushErr := flushFunc(ctx, conn)
		if flushErr == nil {
			flushed = true
		}
		return flushErr
	})

	if err != nil {
		return false, err
	}

	// Only mark success and notify if we actually flushed
	if flushed {
		if c.opts.Logger != nil {
			c.opts.Logger.Printf("db flush completed\n")
		}

		if c.opts.Sentinel != nil {
			c.opts.Sentinel.MarkClean()
		}

	}

	return flushed, nil
}

// Flush perform fsync or WAL checkpoint (depends on FlushMode) on sqlite
// When waitIdleDuration > 0, wait for waitIdleTime since the last write tx got released
func (c *Controller) Flush(ctx context.Context, waitIdleDuration time.Duration, mode FlushMode) error {
	if c == nil {
		return fmt.Errorf("recovery is not enabled")
	}

	// Create custom flush function for this force flush
	flushFunc, err := NewFlushFunc(mode)
	if err != nil {
		return fmt.Errorf("invalid flush mode: %w", err)
	}

	// Keep trying to flush with short idle threshold until successful or context cancelled
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("force flush cancelled: %w", ctx.Err())
		default:
		}

		flushed, err := c.performFlushInternalWithFunc(ctx, waitIdleDuration, flushFunc)
		if err != nil {
			return fmt.Errorf("force flush failed: %w", err)
		}

		if flushed {
			// Successfully flushed
			return nil
		}

		// Not idle enough yet, wait a bit and retry
		select {
		case <-ctx.Done():
			return fmt.Errorf("force flush cancelled: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
			// Short wait before retry
		}
	}
}
