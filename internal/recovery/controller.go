package recovery

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
	IdleAfter    time.Duration
	AcquireWrite func(ctx context.Context, fn func(conn *driver.Conn) error) error
	Flush        func(ctx context.Context, conn *driver.Conn) (Stats, error)
	Trackers     []Tracker
	OnIdleSafe   []OnIdleSafeCallback
	Logger       *log.Logger
}

type Controller struct {
	opts          Options
	flush         func(ctx context.Context, conn *driver.Conn) (Stats, error)
	lastFlush     atomic.Value
	timer         *time.Timer
	timerMu       sync.Mutex
	running       atomic.Bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	lastWriteTime atomic.Value
}

func NewController(opts Options) *Controller {
	if opts.IdleAfter <= 0 {
		opts.IdleAfter = 20 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = log.New(log.Writer(), "[recovery] ", log.LstdFlags)
	}
	if opts.Flush == nil {
		// This shouldn't happen in production, but provide a default for safety
		flush, _ := NewFlushFunc(FlushModeCheckpointPassive)
		opts.Flush = flush
	}

	// Create a stopped timer upfront - avoids all nil checks and races
	timer := time.NewTimer(opts.IdleAfter)
	if !timer.Stop() {
		<-timer.C
	}

	return &Controller{
		opts:  opts,
		flush: opts.Flush,
		timer: timer,
	}
}

func (c *Controller) OnOpen(ctx context.Context) (dirty bool, err error) {
	for _, tracker := range c.opts.Trackers {
		trackerDirty, trackerErr := tracker.OnOpen(ctx)
		if trackerErr != nil {
			return false, fmt.Errorf("tracker OnOpen failed: %w", trackerErr)
		}
		if trackerDirty {
			dirty = true
		}
	}
	if dirty {
		fmt.Printf("db %s is dirty on open\n", ctx.Value("dbPath"))
	}
	return dirty, nil
}

func (c *Controller) Start(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return fmt.Errorf("controller already running")
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	c.wg.Add(1)
	go c.idleLoop()

	for _, tracker := range c.opts.Trackers {
		tracker.MarkDirty()
	}

	return nil
}

func (c *Controller) Stop() error {
	if !c.running.CompareAndSwap(true, false) {
		return nil
	}

	c.cancel()

	c.timerMu.Lock()
	c.timer.Stop()
	c.timerMu.Unlock()

	c.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.performFlush(ctx); err != nil {
		c.opts.Logger.Printf("Failed to flush on stop: %v", err)
	}

	for _, tracker := range c.opts.Trackers {
		if err := tracker.Close(); err != nil {
			c.opts.Logger.Printf("Failed to close tracker: %v", err)
		}
	}

	return nil
}

func (c *Controller) OnWriteEvent(event driver.Event) {
	if event.Type == driver.EventReleaseWrite && c.running.Load() {
		c.lastWriteTime.Store(event.When)

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
		c.timer.Reset(c.opts.IdleAfter)
	}
}

func (c *Controller) idleLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.timer.C:
			lastWriteTime, ok := c.lastWriteTime.Load().(time.Time)
			idleTime := time.Since(lastWriteTime)

			if !ok || idleTime >= c.opts.IdleAfter {
				flushed, err := c.performFlush(c.ctx)
				if err != nil {
					if c.opts.Logger != nil {
						c.opts.Logger.Printf("Idle flush failed: %v", err)
					}
					// Re-arm timer for retry on error
					c.timer.Reset(c.opts.IdleAfter)
				} else if !flushed {
					// We didn't flush because we're not idle anymore
					// Re-arm timer to check again later
					c.timer.Reset(c.opts.IdleAfter)
				}
				// If flushed successfully, don't re-arm - wait for next write event
			} else {
				// Not idle yet, re-arm timer
				c.timer.Reset(c.opts.IdleAfter)
			}
		}
	}
}

func (c *Controller) performFlush(ctx context.Context) (bool, error) {
	return c.performFlushInternal(ctx, c.opts.IdleAfter)
}

func (c *Controller) performFlushInternal(ctx context.Context, idleAfter time.Duration) (bool, error) {
	return c.performFlushInternalWithFunc(ctx, idleAfter, c.flush)
}

func (c *Controller) performFlushInternalWithFunc(ctx context.Context, idleAfter time.Duration, flushFunc func(context.Context, *driver.Conn) (Stats, error)) (bool, error) {
	var stats Stats
	var flushed bool

	err := c.opts.AcquireWrite(ctx, func(conn *driver.Conn) error {
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

		var flushErr error
		stats, flushErr = flushFunc(ctx, conn)
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
		stats.Success = true
		fmt.Printf("db %s flushed: %v\n", ctx.Value("dbPath"), stats)
		c.lastFlush.Store(stats)

		for _, tracker := range c.opts.Trackers {
			tracker.MarkClean()
		}

		for _, callback := range c.opts.OnIdleSafe {
			callback(stats)
		}
	}

	return flushed, nil
}

func (c *Controller) MarkDirty() {
	for _, tracker := range c.opts.Trackers {
		tracker.MarkDirty()
	}
}

func (c *Controller) MarkClean() {
	for _, tracker := range c.opts.Trackers {
		tracker.MarkClean()
	}
}

func (c *Controller) LastFlushStats() (Stats, bool) {
	if v := c.lastFlush.Load(); v != nil {
		return v.(Stats), true
	}
	return Stats{}, false
}

func (c *Controller) ForceFlush(ctx context.Context, waitMinIdleTime time.Duration, mode FlushMode) error {
	if c == nil {
		return fmt.Errorf("controller is nil")
	}

	if waitMinIdleTime <= 0 {
		waitMinIdleTime = 100 * time.Millisecond
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

		flushed, err := c.performFlushInternalWithFunc(ctx, waitMinIdleTime, flushFunc)
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

