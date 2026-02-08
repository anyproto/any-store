package durability

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/internal/btree"
)

type Options struct {
	AutoFlushEnable    bool
	AutoFlushIdleAfter time.Duration
	AutoFlushFunc      func(ctx context.Context, db *btree.DB) error

	// AcquireWrite acquires write access to the database
	AcquireWrite func(ctx context.Context, fn func(db *btree.DB) error) error
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
	lastWriteTime   atomic.Int64
}

func NewController(opts Options) *Controller {
	if opts.AutoFlushIdleAfter <= 0 {
		opts.AutoFlushIdleAfter = 20 * time.Second
	}

	if opts.AutoFlushFunc == nil {
		flush, _ := NewFlushFunc(FlushModeCheckpointPassive)
		opts.AutoFlushFunc = flush
	}

	c := &Controller{
		opts: opts,
	}
	if opts.AutoFlushEnable {
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

// MarkCleanAfterCheck marks the DB as clean after a successful integrity check.
func (c *Controller) MarkCleanAfterCheck() {
	if c.opts.Sentinel != nil {
		c.opts.Sentinel.MarkClean()
	}
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

func (c *Controller) OnWriteEvent() {
	if c.running.Load() {
		c.lastWriteTime.Store(time.Now().UnixMilli())

		if c.opts.Sentinel != nil {
			c.opts.Sentinel.MarkDirty()
		}

		if !c.opts.AutoFlushEnable {
			return
		}
		c.timerMu.Lock()
		defer c.timerMu.Unlock()

		if !c.timer.Stop() {
			select {
			case <-c.timer.C:
			default:
			}
		}
		c.timer.Reset(c.opts.AutoFlushIdleAfter)
	}
}

// Flush perform checkpoint on the btree database
// When waitIdleDuration > 0, wait for waitIdleTime since the last write tx got released
func (c *Controller) Flush(ctx context.Context, waitIdleDuration time.Duration, mode FlushMode) error {
	if c == nil {
		return fmt.Errorf("recovery is not enabled")
	}

	flushFunc, err := NewFlushFunc(mode)
	if err != nil {
		return fmt.Errorf("invalid flush mode: %w", err)
	}

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
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("force flush cancelled: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *Controller) autoFlushLoop() {
	defer c.autoFlushWG.Done()

	for {
		select {
		case <-c.autoFlushCtx.Done():
			return
		case <-c.timer.C:
			if c.timeSinceLastWrite() >= c.opts.AutoFlushIdleAfter {
				flushed, err := c.performFlush(c.autoFlushCtx)
				if err != nil {
					if c.opts.Logger != nil {
						c.opts.Logger.Printf("Idle flush failed: %v", err)
					}
					c.timer.Reset(c.opts.AutoFlushIdleAfter)
				} else if !flushed {
					c.timer.Reset(c.opts.AutoFlushIdleAfter)
				}
			} else {
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

func (c *Controller) performFlushInternalWithFunc(ctx context.Context, idleAfter time.Duration, flushFunc func(context.Context, *btree.DB) error) (bool, error) {
	var flushed bool

	err := c.opts.AcquireWrite(ctx, func(db *btree.DB) error {
		if idleAfter > 0 {
			if c.timeSinceLastWrite() < idleAfter {
				return nil
			}
		}

		flushErr := flushFunc(ctx, db)
		if flushErr == nil {
			flushed = true
		}

		return flushErr
	})

	if err != nil {
		return false, err
	}

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

func (c *Controller) timeSinceLastWrite() time.Duration {
	return time.Since(time.UnixMilli(c.lastWriteTime.Load()))
}
