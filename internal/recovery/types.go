package recovery

import (
	"context"
	"time"
)

type Stats struct {
	LastFlushTime     time.Time
	FlushDuration     time.Duration
	BytesFlushed      int64
	WalFramesFlushed  int
	CheckpointMode    string
	Success           bool
}

type Tracker interface {
	OnOpen(ctx context.Context) (dirty bool, err error)
	MarkDirty()
	MarkClean()
	Close() error
}

type OnIdleSafeCallback func(stats Stats)

type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}