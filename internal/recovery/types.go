package recovery

import (
	"context"
	"time"
)

type Stats struct {
	LastFlushTime    time.Time
	FlushDuration    time.Duration
	BytesFlushed     int64
	WalFramesFlushed int
	CheckpointMode   string
	Success          bool
}

type Tracker interface {
	OnOpen(ctx context.Context) (dirty bool, err error)
	MarkDirty()
	MarkClean()
	Close() error
}

type OnIdleSafeCallback func(stats Stats)
