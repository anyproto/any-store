package recovery

import (
	"context"
)

type Tracker interface {
	OnOpen(ctx context.Context) (dirty bool, err error)
	MarkDirty()
	MarkClean()
	Close() error
}

type OnIdleSafeCallback func()
