package sentinel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/anyproto/any-store/internal/recovery"
)

type SentinelTracker struct {
	path string
	mu   sync.Mutex
}

func New(dbPath string) (*SentinelTracker, recovery.OnIdleSafeCallback) {
	sentinelPath := dbPath + ".lock"
	tracker := &SentinelTracker{
		path: sentinelPath,
	}

	onIdleSafe := func(stats recovery.Stats) {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		_ = os.Remove(tracker.path)
	}

	return tracker, onIdleSafe
}

func (s *SentinelTracker) OnOpen(ctx context.Context) (dirty bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = os.Stat(s.path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to check sentinel file: %w", err)
}

func (s *SentinelTracker) MarkDirty() {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		if !os.IsExist(err) {
			return
		}
		return
	}
	defer file.Close()

	_, _ = file.WriteString("1")
	_ = file.Sync()
}

func (s *SentinelTracker) MarkClean() {
	s.mu.Lock()
	defer s.mu.Unlock()

	_ = os.Remove(s.path)
}

func (s *SentinelTracker) Close() error {
	return nil
}
