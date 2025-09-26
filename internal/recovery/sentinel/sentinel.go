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
	path    string
	mu      sync.Mutex
	isDirty bool // Track dirty state to avoid unnecessary syscalls
}

func New(dbPath string) (*SentinelTracker, recovery.OnIdleSafeCallback) {
	sentinelPath := dbPath + ".lock"
	tracker := &SentinelTracker{
		path: sentinelPath,
	}

	onIdleSafe := func() {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()

		// Skip if already clean
		if !tracker.isDirty {
			return
		}

		if err := os.Remove(tracker.path); err == nil || os.IsNotExist(err) {
			tracker.isDirty = false
		}
	}

	return tracker, onIdleSafe
}

func (s *SentinelTracker) OnOpen(ctx context.Context) (dirty bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = os.Stat(s.path)
	if err == nil {
		s.isDirty = true
		return true, nil
	}
	if os.IsNotExist(err) {
		s.isDirty = false
		return false, nil
	}
	return false, fmt.Errorf("failed to check sentinel file: %w", err)
}

func (s *SentinelTracker) MarkDirty() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Skip if already dirty
	if s.isDirty {
		return
	}

	// Check if file already exists before trying to create
	if _, err := os.Stat(s.path); err == nil {
		s.isDirty = true
		return
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			s.isDirty = true
		}
		return
	}
	defer file.Close()

	_, _ = file.WriteString("1")
	_ = file.Sync()
	s.isDirty = true
}

func (s *SentinelTracker) MarkClean() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Skip if already clean
	if !s.isDirty {
		return
	}

	if err := os.Remove(s.path); err == nil || os.IsNotExist(err) {
		s.isDirty = false
	}
}

func (s *SentinelTracker) Close() error {
	return nil
}
