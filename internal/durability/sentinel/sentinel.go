package sentinel

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
)

const lockFileSuffix = ".lock"

var pid = fmt.Sprintf("%d", syscall.Getpid())

type SentinelTracker struct {
	path    string
	mu      sync.Mutex
	isDirty bool // Track dirty state to avoid unnecessary syscalls
}

func New(dbPath string) *SentinelTracker {
	sentinelPath := dbPath + lockFileSuffix
	tracker := &SentinelTracker{
		path: sentinelPath,
	}

	return tracker
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

	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			s.isDirty = true
		}
		return
	}
	defer file.Close()

	_, _ = file.WriteString(pid)
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
