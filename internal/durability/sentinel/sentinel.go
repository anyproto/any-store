package sentinel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var (
	osStat     = os.Stat
	osOpenFile = os.OpenFile // returns *os.File (WriteString needed)
	osRemove   = os.Remove
)

const lockFileSuffix = ".lock"

var pid = fmt.Sprintf("%d", syscall.Getpid())

type SentinelTracker struct {
	path    string
	mu      sync.Mutex
	isDirty bool // Track dirty state to avoid unnecessary syscalls
}

// New returns the tracker of the sentinel file beside dbPath. The path is made
// absolute here, when the database is opened, so a later change of the working
// directory does not move the marker to another directory. SQLite also names
// its -wal file from the full pathname it resolves once, at open (pager.c:4792,
// pager.c:4927-4931).
func New(dbPath string) (*SentinelTracker, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("sentinel: cannot resolve path: %w", err)
	}
	return &SentinelTracker{path: abs + lockFileSuffix}, nil
}

func (s *SentinelTracker) OnOpen(ctx context.Context) (dirty bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = osStat(s.path)
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
	if _, err := osStat(s.path); err == nil {
		s.isDirty = true
		return
	}

	file, err := osOpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
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

	if err := osRemove(s.path); err == nil || os.IsNotExist(err) {
		s.isDirty = false
	}
}

// Remove removes the sentinel file whatever this process remembers of it:
// the file is shared by every process on the database, and one that this
// process found absent at open, or has not written since, may have been
// created by another. A file that is not there counts as removed.
func (s *SentinelTracker) Remove() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := osRemove(s.path); err == nil || os.IsNotExist(err) {
		s.isDirty = false
	}
}
