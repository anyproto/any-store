//go:build !((linux || darwin) && (amd64 || arm64))

package btree

// dbMmap is a no-op on platforms without mmap support.
type dbMmap struct{}

func newDBMmap(_ fileHandle, _ int64) *dbMmap         { return &dbMmap{} }
func (m *dbMmap) fetch(_ int64, _ int) ([]byte, bool) { return nil, false }
func (m *dbMmap) remap(_ int64) error                 { return nil }
func (m *dbMmap) unmap() error                        { return nil }
func (m *dbMmap) enabled() bool                       { return false }
