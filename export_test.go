package anystore

import (
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// What the tests in package anystore_test reach of the package's inside.

// BtreeOf returns the btree database under d.
func BtreeOf(d DB) *btree.DB { return d.(*db).btreeDB }

// SetBackupBusyTimeout sets how long Backup retries busy Steps and returns
// the function that puts the previous value back.
func SetBackupBusyTimeout(d time.Duration) (restore func()) {
	prev := backupBusyTimeout
	backupBusyTimeout = d
	return func() { backupBusyTimeout = prev }
}

// SetBackupOpenHook installs Backup's hook around the open of its
// destination (nil before the open, the destination after); nil removes it.
func SetBackupOpenHook(f func(dst *btree.DB)) { testBackupOpenHook = f }

// SetBackupBusyWaitHook installs the hook that runs each time Backup is
// about to wait before retrying a busy Step; nil removes it.
func SetBackupBusyWaitHook(f func()) { testBackupBusyWaitHook = f }
