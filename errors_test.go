package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// The refusals of a second open are the btree's own errors under exported
// names, so whatever path returns them, wrapped or not, matches. The
// refusal of a second process cannot be produced on a platform with
// multi-process access; the btree's tests cover it in in-process mode
// (TestInProcess_CrossProcessLockRejected).
func TestOpenRefusals_MatchTheBtreeErrors(t *testing.T) {
	require.ErrorIs(t, fmt.Errorf("open backup destination: %w", btree.ErrDatabaseOpen), ErrDBAlreadyOpen)
	require.ErrorIs(t, fmt.Errorf("open backup destination: %w", btree.ErrInProcessLocked), ErrDBOpenInOtherProcess)
	require.NotErrorIs(t, ErrDBAlreadyOpen, ErrCorrupt)
	require.NotErrorIs(t, ErrDBOpenInOtherProcess, ErrCorrupt)
	require.NotErrorIs(t, ErrDBAlreadyOpen, ErrBusy)
	require.NotErrorIs(t, ErrDBOpenInOtherProcess, ErrBusy)
}

// openErr gives the btree's refusals of a newer WAL or wal-index the
// public ErrIncompatibleVersion, and its busy errors ErrBusy, keeping the
// btree's error in both cases.
func TestOpenErr_VersionAndBusy(t *testing.T) {
	for _, berr := range []error{btree.ErrWALVersion, btree.ErrNewerFormat} {
		err := openErr(fmt.Errorf("btree: recover WAL: %w", berr))
		require.ErrorIs(t, err, ErrIncompatibleVersion)
		require.ErrorIs(t, err, berr)
		require.NotErrorIs(t, err, ErrCorrupt)
	}
	for _, berr := range []error{btree.ErrBusyRecovery, btree.ErrBusySnapshot, btree.ErrReadersBusy} {
		err := openErr(fmt.Errorf("btree: read page 1: %w", berr))
		require.ErrorIs(t, err, ErrBusy)
		require.ErrorIs(t, err, berr)
		require.NotErrorIs(t, err, ErrCorrupt)
	}
}

// The backup refusals of a destination of another kind are the btree's
// errors under exported names.
func TestBackupRefusals_MatchTheBtreeErrors(t *testing.T) {
	require.ErrorIs(t, fmt.Errorf("backup: %w", btree.ErrBackupFormatMismatch), ErrBackupFormatMismatch)
	require.ErrorIs(t, fmt.Errorf("backup: %w", btree.ErrBackupCodecMismatch), ErrBackupCodecMismatch)
	require.NotErrorIs(t, ErrBackupFormatMismatch, ErrCorrupt)
	require.NotErrorIs(t, ErrBackupCodecMismatch, ErrCorrupt)
}
