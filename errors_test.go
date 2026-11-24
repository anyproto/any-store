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
