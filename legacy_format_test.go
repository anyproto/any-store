package anystore

import (
	"os"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// TestMain runs the suite on schema format 5 files when
// ANYSTORE_TEST_LEGACY_FORMAT=1 (make test-legacy-format): the on-disk format
// of every release before v2.5.0, which existing databases keep.
func TestMain(m *testing.M) {
	btree.SetCheckPage1ForTest(true)
	if os.Getenv("ANYSTORE_TEST_LEGACY_FORMAT") == "1" {
		btree.SetCreateLegacyFormatForTest(true)
	}
	os.Exit(m.Run())
}
