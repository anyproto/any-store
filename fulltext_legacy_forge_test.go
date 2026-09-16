package anystore

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// forceSecondFtsIndex creates a second full-text index bypassing
// checkSingleFulltextIndex, to reproduce the on-disk state a database written
// before that guard can have. Test-only.
func forceSecondFtsIndex(t *testing.T, coll Collection) error {
	t.Helper()
	c := coll.(*collection)
	return c.db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		fx, err := c.createFtsIndex(ctx, tx, IndexInfo{
			Name: "atitle", Fields: []string{"title"}, Kind: IndexKindFulltext})
		if err != nil {
			return err
		}
		c.storeFtsIndexes(append(c.loadFtsIndexes(), fx))
		return nil
	})
}
