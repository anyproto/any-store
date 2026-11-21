//go:build vfs

package anystore

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A page read that comes back short, as from a file shorter than its page
// count, fails with io.EOF. That is a read error like any other: the
// iterator reports it from Err and Doc, and does not take it for the end of
// the result, which for an $in would be the matches of the bounds before it.
func TestReadFault_IterShortRead(t *testing.T) {
	const docs = 3000
	in := `{"email":{"$in":["` + userEmail(10) + `","` + userEmail(1500) + `","` + userEmail(2990) + `","nobody@test.com"]}}`
	build := func(t *testing.T, db DB) {
		usersBuild(docs)(t, db)
		coll, err := db.OpenCollection(ctx, "users")
		require.NoError(t, err)
		explain, err := coll.Find(in).Explain(ctx)
		require.NoError(t, err)
		require.Contains(t, explain.Plan, "CoverLookup(email)")
	}
	var sawEOF bool
	eachReadFaultOf(t, io.EOF, build, func(t *testing.T, db DB, rf *readFaults) {
		coll, err := db.OpenCollection(ctx, "users")
		require.NoError(t, err)
		var n int
		err = rf.during(func() error {
			iter, err := coll.Find(in).Iter(ctx)
			if err != nil {
				return err
			}
			for iter.Next() {
				if _, err = iter.Doc(); err != nil {
					_ = iter.Close()
					return err
				}
				n++
			}
			if err = iter.Err(); err != nil {
				_, docErr := iter.Doc()
				assert.ErrorIs(t, docErr, io.EOF, "Doc after the iteration failed")
			}
			return errors.Join(err, iter.Close())
		})
		if err != nil {
			// Only a read that failed ends the iteration with an error:
			// the run with no fault returns all three and a nil Err.
			require.True(t, rf.fired.Load(), "the iteration failed with no read failing: %v", err)
			require.ErrorIs(t, err, io.EOF)
			sawEOF = true
			return
		}
		assert.Equal(t, 3, n, "the result is complete when no error is returned (fault fired: %v)", rf.fired.Load())
	})
	require.True(t, sawEOF, "no short read reached the iterator")
}
