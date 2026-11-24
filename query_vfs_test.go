//go:build vfs

package anystore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A covering lookup whose seek fails returns the error: it is not a key
// that is absent. An equality and an $in over the unique index, counted and
// iterated, return either the read error or the whole result (one document
// for the equality, three for the $in, whose fourth value matches nothing),
// never part of it.
func TestReadFault_CoverLookup(t *testing.T) {
	const docs = 3000
	in := `{"email":{"$in":["` + userEmail(10) + `","` + userEmail(1500) + `","` + userEmail(2990) + `","nobody@test.com"]}}`
	eq := `{"email":"` + userEmail(1500) + `"}`
	build := func(t *testing.T, db DB) {
		usersBuild(docs)(t, db)
		coll, err := db.OpenCollection(ctx, "users")
		require.NoError(t, err)
		for _, cond := range []string{eq, in} {
			explain, err := coll.Find(cond).Explain(ctx)
			require.NoError(t, err)
			require.Contains(t, explain.Plan, "CoverLookup(email)")
		}
	}
	for _, tc := range []struct {
		name, cond string
		want       int
	}{{"equality", eq, 1}, {"in", in, 3}} {
		t.Run(tc.name+" count", func(t *testing.T) {
			eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
				coll, err := db.OpenCollection(ctx, "users")
				require.NoError(t, err)
				var n int
				err = rf.during(func() (err error) {
					n, err = coll.Find(tc.cond).Count(ctx)
					return err
				})
				if err != nil {
					require.ErrorIs(t, err, errInjectedRead)
					return
				}
				assert.Equal(t, tc.want, n, "the count is complete when no error is returned (fault fired: %v)", rf.fired.Load())
			})
		})
		t.Run(tc.name+" iter", func(t *testing.T) {
			eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
				coll, err := db.OpenCollection(ctx, "users")
				require.NoError(t, err)
				var n int
				err = rf.during(func() error {
					iter, err := coll.Find(tc.cond).Iter(ctx)
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
					return errors.Join(iter.Err(), iter.Close())
				})
				if err != nil {
					require.ErrorIs(t, err, errInjectedRead)
					return
				}
				assert.Equal(t, tc.want, n, "the result is complete when no error is returned (fault fired: %v)", rf.fired.Load())
			})
		})
	}
}
