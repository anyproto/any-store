package test

// Test fixture and shared assertions for the black-box any-store suites.
// Everything here drives the library through its exported API only.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

var ctx = context.Background()

func newFixture(t testing.TB, c ...*anystore.Config) *fixture {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		var conf *anystore.Config
		if len(c) != 0 {
			conf = c[0]
		}
		if conf == nil {
			conf = &anystore.Config{}
		}
		conf.InMemory = true
		db, err := anystore.Open(ctx, ":memory:", conf)
		require.NoError(t, err)
		fx := &fixture{DB: db, t: t}
		t.Cleanup(fx.finish)
		return fx
	}
	tmpDir, err := os.MkdirTemp("", "any-store-*")
	require.NoError(t, err)
	return newFixturePath(t, tmpDir, c...)
}

func newFixturePath(t testing.TB, tmpDir string, c ...*anystore.Config) *fixture {
	var conf *anystore.Config
	if len(c) != 0 {
		conf = c[0]
	}

	db, err := anystore.Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), conf)
	require.NoError(t, err)

	fx := &fixture{
		DB:     db,
		tmpDir: tmpDir,
		t:      t,
	}
	t.Cleanup(fx.finish)
	return fx
}

type fixture struct {
	anystore.DB
	tmpDir string
	t      testing.TB
}

func (fx *fixture) finish() {
	closeErr := fx.Close()
	if !errors.Is(closeErr, anystore.ErrDBIsClosed) {
		require.NoError(fx.t, closeErr)
	}
	if fx.tmpDir != "" {
		_ = os.RemoveAll(fx.tmpDir)
	}
}

// assertCollCount counts through Collection.Count, which reads the namespace
// directly. For the planner's count see assertCollCountInTx or assertQueryCount.
func assertCollCount(t interface {
	Helper()
	Errorf(format string, args ...any)
}, coll anystore.Collection, expected int) {
	t.Helper()
	count, err := coll.Count(context.Background())
	if err != nil {
		t.Errorf("count error: %v", err)
		return
	}
	if count != expected {
		t.Errorf("expected count %d, got %d", expected, count)
	}
}

// assertCollCountInTx counts through Find(nil), the planner path, in the
// transaction ctx carries — a different path from assertCollCount.
func assertCollCountInTx(ctx context.Context, t interface {
	Helper()
	Errorf(format string, args ...any)
}, coll anystore.Collection, expected int) {
	t.Helper()
	count, err := coll.Find(nil).Count(ctx)
	if err != nil {
		t.Errorf("count error: %v", err)
		return
	}
	if count != expected {
		t.Errorf("expected count %d, got %d", expected, count)
	}
}

func assertIndexLen(t testing.TB, idx anystore.Index, expected int) bool {
	count, err := idx.Len(ctx)
	require.NoError(t, err)
	return assert.Equal(t, expected, count)
}

func assertQueryCount(t testing.TB, q anystore.Query, expected int) {
	t.Helper()
	count, err := q.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, count)
}

// ftsTestColl creates a collection with a full-text index over the given fields.
func ftsTestColl(t *testing.T, fields ...string) (*fixture, anystore.Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind:   anystore.IndexKindFulltext,
		Fields: fields,
	}))
	return fx, coll
}

func insertJSON(t *testing.T, coll anystore.Collection, jsons ...string) {
	for _, j := range jsons {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(j)))
	}
}

func vrand(n, dim int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	v := make([][]float32, n)
	for i := range v {
		x := make([]float32, dim)
		for d := range x {
			x[d] = rng.Float32()*2 - 1
		}
		v[i] = x
	}
	return v
}

func vecDocJSON(id int, vec []float32) string {
	parts := make([]string, len(vec))
	for i, f := range vec {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return fmt.Sprintf(`{"id":%d,"v":[%s]}`, id, strings.Join(parts, ","))
}

// idBytesOf returns the stored document-id bytes for an integer id.
func idBytesOf(id int) []byte {
	idVal := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, id)).Get("id")
	if idVal == nil {
		panic("idBytesOf: document without id")
	}
	return idVal.MarshalTo(nil)
}

// vhit mirrors the old VectorHit result for tests.
type vhit struct {
	DocId    []byte
	Distance float32
}

// vsearch runs a k-NN search through the public Find() pipeline via the $knn
// operator and returns docId+distance pairs, closest-first. ef<=0 uses the
// index default. k must be > 0: $k is mandatory in the clause.
func vsearch(coll anystore.Collection, field string, q []float32, k, ef int) ([]vhit, error) {
	if k <= 0 {
		panic("vsearch: $knn requires k > 0")
	}
	fq := coll.Find(fmt.Sprintf(`{%q:%s}`, field, vknnJSON(q, k, ef)))
	iter, err := fq.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []vhit
	for iter.Next() {
		d, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		out = append(out, vhit{DocId: d.Value().Get("id").MarshalTo(nil), Distance: iter.Distance()})
	}
	return out, iter.Err()
}

func vl2(a, b []float32) float32 {
	var s float32
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return float32(math.Sqrt(float64(s)))
}

func bruteIDs(vecs [][]float32, q []float32, k int) map[string]bool {
	type p struct {
		i int
		d float32
	}
	all := make([]p, len(vecs))
	for i := range vecs {
		all[i] = p{i, vl2(q, vecs[i])}
	}
	slices.SortFunc(all, func(a, b p) int { return cmp.Compare(a.d, b.d) })
	out := map[string]bool{}
	for i := 0; i < k && i < len(all); i++ {
		out[string(idBytesOf(all[i].i))] = true
	}
	return out
}

// qplannerEnableCounters turns the pipeline profiling counters on for one test.
func qplannerEnableCounters(t *testing.T) {
	anystore.ResetPipelinePerfCounters()
	anystore.EnablePipelinePerfCounters(true)
	t.Cleanup(func() { anystore.EnablePipelinePerfCounters(false) })
}

// qplannerSnapshot reads the planner counters off the public pipeline export.
func qplannerSnapshot() anystore.PipelinePerfCounters {
	return anystore.SnapshotPipelinePerfCounters()
}

// skipIfInMemory skips a test that the in-memory backend cannot run; reason
// says why, in the caller's terms.
func skipIfInMemory(t testing.TB, reason string) {
	t.Helper()
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip(reason)
	}
}
