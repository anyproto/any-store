package query

import (
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// TestFilterOkRawParity asserts the RawFilter contract: whenever OkRaw reports
// handled=true, its verdict must equal Ok on the parsed document — across
// nested paths, arrays (both as values and as path containers, where the raw
// walk must fall back), missing fields, and compressed roots.
func TestFilterOkRawParity(t *testing.T) {
	docs := []string{
		`{"id":1,"a":50,"b":25,"name":"alice","tags":["x","y"],"addr":{"city":"berlin","zip":"10x"},"n":null}`,
		`{"id":2,"a":-3.5,"b":0,"name":"bob","tags":[],"addr":{"city":"paris"}}`,
		`{"id":3,"a":"50","name":"","tags":["z"],"addr":{"deep":{"deeper":7}}}`,
		`{"id":4,"tags":[{"k":1},{"k":2}],"arr":[[1,2],[3]]}`,
		`{"id":5,"a":true,"b":false,"empty":{},"addr":{"city":null}}`,
		`{"id":6}`,
		`{"id":7,"a":[1,2,3],"b":{"0":"zero"}}`,
	}
	conds := []any{
		`{"a":50}`,
		`{"a":{"$gt":10}}`,
		`{"a":{"$lt":0}}`,
		`{"a":{"$in":[50,"50",true]}}`,
		`{"a":{"$exists":true}}`,
		`{"a":{"$exists":false}}`,
		`{"name":{"$ne":"alice"}}`,
		`{"addr.city":"berlin"}`,
		`{"addr.deep.deeper":{"$gte":7}}`,
		`{"addr.city":{"$type":"null"}}`,
		`{"tags":"x"}`,
		`{"tags.0":"x"}`,     // array container on path: raw walk must fall back
		`{"tags.k":1}`,       // array of objects on path
		`{"arr.0.1":2}`,      // nested arrays on path
		`{"b.0":"zero"}`,     // numeric KEY on an object (not an array index)
		`{"$and":[{"a":{"$gte":10}},{"b":{"$lte":30}}]}`,
		`{"$or":[{"a":50},{"name":"bob"}]}`,
		`{"$nor":[{"a":50},{"name":"bob"}]}`,
		`{"a":{"$not":{"$eq":50}}}`,
		`{"$or":[{"tags.0":"x"},{"a":50}]}`, // partially lazy Or
		`{"missing.path.deep":{"$exists":false}}`,
		`{"n":null}`,
	}

	parser := &anyenc.Parser{}
	sp := syncpool.NewSyncPool(0)

	for _, docJSON := range docs {
		v, err := anyenc.ParseJson(docJSON)
		if err != nil {
			t.Fatalf("parse doc %s: %v", docJSON, err)
		}
		plain := v.MarshalTo(nil)
		compressed, _ := v.MarshalCompressed(nil, nil)
		for _, cond := range conds {
			f := MustParseCondition(cond)
			rf, isRaw := f.(RawFilter)
			if !isRaw {
				continue
			}
			for enc, doc := range map[string][]byte{"plain": plain, "compressed": compressed} {
				buf := sp.GetDocBuf()
				parsed, perr := parser.ParseOwned(plain)
				if perr != nil {
					t.Fatalf("reparse doc %s: %v", docJSON, perr)
				}
				want := f.Ok(parsed, buf)

				decoded, derr := buf.Parser.DecodedDoc(doc)
				if derr != nil {
					t.Fatalf("decode doc %s: %v", docJSON, derr)
				}
				got, handled := rf.OkRaw(decoded, buf)
				if handled && got != want {
					t.Errorf("parity broken (%s): doc=%s cond=%v OkRaw=%v Ok=%v", enc, docJSON, cond, got, want)
				}
				sp.ReleaseDocBuf(buf)
			}
		}
	}
}

// TestFilterOkRawLazyCoverage pins which filter shapes are expected to be
// handled lazily, so a refactor silently degrading the fast path to
// full-parse fallback fails a test instead of only a benchmark.
func TestFilterOkRawLazyCoverage(t *testing.T) {
	doc := `{"a":50,"b":25,"addr":{"city":"berlin"},"tags":["x"]}`
	sp := syncpool.NewSyncPool(0)

	cases := []struct {
		cond    any
		handled bool
	}{
		{`{"a":50}`, true},
		{`{"a":{"$gt":10},"b":{"$lt":30}}`, true},
		{`{"addr.city":"berlin"}`, true},
		{`{"$or":[{"a":1},{"b":25}]}`, true},
		{`{"$nor":[{"a":1},{"b":1}]}`, true},
		{`{"a":{"$not":{"$eq":1}}}`, true},
		{`{"tags":"x"}`, true},     // array as terminal value stays lazy
		{`{"tags.0":"x"}`, false},  // array as path container falls back
	}

	v, err := anyenc.ParseJson(doc)
	if err != nil {
		t.Fatal(err)
	}
	raw := v.MarshalTo(nil)

	for _, tc := range cases {
		f := MustParseCondition(tc.cond)
		rf, isRaw := f.(RawFilter)
		if !isRaw {
			t.Fatalf("cond %v: filter %T does not implement RawFilter", tc.cond, f)
		}
		buf := sp.GetDocBuf()
		_, handled := rf.OkRaw(raw, buf)
		if handled != tc.handled {
			t.Errorf("cond %v: handled=%v, want %v", tc.cond, handled, tc.handled)
		}
		sp.ReleaseDocBuf(buf)
	}
}

var _ = fmt.Sprintf // keep fmt for debug edits
