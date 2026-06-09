package anystore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestFtsDataset ingests the air-crash markdown corpus and runs representative
// $text queries against it. It is a manual / integration check: set FTS_DATASET
// to the corpus directory (defaults to the sibling any-store-vector checkout) to
// run it; otherwise it is skipped.
//
//	go test . -run TestFtsDataset -v
func TestFtsDataset(t *testing.T) {
	dir := os.Getenv("FTS_DATASET")
	if dir == "" {
		dir = "../any-store-vector/aircrashdataset"
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil || len(files) == 0 {
		t.Skipf("dataset not found at %s (set FTS_DATASET); skipping", dir)
	}

	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "pages")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind:   IndexKindFulltext,
		Fields: []string{"text"},
	}))

	// Ingest.
	a := &anyenc.Arena{}
	var ingested, totalBytes int
	startIngest := time.Now()
	for _, f := range files {
		raw, rerr := os.ReadFile(f)
		if rerr != nil {
			continue
		}
		id, body := parseFrontmatter(string(raw))
		if id == "" {
			// fall back to filename stem as id
			id = strings.TrimSuffix(filepath.Base(f), ".md")
		}
		totalBytes += len(body)
		a.Reset()
		doc := a.NewObject()
		doc.Set("id", a.NewString(id))
		doc.Set("year", a.NewString(strings.TrimSuffix(filepath.Base(f), ".md")))
		doc.Set("text", a.NewString(body))
		require.NoError(t, coll.Insert(ctx, doc))
		ingested++
	}
	ingestDur := time.Since(startIngest)
	t.Logf("ingested %d docs (%d KB of text) in %v (%.1f docs/s)",
		ingested, totalBytes/1024, ingestDur.Round(time.Millisecond),
		float64(ingested)/ingestDur.Seconds())

	n, err := coll.Count(ctx)
	require.NoError(t, err)
	t.Logf("collection Count = %d", n)

	queries := []string{
		"Hindenburg",
		"Handley Page",
		"London",
		"Boeing",
		"crashes into a mountain",
		"Zeppelin",
	}
	for _, qstr := range queries {
		start := time.Now()
		iter, qerr := coll.Find(`{"$text":{"$search":` + quoteJSON(qstr) + `}}`).
			Sort(`{"score":{"$meta":"textScore"}}`).
			Limit(5).Iter(ctx)
		require.NoError(t, qerr, "query %q", qstr)

		var lines []string
		for iter.Next() {
			doc, derr := iter.Doc()
			require.NoError(t, derr)
			lines = append(lines, doc.Value().GetString("year"))
		}
		require.NoError(t, iter.Err())
		require.NoError(t, iter.Close())
		dur := time.Since(start)

		// total matches (no limit) for context
		total, cerr := coll.Find(`{"$text":{"$search":` + quoteJSON(qstr) + `}}`).Count(ctx)
		require.NoError(t, cerr)
		t.Logf("query %-26q -> %d matches, top5 years=%v  (%v)", qstr, total, lines, dur.Round(time.Microsecond))
	}
}

// parseFrontmatter extracts the `id:` value from leading `---`-delimited YAML
// frontmatter and returns (id, body) where body is the content after it.
func parseFrontmatter(content string) (id, body string) {
	s := strings.TrimLeft(content, "\ufeff \t\r\n")
	if !strings.HasPrefix(s, "---") {
		return "", content
	}
	rest := s[len("---"):]
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return "", content
	}
	front := rest[:end]
	body = rest[end+len("\n---"):]
	for _, line := range strings.Split(front, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "id:") {
			id = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			id = strings.Trim(id, `"`)
			break
		}
	}
	return id, body
}

// quoteJSON returns a minimal JSON string literal for s (enough for our queries).
func quoteJSON(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
