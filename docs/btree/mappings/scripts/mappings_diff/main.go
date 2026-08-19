// Reports drift between the freshly-extracted allowlists (*.gen.json) and the
// hand-edited mapping inputs (go_to_sqlite.json, sqlite_skip.json,
// sqlcipher_codec.json). Designed as the first step after a regen — surfaces
// what needs human triage before build_mappings is run with its strict checks.
//
// Run from the repo root, after extract_funcs + extract_codec_blocks:
//
//	go run ./docs/btree/mappings/scripts/extract_funcs
//	go run ./docs/btree/mappings/scripts/extract_codec_blocks
//	go run ./docs/btree/mappings/scripts/mappings_diff
//	go run ./docs/btree/mappings/scripts/build_mappings
//
// Reports six buckets:
//
//	+ go     new Go funcs that need a row in go_to_sqlite.json
//	- go     rows in go_to_sqlite.json whose Go func no longer exists
//	+ c      new SQLite funcs (in neither go_to_sqlite cites nor sqlite_skip)
//	- c      SQLite funcs cited or skipped that no longer exist upstream
//	+ codec  new SQLCipher codec blocks that need a row in sqlcipher_codec.json
//	- codec  rows in sqlcipher_codec.json whose block no longer exists upstream
//
// Always exits 0; intended for triage, not gating.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type FuncEntry struct {
	File string `json:"file"`
	Func string `json:"func"`
}

type Row struct {
	SQLite []string `json:"sqlite"`
	Note   string   `json:"note"`
}

type SkipGroup struct {
	Group string      `json:"group"`
	Funcs []FuncEntry `json:"funcs"`
}

// CodecBlock matches the records emitted by extract_codec_blocks.
type CodecBlock struct {
	File    string `json:"file"`
	Start   int    `json:"start"`
	End     int    `json:"end"`
	Context string `json:"context"`
}

// Key returns the same string used as the row key in sqlcipher_codec.json:
// "<file>:<start>-<end>".
func (b CodecBlock) Key() string {
	return fmt.Sprintf("%s:%d-%d", b.File, b.Start, b.End)
}

// CodecRow matches the value shape of sqlcipher_codec.json's hand-edited rows.
type CodecRow struct {
	Code []string `json:"code"`
	Note string   `json:"note"`
}

func main() {
	dir := flag.String("dir", "docs/btree/mappings", "directory holding all mapping files")
	flag.Parse()

	goActual := loadFuncSet(filepath.Join(*dir, "any_store_funcs.gen.json"))
	cActual := loadFuncSet(filepath.Join(*dir, "sqlite_funcs.gen.json"))
	forward := loadForward(filepath.Join(*dir, "go_to_sqlite.json"))
	skipped := loadSkipSet(filepath.Join(*dir, "sqlite_skip.json"))
	codecActual, codecCtx := loadCodecBlocks(filepath.Join(*dir, "sqlcipher_codec_blocks.gen.json"))
	codecMapped := loadCodecMap(filepath.Join(*dir, "sqlcipher_codec.json"))

	// Forward map keys = Go funcs we have rows for.
	goMapped := map[string]bool{}
	cCited := map[string]bool{}
	stubRows := 0
	for k, r := range forward {
		goMapped[k] = true
		if len(r.SQLite) == 0 {
			stubRows++
		}
		for _, c := range r.SQLite {
			cCited[c] = true
		}
	}

	// SQLite "accounted for" = cited by some Go row OR explicitly skipped.
	cAccounted := map[string]bool{}
	for k := range cCited {
		cAccounted[k] = true
	}
	for k := range skipped {
		cAccounted[k] = true
	}

	addedGo := diff(goActual, goMapped)
	removedGo := diff(goMapped, goActual)
	addedC := diff(cActual, cAccounted)
	removedC := diffMulti(cCited, skipped, cActual)

	codecMappedKeys := map[string]bool{}
	codecStubs := 0
	for k, r := range codecMapped {
		codecMappedKeys[k] = true
		// In sqlcipher_codec.json an EMPTY `code` list is intentional
		// (out-of-scope hook with a `note`); we don't count those as
		// stubs. Stub means a present row never got a code/note pair.
		if len(r.Code) == 0 && strings.TrimSpace(r.Note) == "" {
			codecStubs++
		}
	}
	addedCodec := diff(codecActual, codecMappedKeys)
	removedCodec := diff(codecMappedKeys, codecActual)

	report("+ go    NEW Go funcs needing rows in go_to_sqlite.json", addedGo)
	report("- go    REMOVED Go funcs (orphan rows in go_to_sqlite.json)", removedGo)
	report("+ c     NEW SQLite funcs (need decision: map or add to sqlite_skip)", addedC)
	report("- c     REMOVED SQLite funcs (stale cites in go_to_sqlite or stale skip entries)", removedC)
	reportCodec("+ codec NEW SQLCipher codec blocks needing rows in sqlcipher_codec.json", addedCodec, codecCtx)
	report("- codec REMOVED SQLCipher codec blocks (orphan rows in sqlcipher_codec.json)", removedCodec)

	fmt.Println("---")
	fmt.Printf("summary: +%d/-%d Go funcs, +%d/-%d SQLite funcs, +%d/-%d codec blocks, %d untriaged stub rows (%d codec)\n",
		len(addedGo), len(removedGo), len(addedC), len(removedC),
		len(addedCodec), len(removedCodec), stubRows+codecStubs, codecStubs)
}

func diff(a, b map[string]bool) []string {
	var out []string
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// diffMulti returns keys present in (a ∪ b) but absent from c.
func diffMulti(a, b, c map[string]bool) []string {
	seen := map[string]bool{}
	for k := range a {
		if !c[k] {
			seen[k] = true
		}
	}
	for k := range b {
		if !c[k] {
			seen[k] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func report(title string, keys []string) {
	fmt.Printf("\n=== %s (%d) ===\n", title, len(keys))
	for _, k := range keys {
		fmt.Println("  " + k)
	}
}

func loadFuncSet(path string) map[string]bool {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	var entries []FuncEntry
	if err := json.Unmarshal(b, &entries); err != nil {
		log.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]bool{}
	for _, e := range entries {
		out[e.File+":"+e.Func] = true
	}
	return out
}

func loadForward(path string) map[string]Row {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	out := map[string]Row{}
	if err := json.Unmarshal(b, &out); err != nil {
		log.Fatalf("parse %s: %v", path, err)
	}
	return out
}

func loadSkipSet(path string) map[string]bool {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	var groups []SkipGroup
	if err := json.Unmarshal(b, &groups); err != nil {
		log.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]bool{}
	for _, g := range groups {
		for _, e := range g.Funcs {
			out[e.File+":"+e.Func] = true
		}
	}
	return out
}

// loadCodecBlocks reads sqlcipher_codec_blocks.gen.json and returns the
// set of block keys plus a parallel map from key → context (function
// name or "(file scope)") so + codec rows can show the context inline.
func loadCodecBlocks(path string) (map[string]bool, map[string]string) {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	var blocks []CodecBlock
	if err := json.Unmarshal(b, &blocks); err != nil {
		log.Fatalf("parse %s: %v", path, err)
	}
	keys := map[string]bool{}
	ctx := map[string]string{}
	for _, blk := range blocks {
		k := blk.Key()
		keys[k] = true
		ctx[k] = blk.Context
	}
	return keys, ctx
}

// loadCodecMap reads sqlcipher_codec.json. The "_about" key is metadata,
// not a row, and is filtered out so the differ doesn't report it as an
// orphan.
func loadCodecMap(path string) map[string]CodecRow {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	// The "_about" entry has a different shape from regular rows
	// (free-form metadata, not {code, note}). Decode into a generic map
	// first, drop it, and then unmarshal the rest into CodecRow.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		log.Fatalf("parse %s: %v", path, err)
	}
	delete(raw, "_about")
	out := map[string]CodecRow{}
	for k, v := range raw {
		var r CodecRow
		if err := json.Unmarshal(v, &r); err != nil {
			log.Fatalf("%s: row %q: %v", path, k, err)
		}
		out[k] = r
	}
	return out
}

// reportCodec is like report but for new codec blocks: adds the upstream
// context next to each key so reviewers can decide where in
// sqlcipher_codec.json the row belongs without grepping.
func reportCodec(title string, keys []string, ctx map[string]string) {
	fmt.Printf("\n=== %s (%d) ===\n", title, len(keys))
	for _, k := range keys {
		c := ctx[k]
		if c == "" {
			c = "(unknown)"
		}
		fmt.Printf("  %s  in %s\n", k, c)
	}
}
