// Reports drift between the freshly-extracted allowlists (*.gen.json) and the
// hand-edited mapping inputs (go_to_sqlite.json, sqlite_skip.json). Designed
// as the first step after a regen — surfaces what needs human triage before
// build_mappings is run with its strict checks.
//
// Run from the repo root, after extract_funcs:
//
//	go run ./docs/mappings/scripts/extract_funcs
//	go run ./docs/mappings/scripts/mappings_diff
//	go run ./docs/mappings/scripts/build_mappings
//
// Reports four buckets:
//
//	+ go    new Go funcs that need a row in go_to_sqlite.json
//	- go    rows in go_to_sqlite.json whose Go func no longer exists
//	+ c     new SQLite funcs (in neither go_to_sqlite cites nor sqlite_skip)
//	- c     SQLite funcs cited or skipped that no longer exist upstream
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

func main() {
	dir := flag.String("dir", "docs/mappings", "directory holding all mapping files")
	flag.Parse()

	goActual := loadFuncSet(filepath.Join(*dir, "any_store_funcs.gen.json"))
	cActual := loadFuncSet(filepath.Join(*dir, "sqlite_funcs.gen.json"))
	forward := loadForward(filepath.Join(*dir, "go_to_sqlite.json"))
	skipped := loadSkipSet(filepath.Join(*dir, "sqlite_skip.json"))

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

	report("+ go    NEW Go funcs needing rows in go_to_sqlite.json", addedGo)
	report("- go    REMOVED Go funcs (orphan rows in go_to_sqlite.json)", removedGo)
	report("+ c     NEW SQLite funcs (need decision: map or add to sqlite_skip)", addedC)
	report("- c     REMOVED SQLite funcs (stale cites in go_to_sqlite or stale skip entries)", removedC)

	fmt.Println("---")
	fmt.Printf("summary: +%d/-%d Go funcs, +%d/-%d SQLite funcs, %d untriaged stub rows\n",
		len(addedGo), len(removedGo), len(addedC), len(removedC), stubRows)
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
