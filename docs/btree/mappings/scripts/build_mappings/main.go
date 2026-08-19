// Rebuilds the derived mapping artifacts from go_to_sqlite.json.
//
// Inputs (all under docs/btree/mappings/):
//
//	go_to_sqlite.json         — authoritative forward map, keyed by "<go_file>:<func>"
//	any_store_funcs.gen.json  — canonical Go-func list
//	sqlite_funcs.gen.json     — canonical SQLite-func allowlist
//
// Outputs (all under docs/btree/mappings/):
//
//	sqlite_to_go.gen.json           — reverse map, keyed by "<sqlite_file>:<func>", value is
//	                                  the list of Go citers (empty list when uncited)
//	sqlite_funcs_unmapped.gen.json  — subset of sqlite_funcs.gen.json whose funcs are uncited
//
// go_to_sqlite.json is rewritten in place with deterministic key ordering (the order of
// any_store_funcs.gen.json) and sorted cite lists.
// Run from the repo root:
//
//	go run ./docs/btree/mappings/scripts/build_mappings
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
)

type Row struct {
	SQLite []string `json:"sqlite"`
	Note   string   `json:"note"`
}

type FuncEntry struct {
	File string `json:"file"`
	Func string `json:"func"`
}

func main() {
	var (
		dir = flag.String("dir", "docs/btree/mappings", "directory holding all mapping files (inputs and outputs)")
	)
	flag.Parse()

	goList, err := loadFuncs(filepath.Join(*dir, "any_store_funcs.gen.json"))
	if err != nil {
		log.Fatalf("load go funcs: %v", err)
	}
	cList, err := loadFuncs(filepath.Join(*dir, "sqlite_funcs.gen.json"))
	if err != nil {
		log.Fatalf("load sqlite funcs: %v", err)
	}
	sqliteAllow := map[string]bool{}
	for _, e := range cList {
		sqliteAllow[e.File+":"+e.Func] = true
	}

	outDir := *dir
	forwardPath := filepath.Join(outDir, "go_to_sqlite.json")

	forward, err := loadForward(forwardPath)
	if err != nil {
		log.Fatalf("load %s: %v", forwardPath, err)
	}

	// Coverage check: every Go func in any_store_funcs.gen.json must appear.
	var missing []string
	want := map[string]bool{}
	for _, e := range goList {
		key := e.File + ":" + e.Func
		want[key] = true
		if _, ok := forward[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		log.Fatalf("go_to_sqlite.json missing rows for %d go funcs; first 5: %v", len(missing), trunc(missing, 5))
	}
	var extra []string
	for k := range forward {
		if !want[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) > 0 {
		log.Fatalf("%d forward rows have unknown go keys; first 5: %v", len(extra), trunc(extra, 5))
	}

	// Validate every cite is in the allowlist.
	var bad []string
	for key, r := range forward {
		for _, c := range r.SQLite {
			if !sqliteAllow[c] {
				bad = append(bad, key+" -> "+c)
			}
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		log.Fatalf("%d cites not in sqlite_funcs.gen.json; first 5: %v", len(bad), trunc(bad, 5))
	}

	// Normalise cite lists and rewrite forward in canonical key order.
	for k, r := range forward {
		if r.SQLite == nil {
			r.SQLite = []string{}
		}
		sort.Strings(r.SQLite)
		forward[k] = r
	}
	if err := writeOrderedObject(forwardPath, goList, forward); err != nil {
		log.Fatalf("write go_to_sqlite.json: %v", err)
	}

	// Build reverse map: sqlite_key -> []go_key, covering every entry in sqlite_funcs.gen.json.
	reverse := map[string][]string{}
	for goKey, r := range forward {
		for _, c := range r.SQLite {
			reverse[c] = append(reverse[c], goKey)
		}
	}
	for k := range reverse {
		sort.Strings(reverse[k])
	}
	for _, e := range cList {
		key := e.File + ":" + e.Func
		if _, ok := reverse[key]; !ok {
			reverse[key] = []string{}
		}
	}
	if err := writeOrderedReverse(filepath.Join(outDir, "sqlite_to_go.gen.json"), cList, reverse); err != nil {
		log.Fatalf("write sqlite_to_go.gen.json: %v", err)
	}

	// Uncited subset.
	var unused []FuncEntry
	for _, e := range cList {
		key := e.File + ":" + e.Func
		if len(reverse[key]) == 0 {
			unused = append(unused, e)
		}
	}
	if err := writeJSON(filepath.Join(outDir, "sqlite_funcs_unmapped.gen.json"), unused); err != nil {
		log.Fatalf("write sqlite_funcs_unmapped.gen.json: %v", err)
	}

	// Summary.
	var mapped, unmapped, noted int
	for _, r := range forward {
		if len(r.SQLite) > 0 {
			mapped++
		} else {
			unmapped++
		}
		if r.Note != "" {
			noted++
		}
	}
	fmt.Printf("go_to_sqlite.json:              %d rows (mapped=%d unmapped=%d noted=%d)\n",
		len(forward), mapped, unmapped, noted)
	fmt.Printf("sqlite_to_go.gen.json:          %d sqlite keys (%d with Go cites)\n",
		len(reverse), len(reverse)-len(unused))
	fmt.Printf("sqlite_funcs_unmapped.gen.json: %d uncited sqlite funcs\n", len(unused))
}

func loadFuncs(path string) ([]FuncEntry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []FuncEntry
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func loadForward(path string) (map[string]Row, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]Row{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// writeOrderedObject serialises a keyed object with keys emitted in the order of the
// canonical any_store_funcs.gen.json list. Go's json.Marshal does not preserve map order.
func writeOrderedObject(path string, order []FuncEntry, m map[string]Row) error {
	var buf bytes.Buffer
	buf.WriteString("{\n")
	for i, e := range order {
		key := e.File + ":" + e.Func
		row := m[key]
		if row.SQLite == nil {
			row.SQLite = []string{}
		}
		kb, _ := json.Marshal(key)
		vb, err := json.MarshalIndent(row, "  ", "  ")
		if err != nil {
			return err
		}
		buf.WriteString("  ")
		buf.Write(kb)
		buf.WriteString(": ")
		buf.Write(vb)
		if i < len(order)-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString("}\n")
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func writeOrderedReverse(path string, order []FuncEntry, m map[string][]string) error {
	var buf bytes.Buffer
	buf.WriteString("{\n")
	for i, e := range order {
		key := e.File + ":" + e.Func
		v := m[key]
		if v == nil {
			v = []string{}
		}
		kb, _ := json.Marshal(key)
		vb, err := json.MarshalIndent(v, "  ", "  ")
		if err != nil {
			return err
		}
		buf.WriteString("  ")
		buf.Write(kb)
		buf.WriteString(": ")
		buf.Write(vb)
		if i < len(order)-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString("}\n")
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func trunc(xs []string, n int) []string {
	if len(xs) <= n {
		return xs
	}
	return xs[:n]
}
