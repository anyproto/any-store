// Extracts function lists from two sources and writes them to JSON files:
//
//	docs/mappings/sqlite_funcs.gen.json     — functions from SQLite C sources (btree/wal/pager/pcache/os_unix shm bits)
//	docs/mappings/any_store_funcs.gen.json  — functions from internal/btree Go sources
//
// Run:
//
//	go run ./scripts/extract_funcs \
//	    -c   ../sqlitec/src \
//	    -go  ./internal/btree \
//	    -out-c  docs/mappings/sqlite_funcs.gen.json \
//	    -out-go docs/mappings/any_store_funcs.gen.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Entry struct {
	File string `json:"file"`
	Func string `json:"func"`
}

func main() {
	var (
		cDir    = flag.String("c", "../sqlitec/src", "directory with SQLite C sources")
		goDir   = flag.String("go", "./internal/btree", "directory with Go sources")
		outC    = flag.String("out-c", "docs/mappings/sqlite_funcs.gen.json", "output JSON for C")
		outGo   = flag.String("out-go", "docs/mappings/any_store_funcs.gen.json", "output JSON for Go")
		cFiles  = flag.String("c-files", "btree.c,btree.h,btreeInt.h,wal.c,wal.h,pager.c,pager.h,pcache.c,pcache.h,pcache1.c,memdb.c,backup.c,util.c,os_unix.c,os_win.c", "comma-separated C files to scan (relative to -c)")
		shmOnly = flag.Bool("shm-filter-os", true, "for os_unix.c/os_win.c, keep only shm-related functions")
	)
	flag.Parse()

	cEntries, err := scanC(*cDir, strings.Split(*cFiles, ","), *shmOnly)
	if err != nil {
		log.Fatalf("scanC: %v", err)
	}
	if err := writeJSON(*outC, cEntries); err != nil {
		log.Fatalf("write %s: %v", *outC, err)
	}

	goEntries, err := scanGo(*goDir)
	if err != nil {
		log.Fatalf("scanGo: %v", err)
	}
	if err := writeJSON(*outGo, goEntries); err != nil {
		log.Fatalf("write %s: %v", *outGo, err)
	}

	fmt.Printf("wrote %s (%d funcs) and %s (%d funcs)\n",
		*outC, len(cEntries), *outGo, len(goEntries))
}

func writeJSON(path string, entries []Entry) error {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].File != entries[j].File {
			return entries[i].File < entries[j].File
		}
		return entries[i].Func < entries[j].Func
	})
	b, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// --- Go extraction --------------------------------------------------------

func scanGo(dir string) ([]Entry, error) {
	var out []Entry
	fset := token.NewFileSet()
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := filepath.Base(path)
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			fname := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) > 0 {
				fname = recvTypeString(fn.Recv.List[0].Type) + "." + fname
			}
			out = append(out, Entry{File: name, Func: fname})
		}
		return nil
	})
	return out, err
}

func recvTypeString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + recvTypeString(t.X)
	case *ast.IndexExpr:
		return recvTypeString(t.X)
	case *ast.IndexListExpr:
		return recvTypeString(t.X)
	}
	return "?"
}

// --- C extraction ---------------------------------------------------------
//
// Strategy: tokenize the source while stripping comments, strings, and
// preprocessor lines; then look for the pattern
//
//	<identifier> ( balanced-parens ) { ...
//
// at top-level brace depth 0, where the `(` directly follows the identifier.
// The identifier immediately before that `(` is the function name. We reject
// obvious non-functions (control keywords, macros, typedefs).
//
// This is a heuristic but SQLite's C is very regular so it lines up well.

func scanC(dir string, files []string, shmOnlyForOS bool) ([]Entry, error) {
	var out []Entry
	for _, rel := range files {
		rel = strings.TrimSpace(rel)
		if rel == "" {
			continue
		}
		path := filepath.Join(dir, rel)
		src, err := os.ReadFile(path)
		if err != nil {
			// Skip missing files rather than erroring — tree layout may vary.
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		stripped := stripCCommentsAndStringsAndCPP(src)
		funcs := findCFunctions(stripped)
		if shmOnlyForOS && (rel == "os_unix.c" || rel == "os_win.c") {
			funcs = filterShm(funcs)
		}
		sort.Strings(funcs)
		funcs = dedup(funcs)
		for _, name := range funcs {
			out = append(out, Entry{File: rel, Func: name})
		}
	}
	return out, nil
}

func dedup(xs []string) []string {
	out := xs[:0]
	var prev string
	for i, x := range xs {
		if i == 0 || x != prev {
			out = append(out, x)
			prev = x
		}
	}
	return out
}

func filterShm(funcs []string) []string {
	var out []string
	for _, f := range funcs {
		lf := strings.ToLower(f)
		if strings.Contains(lf, "shm") {
			out = append(out, f)
		}
	}
	return out
}

// stripCCommentsAndStringsAndCPP replaces C/C++ comments, string & char
// literals, and preprocessor-directive lines with whitespace of equal length,
// so byte offsets are preserved for downstream analysis.
func stripCCommentsAndStringsAndCPP(src []byte) []byte {
	out := make([]byte, len(src))
	copy(out, src)
	n := len(out)
	i := 0
	atLineStart := true
	blank := func(from, to int) {
		for k := from; k < to; k++ {
			if out[k] != '\n' {
				out[k] = ' '
			}
		}
	}
	for i < n {
		c := out[i]
		// preprocessor line: '#' as first non-space char on line
		if atLineStart && (c == '#' || (c == ' ' || c == '\t')) {
			j := i
			for j < n && (out[j] == ' ' || out[j] == '\t') {
				j++
			}
			if j < n && out[j] == '#' {
				// blank through end of logical line (honor line continuations)
				k := j
				for k < n {
					if out[k] == '\\' && k+1 < n && out[k+1] == '\n' {
						k += 2
						continue
					}
					if out[k] == '\n' {
						break
					}
					k++
				}
				blank(i, k)
				i = k
				continue
			}
		}
		if c == '/' && i+1 < n {
			if out[i+1] == '/' {
				j := i
				for j < n && out[j] != '\n' {
					j++
				}
				blank(i, j)
				i = j
				atLineStart = false
				continue
			}
			if out[i+1] == '*' {
				j := i + 2
				for j+1 < n && !(out[j] == '*' && out[j+1] == '/') {
					j++
				}
				if j+1 < n {
					j += 2
				} else {
					j = n
				}
				blank(i, j)
				i = j
				atLineStart = false
				continue
			}
		}
		if c == '"' || c == '\'' {
			quote := c
			j := i + 1
			for j < n && out[j] != quote {
				if out[j] == '\\' && j+1 < n {
					j += 2
					continue
				}
				if out[j] == '\n' {
					break
				}
				j++
			}
			if j < n {
				j++
			}
			blank(i, j)
			i = j
			atLineStart = false
			continue
		}
		if c == '\n' {
			atLineStart = true
		} else if c != ' ' && c != '\t' && c != '\r' {
			atLineStart = false
		}
		i++
	}
	return out
}

var cKeywords = map[string]bool{
	"if": true, "else": true, "for": true, "while": true, "do": true,
	"switch": true, "case": true, "return": true, "sizeof": true,
	"typedef": true, "struct": true, "union": true, "enum": true,
	"static": true, "extern": true, "inline": true, "const": true,
	"volatile": true, "register": true, "signed": true, "unsigned": true,
	"void": true, "int": true, "char": true, "short": true, "long": true,
	"float": true, "double": true, "goto": true, "break": true,
	"continue": true, "default": true,
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
func isIdentCont(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

// findCFunctions walks the sanitized source (comments/strings/CPP blanked out)
// and collects names that fit the C function-definition shape. We locate
// each ")" followed by optional whitespace and a "{" and walk backward to
// find the matching "(" and the identifier in front of it. Brace-depth
// tracking is deliberately avoided — SQLite has #if/#else blocks that
// duplicate "{" without a matching "}" (both alternatives contribute a
// brace when the preprocessor is blanked), which desynchronises any
// naive depth counter.
func findCFunctions(src []byte) []string {
	n := len(src)
	var out []string
	for k := 0; k < n; k++ {
		if src[k] != ')' {
			continue
		}
		// must be followed (ignoring whitespace) by '{'
		m := k + 1
		for m < n && (src[m] == ' ' || src[m] == '\t' || src[m] == '\n' || src[m] == '\r') {
			m++
		}
		if m >= n || src[m] != '{' {
			continue
		}
		// Walk backward to the matching '('.
		openParen := findMatchingOpenParen(src, k)
		if openParen < 0 {
			continue
		}
		// Identifier immediately before openParen (after optional whitespace).
		j := openParen - 1
		for j >= 0 && (src[j] == ' ' || src[j] == '\t' || src[j] == '\n' || src[j] == '\r') {
			j--
		}
		if j < 0 || !isIdentCont(src[j]) {
			continue
		}
		end := j + 1
		for j >= 0 && isIdentCont(src[j]) {
			j--
		}
		start := j + 1
		name := string(src[start:end])
		if cKeywords[name] {
			continue
		}
		// Require that this looks like a top-level definition, not a nested
		// expression: either the identifier itself is at column 0, or the
		// preceding non-empty line is blank / ends with a "}" / "};" / has
		// a return-type-looking prefix. Practically, we accept it when the
		// characters between the previous "\n\n" (blank line), a ";",
		// "}", "{" at line start, or the beginning of file are only
		// identifier/whitespace/"*" characters — the shape of a C
		// declarator/return type.
		if !looksLikeTopLevelDef(src, start) {
			continue
		}
		out = append(out, name)
	}
	return out
}

func findMatchingOpenParen(src []byte, closeIdx int) int {
	depth := 1
	for i := closeIdx - 1; i >= 0; i-- {
		switch src[i] {
		case ')':
			depth++
		case '(':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// looksLikeTopLevelDef checks that the region preceding the candidate
// function name (from the previous ";", "}", or start of file up to the
// name) is "declarator-shaped": identifiers, whitespace, "*", "const",
// storage-class specifiers — no parentheses, no assignment, no braces.
func looksLikeTopLevelDef(src []byte, nameStart int) bool {
	// Walk backwards to the nearest ';', '}', '{' (line-start) or start of file.
	i := nameStart - 1
	for i >= 0 {
		c := src[i]
		if c == ';' || c == '}' || c == '{' {
			i++
			break
		}
		// A ')' before the name rules it out — we'd be inside an expression.
		if c == ')' || c == '=' || c == ',' {
			return false
		}
		i--
	}
	if i < 0 {
		i = 0
	}
	// The declarator prefix should only contain identifiers, whitespace, '*',
	// and common qualifiers. Any unexpected char disqualifies.
	for j := i; j < nameStart; j++ {
		c := src[j]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '*' {
			continue
		}
		if isIdentCont(c) {
			continue
		}
		return false
	}
	return true
}
