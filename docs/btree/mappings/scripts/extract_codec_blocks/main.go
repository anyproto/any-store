// Extracts every SQLCipher codec hook block from ../sqlcipher/src and writes
// docs/btree/mappings/sqlcipher_codec_blocks.gen.json so mappings_diff can
// surface any block that has no corresponding row in the hand-edited
// sqlcipher_codec.json.
//
// A "codec block" is a preprocessor block guarded on SQLITE_HAS_CODEC:
//
//	#if defined(SQLITE_HAS_CODEC)   |   #ifdef SQLITE_HAS_CODEC   |   #if SQLITE_HAS_CODEC
//	  ... body ...
//	#endif
//
// Block boundaries are file:start-end where start = the `#if[def]` line and
// end = the matching `#endif` line, inclusive. Nested `#if` directives inside
// the body are tracked so the outermost matching `#endif` closes the block.
//
// Each emitted record also carries a heuristic `context` (the enclosing
// top-level function name when the block falls inside one, otherwise
// "(file scope)") so downstream tools and human reviewers can locate the
// block at a glance without re-reading the source.
//
// Run from the repo root:
//
//	go run ./docs/btree/mappings/scripts/extract_codec_blocks \
//	    -src ../sqlcipher/src \
//	    -out docs/btree/mappings/sqlcipher_codec_blocks.gen.json
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Block records one #if[def] SQLITE_HAS_CODEC ... #endif region.
//
// File is the basename relative to the sqlcipher src dir (e.g. "wal.c") so
// keys stay short and stable when the absolute path changes.
type Block struct {
	File    string `json:"file"`
	Start   int    `json:"start"`
	End     int    `json:"end"`
	Context string `json:"context"`
}

// Key returns the "file.c:start-end" string used as the row key in
// sqlcipher_codec.json.
func (b Block) Key() string {
	return fmt.Sprintf("%s:%d-%d", b.File, b.Start, b.End)
}

func main() {
	src := flag.String("src", "../sqlcipher/src", "directory with SQLCipher C sources")
	out := flag.String("out", "docs/btree/mappings/sqlcipher_codec_blocks.gen.json", "output JSON")
	skipPatterns := flag.String("skip", defaultSkipGlobs,
		"comma-separated glob patterns (basename) to skip — files outside the btree port's scope")
	flag.Parse()

	skip := parseSkip(*skipPatterns)

	files, err := listCFiles(*src, skip)
	if err != nil {
		log.Fatalf("list %s: %v", *src, err)
	}

	var blocks []Block
	for _, f := range files {
		path := filepath.Join(*src, f)
		bs, err := scanFile(path, f)
		if err != nil {
			log.Fatalf("scan %s: %v", path, err)
		}
		blocks = append(blocks, bs...)
	}

	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].File != blocks[j].File {
			return blocks[i].File < blocks[j].File
		}
		return blocks[i].Start < blocks[j].Start
	})

	if err := writeJSON(*out, blocks); err != nil {
		log.Fatalf("write %s: %v", *out, err)
	}
	fmt.Printf("wrote %s (%d blocks across %d files)\n", *out, len(blocks), len(files))
}

// defaultSkipGlobs lists basename globs that are definitively out of scope
// for the btree port. Anything not matching one of these is scanned by
// default — the goal is "don't silently drop a new SQLCipher hook"; any
// false positive lands in the gen.json and the differ surfaces it for
// human triage.
//
//	crypto_*.c    — alternate cipher implementations (any-store users supply
//	                their own Codec via the Options.Codec interface)
//	sqlcipher.{c,h} — SQLCipher's own context-management API; not exposed
//	                  by the btree layer
//	tclsqlite.c   — TCL bindings (not part of the Go port)
//	test*.c       — test harness sources (not API)
const defaultSkipGlobs = "crypto_*.c,sqlcipher.c,sqlcipher.h,tclsqlite.c,test*.c"

func parseSkip(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func skipMatch(name string, patterns []string) bool {
	for _, p := range patterns {
		if ok, _ := filepath.Match(p, name); ok {
			return true
		}
	}
	return false
}

func listCFiles(dir string, skip []string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// .in files are configure-time templates (e.g. sqlite.h.in) but
		// SQLCipher patches them with codec-related public-API decls
		// that ultimately ship in the generated header. Treat them as
		// first-class sources so the public-API codec hooks aren't
		// silently dropped.
		if !strings.HasSuffix(name, ".c") &&
			!strings.HasSuffix(name, ".h") &&
			!strings.HasSuffix(name, ".h.in") {
			continue
		}
		if skipMatch(name, skip) {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// codecGuardRe matches the `#if[def]` line that opens a region whose
// condition mentions SQLITE_HAS_CODEC. The forms we want — any of these,
// plus compound conditionals where the macro is one term among several:
//
//	#ifdef SQLITE_HAS_CODEC
//	#if SQLITE_HAS_CODEC
//	#if defined(SQLITE_HAS_CODEC)
//	#if !defined(SQLITE_OMIT_BLOB_LITERAL) || defined(SQLITE_HAS_CODEC)
//	#if defined(SQLITE_HAS_CODEC) && !defined(SQLITE_OMIT_WAL)
//
// Match shape: a `#if`/`#ifdef`/`#ifndef` line where SQLITE_HAS_CODEC
// appears as a whole-word token anywhere in the condition. Leading
// whitespace is permitted — `global.c` uses `# ifdef` (space after `#`).
//
// We use `[^\n]*` after the directive to span the rest of the line; this
// stays single-line because the C preprocessor terminates conditionals at
// the line break (continuation requires a `\` we don't see in SQLCipher
// usage).
var codecGuardRe = regexp.MustCompile(`^\s*#\s*if(?:def|ndef)?\b[^\n]*\bSQLITE_HAS_CODEC\b`)

// otherIfRe matches any other `#if`-class directive that nests inside a
// codec block and must be paired with its own `#endif` so we don't return
// from the outer block prematurely. Includes `#if`, `#ifdef`, `#ifndef`.
var otherIfRe = regexp.MustCompile(`^\s*#\s*if(?:def|ndef)?\b`)

// endifRe matches `#endif`, with or without a trailing comment.
var endifRe = regexp.MustCompile(`^\s*#\s*endif\b`)

// scanFile walks one source file, locates every codec block, and resolves
// the enclosing function for each (heuristic — see findEnclosingFunc).
func scanFile(path, base string) ([]Block, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}

	funcRanges := scanFuncRanges(lines)

	var blocks []Block
	for i := 0; i < len(lines); i++ {
		if !codecGuardRe.MatchString(lines[i]) {
			continue
		}
		end := matchEndif(lines, i)
		if end < 0 {
			return nil, fmt.Errorf("%s:%d: unterminated #if SQLITE_HAS_CODEC", base, i+1)
		}
		blocks = append(blocks, Block{
			File:    base,
			Start:   i + 1,
			End:     end + 1,
			Context: enclosingFunc(funcRanges, i+1),
		})
		// Continue scanning after the matched #endif (skip the body — any
		// nested codec block would be an unusual nesting we don't expect,
		// but if it appears we want to record it on its own, so we don't
		// jump past `end`. We only avoid re-matching the line we already
		// matched.)
	}
	return blocks, nil
}

// matchEndif returns the index (0-based) of the `#endif` that closes the
// `#if`-class directive at lineIdx, accounting for nested `#if`s. Returns
// -1 if the file ends before a matching `#endif`.
func matchEndif(lines []string, openIdx int) int {
	depth := 1
	for i := openIdx + 1; i < len(lines); i++ {
		switch {
		case endifRe.MatchString(lines[i]):
			depth--
			if depth == 0 {
				return i
			}
		case otherIfRe.MatchString(lines[i]):
			depth++
		}
	}
	return -1
}

// scanFuncRanges returns one entry per top-level C function definition,
// covering the function from its name's line through the line of its
// body's closing brace.
//
// Strategy:
//
//  1. Blank out comments, string/char literals, and preprocessor
//     directives so brace counting and paren matching aren't confused by
//     content inside them.
//  2. Walk the cleaned source tracking brace depth. At depth 0, when a
//     ")" is followed (after whitespace) by "{", treat it as a function-
//     definition signature: walk back from "(" to extract the identifier
//     name and validate the prefix looks like a return-type declarator.
//  3. Track the matching "}" that brings depth back to 0; that line is
//     the function's end.
//
// This mirrors the heuristic in extract_funcs/main.go (findCFunctions)
// but additionally tracks end-of-body via brace depth so a code block
// that lands between functions reports as "(file scope)" instead of
// being mis-attributed to the previous function.
func scanFuncRanges(lines []string) []funcRange {
	src := []byte(strings.Join(lines, "\n"))
	stripped := stripCNoise(src)
	byteToLine := make([]int, len(stripped)+1)
	line := 1
	for i := 0; i < len(stripped); i++ {
		byteToLine[i] = line
		if stripped[i] == '\n' {
			line++
		}
	}
	byteToLine[len(stripped)] = line

	var out []funcRange
	depth := 0
	// Stack of currently-open function indices in `out`. When the depth
	// returns to 0 we close the most-recent open function.
	var openStack []int
	pendingName := ""
	pendingLine := 0
	for k := 0; k < len(stripped); k++ {
		c := stripped[k]
		switch c {
		case '{':
			if depth == 0 && pendingName != "" {
				out = append(out, funcRange{start: pendingLine, name: pendingName})
				openStack = append(openStack, len(out)-1)
				pendingName = ""
				pendingLine = 0
			}
			depth++
		case '}':
			depth--
			if depth == 0 && len(openStack) > 0 {
				idx := openStack[len(openStack)-1]
				openStack = openStack[:len(openStack)-1]
				out[idx].end = byteToLine[k]
			}
		case ')':
			if depth != 0 {
				continue
			}
			// Look ahead: must be ws*, then '{' for a function definition.
			m := k + 1
			for m < len(stripped) && (stripped[m] == ' ' || stripped[m] == '\t' || stripped[m] == '\n' || stripped[m] == '\r') {
				m++
			}
			if m >= len(stripped) || stripped[m] != '{' {
				continue
			}
			open := matchingOpenParen(stripped, k)
			if open < 0 {
				continue
			}
			j := open - 1
			for j >= 0 && (stripped[j] == ' ' || stripped[j] == '\t' || stripped[j] == '\n' || stripped[j] == '\r') {
				j--
			}
			if j < 0 || !isIdentCont(stripped[j]) {
				continue
			}
			end := j + 1
			for j >= 0 && isIdentCont(stripped[j]) {
				j--
			}
			start := j + 1
			name := string(stripped[start:end])
			if isControlKeyword(name) {
				continue
			}
			if !looksLikeTopLevelDef(stripped, start) {
				continue
			}
			pendingName = name
			pendingLine = byteToLine[start]
		}
	}
	// Functions left unclosed at EOF (shouldn't happen for valid C) get
	// end == 0 — treat as extending to the last line so they still cover
	// their body.
	for i := range out {
		if out[i].end == 0 {
			out[i].end = len(byteToLine) - 1
		}
	}
	return out
}

type funcRange struct {
	start int
	end   int
	name  string
}

func matchingOpenParen(src []byte, closeIdx int) int {
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

// stripCNoise blanks comments, string/char literals, and preprocessor
// directives in src while preserving byte positions and newlines, so
// downstream paren / brace matching doesn't trip over content inside them.
// Mirrors extract_funcs/main.go:stripCCommentsAndStringsAndCPP.
func stripCNoise(src []byte) []byte {
	out := make([]byte, len(src))
	copy(out, src)
	n := len(out)
	atLineStart := true
	blank := func(from, to int) {
		for k := from; k < to; k++ {
			if out[k] != '\n' {
				out[k] = ' '
			}
		}
	}
	i := 0
	for i < n {
		c := out[i]
		if atLineStart {
			j := i
			for j < n && (out[j] == ' ' || out[j] == '\t') {
				j++
			}
			if j < n && out[j] == '#' {
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

func isIdentCont(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// looksLikeTopLevelDef screens out matches that aren't actually function
// definitions — for example calls inside an expression (`if (foo() ){`),
// initialisers, function-pointer typedefs. Walks back from nameStart to
// the nearest ';', '}', '{', or BOF and checks the intervening run looks
// like a return-type declarator (idents, whitespace, '*', no '=' / ','
// / '(').
func looksLikeTopLevelDef(src []byte, nameStart int) bool {
	i := nameStart - 1
	for i >= 0 {
		c := src[i]
		if c == ';' || c == '}' || c == '{' {
			i++
			break
		}
		if c == ')' || c == '=' || c == ',' {
			return false
		}
		i--
	}
	if i < 0 {
		i = 0
	}
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

func isControlKeyword(s string) bool {
	switch s {
	case "if", "else", "for", "while", "do", "switch", "return", "sizeof",
		"typedef", "struct", "union", "enum", "static", "extern", "inline",
		"const", "volatile", "register", "signed", "unsigned",
		"void", "int", "char", "short", "long", "float", "double",
		"goto", "break", "continue", "default":
		return true
	}
	return false
}

// enclosingFunc returns the function whose body contains line, or
// "(file scope)" when line lies between functions or before the first
// function in the file. funcs is in source order by construction.
func enclosingFunc(funcs []funcRange, line int) string {
	idx := sort.Search(len(funcs), func(i int) bool { return funcs[i].start > line })
	if idx == 0 {
		return "(file scope)"
	}
	prev := funcs[idx-1]
	if line >= prev.start && line <= prev.end {
		return prev.name
	}
	return "(file scope)"
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
