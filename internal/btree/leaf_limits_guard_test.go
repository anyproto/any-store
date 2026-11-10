package btree

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLeafCodeTakesLimitsFromPager fails when production code reaches for the
// index-btree payload rule outside the functions that own it. A leaf cell's
// rule belongs to the file and comes from pager.leafLimits; a leaf site that
// computed the index rule itself would write or parse cells under a different
// rule from the rest of the file, which shows up only as corruption.
func TestLeafCodeTakesLimitsFromPager(t *testing.T) {
	// the index rule itself, and the interior-cell code that always uses it
	allowed := map[string]bool{
		"indexLimits":                  true,
		"interiorLocalPayloadSize":     true,
		"leafLimits":                   true, // pager.leafLimits: the one place the leaf rule is chosen
		"parseInteriorCell":            true,
		"interiorCellSizeWithOverflow": true,
		"interiorFullKey":              true,
		"collectInteriorCellsKeepBuf":  true,
		"rebuildInteriorPage":          true,
		"insertSepIntoInterior":        true,
	}
	guarded := map[string]bool{"maxLocalPayload": true, "minLocalPayload": true, "indexLimits": true}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			checked++
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok || !guarded[id.Name] {
					return true
				}
				assert.True(t, allowed[fn.Name.Name],
					"%s: %s calls %s; leaf code takes its limits from pager.leafLimits",
					fset.Position(call.Pos()), fn.Name.Name, id.Name)
				return true
			})
		}
	}
	require.NotZero(t, checked, "no functions were inspected")
}
