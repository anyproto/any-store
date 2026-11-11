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
	// Callers are named with their receiver type, so a method of another type
	// with an allowed name is not let through.
	//
	// The index rule: its constructor, the one place the leaf rule is chosen,
	// and the interior-cell code, which always uses it.
	indexRule := map[string]bool{
		"indexLimits":                       true,
		"interiorLocalPayloadSize":          true,
		"pager.leafLimitsFor":               true,
		"tableLeafLimits":                   true,
		"leafLimitsOf":                      true,
		"parseInteriorCell":                 true,
		"interiorCellSizeWithOverflow":      true,
		"interiorFullKey":                   true,
		"btree.collectInteriorCellsKeepBuf": true,
		"btree.rebuildInteriorPage":         true,
		"btree.insertSepIntoInterior":       true,
	}
	// interiorLocalPayloadSize is the index rule too: only interior-cell code
	// may call it. checkTreePage walks both kinds of page and uses it in its
	// interior branch.
	interiorSize := map[string]bool{
		"parseInteriorCell":                 true,
		"interiorCellSizeWithOverflow":      true,
		"interiorFullKey":                   true,
		"btree.collectInteriorCellsKeepBuf": true,
		"btree.rebuildInteriorPage":         true,
		"btree.insertSepIntoInterior":       true,
		"integrityChecker.checkTreePage":    true,
	}
	guarded := map[string]map[string]bool{
		"maxLocalPayload":          indexRule,
		"minLocalPayload":          indexRule,
		"indexLimits":              indexRule,
		"interiorLocalPayloadSize": interiorSize,
	}

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
			caller := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				recv := fn.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					caller = id.Name + "." + caller
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				allowed, ok := guarded[id.Name]
				if !ok {
					return true
				}
				assert.True(t, allowed[caller],
					"%s: %s calls %s; leaf code takes its limits from pager.leafLimits",
					fset.Position(call.Pos()), caller, id.Name)
				return true
			})
		}
	}
	require.NotZero(t, checked, "no functions were inspected")
}
