//go:build noasm || (!amd64 && !arm64)

package simd

import (
	"reflect"
	"testing"
)

// TestDispatchFallback pins the pure-Go fallback where dispatch_other.go
// applies: under -tags noasm and on arches without a SIMD kernel.
func TestDispatchFallback(t *testing.T) {
	same := func(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }
	if !same(dotImpl, dotScalar) || !same(l2Impl, l2Scalar) || !same(dotFBImpl, dotFloatByteScalar) {
		t.Error("a SIMD kernel is selected; want the pure-Go fallback")
	}
	if accelerated || acceleratedFB {
		t.Errorf("accelerated = %v, acceleratedFB = %v; want false", accelerated, acceleratedFB)
	}
}
