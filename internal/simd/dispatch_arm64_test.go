//go:build !noasm && arm64

package simd

import (
	"reflect"
	"runtime"
	"testing"

	"golang.org/x/sys/cpu"
)

// funcName names the function a func value points at.
func funcName(f any) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// TestDispatchArm64FloatByte pins the arm64 float×byte dispatch to any-store's
// NEON kernel, so a dispatch_arm64.go that selects the vendored
// asm.DotFloatByte_Neon (or anything else) fails here. Without ASIMD the
// fallback stays.
func TestDispatchArm64FloatByte(t *testing.T) {
	want, wantAccel := dotFloatByteScalar, false
	if cpu.ARM64.HasASIMD {
		want, wantAccel = dotFloatByteNEON, true
	}
	if reflect.ValueOf(dotFBImpl).Pointer() != reflect.ValueOf(want).Pointer() {
		t.Errorf("dotFBImpl is %s, want %s", funcName(dotFBImpl), funcName(want))
	}
	if acceleratedFB != wantAccel {
		t.Errorf("acceleratedFB = %v, want %v", acceleratedFB, wantAccel)
	}
}
