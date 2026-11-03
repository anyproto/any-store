package simd

import (
	"math"
	"math/rand"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// TestKernelLengths runs every exported function that reaches a kernel on
// pairs of vectors with empty, nil, shorter and longer operands, under each set
// of kernels in turn, and checks it against the pure-Go kernels alone: the same
// result, or, where those panic, a panic from the function's own length check.
// A kernel that reads past a vector can fault, which no recover catches, so
// each case runs in a child process (TestKernelLengthsChild) and a fault fails
// only that case.
func TestKernelLengths(t *testing.T) {
	for _, ks := range kernelSets() {
		t.Run(ks.name, func(t *testing.T) {
			t.Parallel()
			mode := ""
			if ks.probe {
				if out, err := runChild("probe/" + ks.name); err != nil {
					t.Logf("this CPU does not run the %s kernels, so cases that hand them a non-empty vector skip:\n%s", ks.name, childFailure(out))
					mode = "/tripwire"
				}
			}
			for _, e := range entryPoints {
				t.Run(e.name, func(t *testing.T) {
					t.Parallel()
					for _, c := range lengthCases {
						t.Run(c.name, func(t *testing.T) {
							t.Parallel()
							out, err := runChild(ks.name + "/" + e.name + "/" + c.name + mode)
							switch {
							case err != nil:
								t.Errorf("child %v:\n%s", err, childFailure(out))
							case strings.Contains(out, "--- SKIP"):
								t.Skip(childLog(out))
							}
						})
					}
				})
			}
		})
	}
}

// childEnv names the case TestKernelLengthsChild runs.
const childEnv = "SIMD_KERNEL_LENGTHS_CASE"

// TestKernelLengthsChild runs one case of TestKernelLengths, named by childEnv
// as set/function/case, optionally suffixed /tripwire; or, for probe/set, runs
// the set's kernels once to show this CPU can.
func TestKernelLengthsChild(t *testing.T) {
	spec := os.Getenv(childEnv)
	if spec == "" {
		t.Skip("runs in a child process of TestKernelLengths")
	}
	spec, tripwire := strings.CutSuffix(spec, "/tripwire")
	for _, ks := range kernelSets() {
		if spec == "probe/"+ks.name {
			a, _, u := lengthCases[0].vectors()
			ks.dot(a, a)
			ks.l2(a, a)
			ks.dotFB(a, u)
			return
		}
		if tripwire {
			ks = ks.tripwired(t)
		}
		for _, e := range entryPoints {
			for _, c := range lengthCases {
				if spec == ks.name+"/"+e.name+"/"+c.name {
					checkLengthCase(t, ks, e, c)
					return
				}
			}
		}
	}
	t.Fatalf("unknown case %q", spec)
}

// checkLengthCase runs e on c's vectors with ks installed and compares it with
// e's reference.
func checkLengthCase(t *testing.T, ks kernelSet, e entryPoint, c lengthCase) {
	a, b, u := c.vectors()
	want, wantPanic := catch(func() float32 { return e.ref(a, b, u) })
	var got float32
	var p any
	ks.use(func() { got, p = catch(func() float32 { return e.call(a, b, u) }) })
	switch {
	case wantPanic != nil && p == nil:
		t.Errorf("%s returned %v; the pure-Go kernels panic: %v", e.name, got, wantPanic)
	case wantPanic != nil:
		if s, _ := p.(string); !strings.HasPrefix(s, "simd: "+e.name+": ") {
			t.Errorf("%s panicked with %q, want its own length check", e.name, p)
		}
	case p != nil:
		t.Errorf("%s panicked with %v; the pure-Go kernels give %v", e.name, p, want)
	case !closeTo(float64(got), float64(want)): // NaN fails
		t.Errorf("%s = %v, the pure-Go kernels give %v", e.name, got, want)
	}
}

// catch returns f's result, or what it panicked with.
func catch(f func() float32) (r float32, p any) {
	defer func() { p = recover() }()
	return f(), nil
}

// lengthCase pairs a first and a second vector by length; -1 stands for nil.
type lengthCase struct {
	name string
	a, b int
}

var lengthCases = []lengthCase{
	{"equal", 768, 768},
	{"b-longer", 768, 800},
	{"b-shorter", 768, 767},
	{"b-nil", 768, -1},
	{"b-empty", 768, 0},
	{"a-nil", -1, 768},
	{"a-empty", 0, 768},
	{"nil-nil", -1, -1},
	{"empty-empty", 0, 0},
}

// vectors returns c's float vectors and a byte vector as long as the second.
func (c lengthCase) vectors() (a, b []float32, u []byte) {
	r := rand.New(rand.NewSource(1))
	vec := func(n int) []float32 {
		if n < 0 {
			return nil
		}
		return randVec(r, n)
	}
	a, b = vec(c.a), vec(c.b)
	if c.b >= 0 {
		u = make([]byte, c.b)
		for i := range u {
			u[i] = byte(r.Intn(256))
		}
	}
	return a, b, u
}

// entryPoint is an exported function that reaches a kernel. ref computes its
// result from the pure-Go kernels directly, never through the function itself.
type entryPoint struct {
	name string
	call func(a, b []float32, u []byte) float32
	ref  func(a, b []float32, u []byte) float32
}

var entryPoints = []entryPoint{
	{
		"Dot",
		func(a, b []float32, _ []byte) float32 { return Dot(a, b) },
		func(a, b []float32, _ []byte) float32 { return dotScalar(a, b) },
	},
	{
		"L2Squared",
		func(a, b []float32, _ []byte) float32 { return L2Squared(a, b) },
		func(a, b []float32, _ []byte) float32 { return l2Scalar(a, b) },
	},
	{
		"Distance",
		func(a, b []float32, _ []byte) float32 { return Distance(a, b) },
		func(a, b []float32, _ []byte) float32 { return sqrt32(l2Scalar(a, b)) },
	},
	{
		"Norm",
		func(a, _ []float32, _ []byte) float32 { return Norm(a) },
		func(a, _ []float32, _ []byte) float32 { return sqrt32(dotScalar(a, a)) },
	},
	{
		"NormalizeInto", // the norm of the result: 1, or 0 for a zero vector
		func(a, _ []float32, _ []byte) float32 { v := NormalizeInto(nil, a); return sqrt32(dotScalar(v, v)) },
		func(a, _ []float32, _ []byte) float32 { return min(dotScalar(a, a), 1) },
	},
	{
		"CosineSimilarity",
		func(a, b []float32, _ []byte) float32 { return CosineSimilarity(a, b) },
		func(a, b []float32, _ []byte) float32 {
			na, nb := dotScalar(a, a), dotScalar(b, b)
			if na == 0 || nb == 0 {
				return 0
			}
			return dotScalar(a, b) / sqrt32(na) / sqrt32(nb)
		},
	},
	{
		"DotFloatByte",
		func(a, _ []float32, u []byte) float32 { return DotFloatByte(a, u) },
		func(a, _ []float32, u []byte) float32 { return dotFloatByteScalar(a, u) },
	},
}

func sqrt32(x float32) float32 { return float32(math.Sqrt(float64(x))) }

// kernelSet is a set of kernels to install as if init had selected them. A set
// marked probe may need instructions this CPU lacks: a child finds out by
// running it, since cpuid can understate support (Rosetta reports no AVX2 yet
// runs it).
type kernelSet struct {
	name  string
	dot   func(a, b []float32) float32
	l2    func(a, b []float32) float32
	dotFB func([]float32, []byte) float32
	probe bool
}

// archKernelSets are this arch's kernels, installed whatever cpuid reports.
var archKernelSets []kernelSet

func kernelSets() []kernelSet {
	return append([]kernelSet{
		{name: "dispatched", dot: dotImpl, l2: l2Impl, dotFB: dotFBImpl},
		{name: "fallback", dot: dotScalar, l2: l2Scalar, dotFB: dotFloatByteScalar},
	}, archKernelSets...)
}

// use runs f with ks installed and then restores the kernels init selected.
func (ks kernelSet) use(f func()) {
	d, l, fb := dotImpl, l2Impl, dotFBImpl
	defer func() { dotImpl, l2Impl, dotFBImpl = d, l, fb }()
	dotImpl, l2Impl, dotFBImpl = ks.dot, ks.l2, ks.dotFB
	f()
}

// tripwired returns ks with each kernel replaced by one that skips the test
// rather than hand ks's kernel a non-empty vector, for a set this CPU cannot
// run. Empty vectors still reach ks's kernels, which must not fault on them.
func (ks kernelSet) tripwired(t *testing.T) kernelSet {
	skip := func() { t.Skipf("this CPU does not run the %s kernels", ks.name) }
	dot, l2, dotFB := ks.dot, ks.l2, ks.dotFB
	ks.dot = func(a, b []float32) float32 {
		if len(a) > 0 {
			skip()
		}
		return dot(a, b)
	}
	ks.l2 = func(a, b []float32) float32 {
		if len(a) > 0 {
			skip()
		}
		return l2(a, b)
	}
	ks.dotFB = func(q []float32, u []byte) float32 {
		if len(q) > 0 {
			skip()
		}
		return dotFB(q, u)
	}
	return ks
}

// runChild runs TestKernelLengthsChild on spec in a fresh test binary and
// returns its output, with an error unless it passed or skipped.
func runChild(spec string) (string, error) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestKernelLengthsChild$", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"="+spec)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// childLog returns the lines a child's test logged.
func childLog(out string) string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "    ") {
			lines = append(lines, strings.TrimSpace(l))
		}
	}
	return strings.Join(lines, "\n")
}

// childFailure says why a child failed: its test's errors, or the head of the
// fatal fault and the kernel source line it was in.
func childFailure(out string) string {
	if strings.Contains(out, "--- FAIL") {
		return childLog(out)
	}
	var why []string
	inKernel := false
	for _, l := range strings.Split(out, "\n") {
		for _, p := range []string{"unexpected fault address", "fatal error:", "[signal ", "SIG", "runtime: g ", "panic: "} {
			if strings.HasPrefix(l, p) && len(why) < 4 && !slices.Contains(why, l) {
				why = append(why, l)
				break
			}
		}
		if i := strings.Index(l, "internal/simd/asm/"); i >= 0 && strings.Contains(l, ".s:") && !inKernel {
			why = append(why, "at "+strings.Fields(l[i:])[0])
			inKernel = true
		}
	}
	if len(why) == 0 {
		return out
	}
	return strings.Join(why, "\n")
}
