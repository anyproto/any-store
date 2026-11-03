// Package buildpool holds the process-wide budget that vector index builds
// and compactions — HNSW (internal/vindex) and IVF (internal/vivf) alike —
// run their build tasks under (anystore.SetVectorBuildConcurrency): however
// many indexes build at once, no more build tasks execute concurrently than
// the budget has tokens. Whatever executes a build task holds a token while
// it does, whether a worker goroutine or a phase's own goroutine running its
// tasks in turn. The serial work a build does between its phases — btree
// writes, sorts, centroid updates, sampling — holds none and is outside the
// cap.
//
// A phase takes the current budget when it starts and keeps it to the end, so
// a Set while it runs changes only the phases that start after. Every holder
// returns its token when its phase's tasks run out, and a worker still waiting
// for one when every task has been claimed gives up, so a phase ends as soon
// as its own tasks are done and phases back to back each get the budget
// afresh. A goroutine must not wait on the budget while it holds one of its
// tokens: with every token so held, nothing completes.
package buildpool

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Budget is a number of tokens; whatever executes a build task holds one.
type Budget struct{ tokens chan struct{} }

var current atomic.Pointer[Budget]

func init() { Set(runtime.GOMAXPROCS(0)) }

// Set replaces the process-wide budget with one of n tokens; n < 1 is treated
// as 1. Phases already running keep the budget they took.
func Set(n int) {
	current.Store(&Budget{tokens: make(chan struct{}, max(n, 1))})
}

// Current returns the budget a phase about to start draws its tokens from.
func Current() *Budget { return current.Load() }

// Size returns how many build tasks the budget lets execute at once.
func (b *Budget) Size() int { return cap(b.tokens) }

// Acquire takes a token, waiting until one is free. Release returns it.
func (b *Budget) Acquire() { b.tokens <- struct{}{} }

// Release returns a token taken by Acquire.
func (b *Budget) Release() { <-b.tokens }

// acquireUnless takes a token and reports true, or gives up and reports false
// once done is closed, whichever comes first.
func (b *Budget) acquireUnless(done <-chan struct{}) bool {
	select {
	case b.tokens <- struct{}{}:
		return true
	case <-done:
		return false
	}
}

// Run calls fn(task) once for each task in [0, tasks) on up to Size()
// workers, and returns when all have returned (see Work). The caller must not
// hold a token of b, and fn must not wait on the budget.
func (b *Budget) Run(tasks int, fn func(task int)) {
	Work(b, b.Size(), tasks, func() struct{} { return struct{}{} }, func(_ struct{}, task int) { fn(task) })
}

// Work calls do(state, item) once for each item in [0, n) and returns when
// all have returned. With more than one worker it starts up to workers
// goroutines; each waits for a token of b, makes its state with newState, and
// claims items in turn until none is left, then returns the token. A worker
// still waiting for a token when every item has been claimed gives up without
// one, so a phase never waits on a token it has no work for. With one worker
// the calling goroutine takes a token and does every item in order with one
// state. The caller must not hold a token of b, and do must not wait on the
// budget.
func Work[S any](b *Budget, workers, n int, newState func() S, do func(state S, item int)) {
	if n <= 0 {
		return
	}
	workers = min(workers, n)
	if workers <= 1 {
		b.Acquire()
		defer b.Release()
		state := newState()
		for i := 0; i < n; i++ {
			do(state, i)
		}
		return
	}
	var next atomic.Int64
	claimed := make(chan struct{}) // closed once every item has been claimed
	var closeClaimed sync.Once
	claim := func() (int, bool) {
		i := int(next.Add(1) - 1)
		if i >= n-1 {
			closeClaimed.Do(func() { close(claimed) })
		}
		return i, i < n
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			if !b.acquireUnless(claimed) {
				return
			}
			defer b.Release()
			var state S
			started := false
			for {
				i, ok := claim()
				if !ok {
					return
				}
				if !started {
					state, started = newState(), true
				}
				do(state, i)
			}
		})
	}
	wg.Wait()
}
