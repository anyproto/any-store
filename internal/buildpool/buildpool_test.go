package buildpool

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func budgetOf(n int) *Budget { return &Budget{tokens: make(chan struct{}, n)} }

// waitDone fails the test if done does not close in time (a deadlock).
func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("phase did not complete")
	}
}

func TestRunCallsEveryTaskOnce(t *testing.T) {
	for _, size := range []int{1, 2, 3, 8} {
		for _, tasks := range []int{0, 1, 2, 5, 100} {
			counts := make([]atomic.Int32, tasks)
			budgetOf(size).Run(tasks, func(task int) { counts[task].Add(1) })
			for task := range counts {
				require.EqualValues(t, 1, counts[task].Load(), "size %d tasks %d task %d", size, tasks, task)
			}
		}
	}
}

// peakTasks runs phases of the given number of tasks on b from several
// goroutines at once and returns the most tasks that ever executed at the
// same time.
func peakTasks(t *testing.T, b *Budget, goroutines, phases, tasks int) int32 {
	t.Helper()
	var active, peak atomic.Int32
	task := func(int) {
		n := active.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(200 * time.Microsecond)
		active.Add(-1)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				for range phases {
					b.Run(tasks, task)
				}
			})
		}
		wg.Wait()
	}()
	waitDone(t, done)
	return peak.Load()
}

// Concurrent phases on one budget never execute more tasks at once than it has
// tokens: at a budget of one, whether a phase has one task or many, and at a
// larger budget with single-task phases, which run on their callers.
func TestRunNeverExceedsBudget(t *testing.T) {
	for _, tc := range []struct{ size, tasks int }{
		{1, 1}, {1, 8}, {3, 1}, {3, 2}, {3, 24},
	} {
		b := budgetOf(tc.size)
		peak := peakTasks(t, b, 6, 20, tc.tasks)
		require.LessOrEqual(t, peak, int32(tc.size), "budget %d, %d-task phases", tc.size, tc.tasks)
		if tc.size > 1 {
			require.Greater(t, peak, int32(1), "budget %d, %d-task phases: tasks do run in parallel", tc.size, tc.tasks)
		}
		require.Zero(t, len(b.tokens), "every token returned")
	}
}

// A phase, even of a single task, waits for a token while the budget is
// fully held.
func TestRunWaitsForAToken(t *testing.T) {
	for _, tasks := range []int{1, 4} {
		b := budgetOf(2)
		b.Acquire()
		b.Acquire()
		ran := make(chan struct{})
		go b.Run(tasks, func(task int) {
			if task == 0 {
				close(ran)
			}
		})
		select {
		case <-ran:
			t.Fatalf("%d-task phase ran with no token free", tasks)
		case <-time.After(50 * time.Millisecond):
		}
		b.Release()
		waitDone(t, ran)
		b.Release()
	}
}

// Phases back to back, from several goroutines at once, all complete: every
// worker returns its token when its phase's tasks run out.
func TestPhasesBackToBackComplete(t *testing.T) {
	for _, size := range []int{1, 2} {
		b := budgetOf(size)
		done := make(chan struct{})
		go func() {
			defer close(done)
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					for range 50 {
						b.Run(8, func(int) {})
					}
				})
			}
			wg.Wait()
		}()
		waitDone(t, done)
		require.Zero(t, len(b.tokens), "size %d: every token returned", size)
	}
}

// A worker waiting for a token gives up as soon as its phase's items have all
// been claimed, instead of waiting on tokens other phases hold. A worker that
// starts late, after another took the phase's last item, would otherwise
// queue behind those holders and keep the phase from returning.
func TestWaitingWorkerGivesUpOnceClaimed(t *testing.T) {
	b := budgetOf(1)
	b.Acquire() // another phase holds the only token for as long as it likes
	defer b.Release()
	claimed := make(chan struct{})
	got := make(chan bool, 1)
	go func() { got <- b.acquireUnless(claimed) }()
	select {
	case <-got:
		t.Fatal("stopped waiting before its phase's items were claimed")
	case <-time.After(50 * time.Millisecond):
	}
	close(claimed)
	select {
	case took := <-got:
		require.False(t, took)
	case <-time.After(10 * time.Second):
		t.Fatal("still waiting for a token after every item was claimed")
	}
}

// With one of two tokens held elsewhere, one worker does the whole phase and
// Run returns while the outside token is still held.
func TestRunWithBudgetPartlyHeld(t *testing.T) {
	b := budgetOf(2)
	b.Acquire()
	defer b.Release()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Run(4, func(int) {})
	}()
	waitDone(t, done)
	require.Equal(t, 1, len(b.tokens), "only the outside token is held")
}

// Work makes one state per worker that does items, and a worker does its items
// with its own state.
func TestWorkStatePerWorker(t *testing.T) {
	type state struct{ items []int }
	for _, workers := range []int{1, 3} {
		b := budgetOf(workers)
		var mu sync.Mutex
		var states []*state
		Work(b, workers, 50, func() *state {
			s := &state{}
			mu.Lock()
			states = append(states, s)
			mu.Unlock()
			return s
		}, func(s *state, item int) {
			s.items = append(s.items, item)
			time.Sleep(50 * time.Microsecond)
		})
		require.LessOrEqual(t, len(states), workers)
		seen := make(map[int]bool)
		for _, s := range states {
			require.NotEmpty(t, s.items, "a state is made only for a worker that does items")
			for _, item := range s.items {
				require.False(t, seen[item], "item %d done twice", item)
				seen[item] = true
			}
		}
		require.Len(t, seen, 50)
		if workers == 1 {
			require.Len(t, states, 1)
			require.True(t, slices.IsSorted(states[0].items), "one worker does the items in order")
		}
	}
}

func TestSet(t *testing.T) {
	defer Set(Current().Size())
	before := Current()
	Set(5)
	require.NotSame(t, before, Current())
	require.Equal(t, 5, Current().Size())
	Set(0)
	require.Equal(t, 1, Current().Size(), "n < 1 is treated as 1")
}
