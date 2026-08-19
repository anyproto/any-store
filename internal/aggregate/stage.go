package aggregate

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// ctxCheckEvery is how often (in consumed rows) blocking stages poll
// Ctx.Context for cancellation while draining upstream.
const ctxCheckEvery = 4096

// Stage is a pull-driven aggregation operator over anyenc documents.
//
// The chain is strictly synchronous: a stage produces a row only when its
// consumer asks for one, so row lifetimes are phase-locked across the chain.
type Stage interface {
	// Next returns the next document, or (nil, nil) at the end of the stream.
	// The returned value is valid only until the next Next call on this stage.
	Next(ctx *Ctx) (*anyenc.Value, error)

	// Close releases stage resources. It must be safe to call multiple times
	// and after a mid-stream abort.
	Close()

	// String describes the stage for Explain output. Never called on the hot
	// path.
	fmt.Stringer
}

// Ctx carries per-pipeline shared state through the stage chain.
//
// RowArena ownership: the arena is reset only by the current "row owner" —
// the source stage (once per source row), a blocking stage on its output side
// (once per emitted row), and $project/$addFields when Build proves nothing
// below them keeps arena-allocated data alive across pulls. Non-owning stages
// allocate on RowArena freely and never reset it, so synthesized values and
// zero-copy aliases of the source document stay valid for the whole life of
// the row.
type Ctx struct {
	Context  context.Context
	RowArena *anyenc.Arena
	Buf      *syncpool.DocBuffer
	Mem      *MemAccount
}

// MemAccount is a coarse retained-bytes budget shared by the blocking stages
// of one pipeline ($group state, $sort arena). Streaming stages retain
// nothing and don't report.
type MemAccount struct {
	limit int
	used  int
}

func NewMemAccount(limit int) *MemAccount {
	return &MemAccount{limit: limit}
}

// Add records n retained bytes and fails with ErrMemoryLimitExceeded once the
// budget is exhausted. n may be negative when a stage shrinks/releases.
func (m *MemAccount) Add(n int) error {
	if m == nil {
		return nil
	}
	m.used += n
	if m.limit > 0 && m.used > m.limit {
		return ErrMemoryLimitExceeded
	}
	return nil
}

// Used returns the currently accounted retained bytes.
func (m *MemAccount) Used() int {
	if m == nil {
		return 0
	}
	return m.used
}

// Limits bounds the memory-unbounded parts of a pipeline. Zero values mean
// "use default"; negative values mean "unlimited".
type Limits struct {
	// MaxGroups caps the number of unique $group keys.
	MaxGroups int
	// MaxAccumArrayLen caps $push / $addToSet result arrays.
	MaxAccumArrayLen int
	// MaxMemoryBytes caps retained bytes across all blocking stages.
	MaxMemoryBytes int
}

const (
	DefaultGroupLimit      = 50_000
	DefaultAccumArrayLimit = 10_000
	DefaultMemoryLimit     = 256 << 20
)

func (l Limits) WithDefaults() Limits {
	if l.MaxGroups == 0 {
		l.MaxGroups = DefaultGroupLimit
	}
	if l.MaxAccumArrayLen == 0 {
		l.MaxAccumArrayLen = DefaultAccumArrayLimit
	}
	if l.MaxMemoryBytes == 0 {
		l.MaxMemoryBytes = DefaultMemoryLimit
	}
	return l
}
