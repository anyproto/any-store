package anystore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/aggregate"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// AggQuery is a MongoDB-style aggregation pipeline over a collection.
//
// The longest pushable pipeline prefix ($match chains, then $sort/$skip/
// $limit in canonical order) is compiled into a regular query and executed by
// the access planner — indexes, CBO, full-text ($text) and vector sources all
// apply, including their _score/_distance virtual fields. The remaining
// stages stream over the result inside the same read transaction. $expr
// predicates in $match are always residual per-document conditions: the
// ordinary filter keys around them still push down, the expressions never
// become index bounds (see docs/aggregation.md).
//
// An AggQuery and the Iterator it produces are single-goroutine (never shared
// across goroutines): compiled expressions carry per-pipeline scratch state
// (e.g. $concat's buffer) and are not reentrant.
type AggQuery interface {
	// GroupLimit overrides the maximum number of unique $group keys
	// (default aggregate.DefaultGroupLimit; negative = unlimited).
	GroupLimit(n int) AggQuery

	// AccumArrayLimit overrides the maximum $push / $addToSet array length
	// (default aggregate.DefaultAccumArrayLimit; negative = unlimited).
	AccumArrayLimit(n int) AggQuery

	// MemoryLimit overrides the retained-bytes budget shared by the
	// pipeline's blocking stages — $group state and in-pipeline $sort
	// (default aggregate.DefaultMemoryLimit; negative = unlimited).
	MemoryLimit(bytes int) AggQuery

	// IndexHint adds or removes boost for indexes considered by the pushed
	// prefix's query plan.
	IndexHint(hints ...IndexHint) AggQuery

	// Iter executes the pipeline and returns an Iterator over result
	// documents. Score and Distance report 0 on aggregation iterators; for a
	// full-text or vector $match prefix the values remain available as the
	// _score / _distance document fields. See Iterator for the
	// mutation-while-iterating contract (undefined, SQLite-style).
	Iter(ctx context.Context) (Iterator, error)

	// Count executes the pipeline and returns the number of result documents.
	Count(ctx context.Context) (int, error)

	// Explain returns the access plan of the pushed prefix and the list of
	// in-pipeline stages.
	Explain(ctx context.Context) (Explain, error)
}

func (c *collection) Aggregate(pipeline any) AggQuery {
	q := &aggQuery{c: c}
	q.pipeline, q.err = aggregate.ParsePipeline(pipeline)
	return q
}

// AggregateStages returns the stage vocabulary accepted by Aggregate's
// pipeline parser — every stage key it recognizes ($set being $addFields'
// alias), sorted and with the leading '$'. Like query.Operators, it is the
// parser's own vocabulary exported as data: use it to advertise the grammar
// (docs, error payloads) instead of hand-copying the list. A pipeline that
// does not parse is reported as a *query.ParseError with Source "pipeline",
// whose Path's leading segment is the failing stage's index.
func AggregateStages() []string { return aggregate.Stages() }

// AggregateAccumulators returns the $group accumulator vocabulary accepted by
// Aggregate's pipeline parser, sorted and with the leading '$'.
func AggregateAccumulators() []string { return aggregate.Accumulators() }

type aggQuery struct {
	c        *collection
	pipeline aggregate.Pipeline
	limits   aggregate.Limits
	hints    []IndexHint
	err      error
}

func (q *aggQuery) GroupLimit(n int) AggQuery {
	q.limits.MaxGroups = n
	return q
}

func (q *aggQuery) AccumArrayLimit(n int) AggQuery {
	q.limits.MaxAccumArrayLen = n
	return q
}

func (q *aggQuery) MemoryLimit(bytes int) AggQuery {
	q.limits.MaxMemoryBytes = bytes
	return q
}

func (q *aggQuery) IndexHint(hints ...IndexHint) AggQuery {
	q.hints = hints
	return q
}

// prefixQuery compiles the pushable pipeline prefix into a regular collection
// query and returns the remaining in-pipeline stages.
func (q *aggQuery) prefixQuery() (*collQuery, aggregate.Pipeline, error) {
	prefix, rest := aggregate.SplitPrefix(q.pipeline)
	if err := q.validateInPipelineStages(rest); err != nil {
		return nil, nil, err
	}
	return &collQuery{
		c:          q.c,
		cond:       prefix.Filter,
		sort:       prefix.Sort,
		limit:      uint(prefix.Limit),
		offset:     uint(prefix.Skip),
		indexHints: q.hints,
	}, rest, nil
}

// validateInPipelineStages rejects stages that would execute with silently
// wrong semantics outside the pushdown prefix:
//
//   - query.Text is a residual-only predicate (Ok always passes — matching is
//     done by the FTS scan that the planner builds), so a $match with $text
//     that did not reach the access planner would match every row instead of
//     searching.
//   - query.Knn is a ranked source, not a predicate (Ok fails closed), so a
//     $match with $knn that did not reach the access planner would match no
//     row instead of searching.
//   - a legacy bare-array ANN clause degrades to a literal array comparison
//     here (0 rows on packed storage); it is a hard error on every path, and
//     an in-pipeline $match — which compilePlan's guard never sees, because
//     only the pushdown prefix reaches the compiler — must reject it itself.
func (q *aggQuery) validateInPipelineStages(rest aggregate.Pipeline) error {
	var (
		vidxs  []*vectorIndex
		loaded bool
	)
	for _, spec := range rest {
		switch sp := spec.(type) {
		case aggregate.LookupSpec:
			// $lookup is scoped to a self-join on the primary key: "from" may
			// only name the aggregated collection (the parser can't know its
			// name), and the "id" foreign field must actually be the pk.
			if sp.From != "" && sp.From != q.c.name {
				return fmt.Errorf("%w: from %q, aggregating %q", errAggLookupFrom, sp.From, q.c.name)
			}
			if q.c.primaryKey != "id" {
				return fmt.Errorf("%w: collection %q's primary key is %q", errAggLookupPrimaryKey, q.c.name, q.c.primaryKey)
			}
		case aggregate.MatchSpec:
			if query.ContainsText(sp.Filter) {
				return errAggTextNotInPrefix
			}
			if query.ContainsKnn(sp.Filter) {
				return errAggVectorNotInPrefix
			}
			if !loaded {
				vidxs, loaded = q.c.loadVectorIndexes(), true
			}
			if err := rejectLegacyVectorClause(sp.Filter, vidxs); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	errAggTextNotInPrefix   = errors.New("any-store: aggregate: $text is only supported in the leading $match stages (the pushdown prefix)")
	errAggVectorNotInPrefix = errors.New("any-store: aggregate: a vector ($knn) clause is only supported in the leading $match stages (the pushdown prefix)")
	errAggLookupFrom        = errors.New(`any-store: aggregate: $lookup joins only the aggregated collection itself ("from" must match it or be omitted)`)
	errAggLookupPrimaryKey  = errors.New(`any-store: aggregate: $lookup requires the primary key field "id"`)
)

func (q *aggQuery) Iter(ctx context.Context) (Iterator, error) {
	if q.err != nil {
		return nil, q.err
	}
	// Blocking stages poll the context only every few thousand rows; an
	// already-cancelled context should not start the pipeline at all.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cq, rest, err := q.prefixQuery()
	if err != nil {
		return nil, err
	}
	inner, err := cq.Iter(ctx)
	if err != nil {
		return nil, err
	}

	var (
		env      aggregate.Env
		lookupTx ReadTx
	)
	if aggregate.HasLookup(rest) {
		var btx *btree.ReadTx
		if btx, lookupTx, err = q.lookupBtreeTx(ctx, inner); err != nil {
			_ = inner.Close()
			return nil, err
		}
		env.Lookup = q.c.lookupFunc(btx)
	}

	limits := q.limits.WithDefaults()
	root, err := aggregate.Build(&sourceStage{iter: inner}, rest, limits, env)
	if err != nil {
		_ = inner.Close()
		if lookupTx != nil {
			_ = lookupTx.Commit()
		}
		return nil, err
	}

	// The pipeline needs its own buffer: the inner planIterator owns the one
	// it parses stored documents into.
	buf := q.c.db.syncPool.GetDocBuf()
	return &aggIterator{
		root:     root,
		inner:    inner,
		lookupTx: lookupTx,
		c:        q.c,
		buf:      buf,
		actx: &aggregate.Ctx{
			Context:  ctx,
			RowArena: buf.Arena,
			Buf:      buf,
			Mem:      aggregate.NewMemAccount(limits.MaxMemoryBytes),
		},
	}, nil
}

// lookupBtreeTx returns the btree read tx $lookup point reads run against:
// the streaming iterator's own tx (same snapshot; the iterator holds it open
// until Close, which happens after every stage — blocking ones included —
// has finished). When the pushdown prefix is provably empty the iterator has
// no tx at all, yet a $count downstream can still synthesize rows whose
// fields feed $lookup — then a dedicated read tx is opened, returned as held
// for aggIterator to release on Close.
func (q *aggQuery) lookupBtreeTx(ctx context.Context, inner Iterator) (btx *btree.ReadTx, held ReadTx, err error) {
	if pi, ok := inner.(*planIterator); ok {
		return pi.tx.btreeReadTx(), nil, nil
	}
	tx, err := q.c.db.getReadTx(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err = q.c.alive(); err != nil {
		_ = tx.Commit()
		return nil, nil, err
	}
	return tx.btreeReadTx(), tx, nil
}

// lookupFunc serves $lookup point reads against btx. A missing key is a
// non-match, not an error. The document is parsed owned into buf (stage-owned
// scratch): valid until the stage reuses the same buf.
func (c *collection) lookupFunc(btx *btree.ReadTx) aggregate.LookupFunc {
	return func(key []byte, buf *syncpool.DocBuffer) (*anyenc.Value, error) {
		var err error
		buf.DocBuf, err = btx.AppendValue(c.ns, key, buf.DocBuf[:0])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return buf.Parser.ParseOwned(buf.DocBuf)
	}
}

func (q *aggQuery) Count(ctx context.Context) (count int, err error) {
	iter, err := q.Iter(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := iter.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	for iter.Next() {
		count++
	}
	return count, iter.Err()
}

func (q *aggQuery) Explain(ctx context.Context) (explain Explain, err error) {
	if q.err != nil {
		return Explain{}, q.err
	}
	cq, rest, err := q.prefixQuery()
	if err != nil {
		return Explain{}, err
	}
	// Capture the pushdown shape before Explain runs: makeQuery normalizes a
	// nil cond to query.All.
	pushFilter, pushSort := cq.cond, cq.sort
	pushSkip, pushLimit := cq.offset, cq.limit
	if explain, err = cq.Explain(ctx); err != nil {
		return
	}
	var b strings.Builder
	b.WriteString(explain.Plan)
	if pushFilter != nil || pushSort != nil || pushLimit > 0 || pushSkip > 0 {
		b.WriteString("\nPushdown:")
		if pushFilter != nil {
			fmt.Fprintf(&b, " filter=%s", pushFilter)
		}
		if pushSort != nil {
			b.WriteString(" sort")
		}
		if pushSkip > 0 {
			fmt.Fprintf(&b, " skip=%d", pushSkip)
		}
		if pushLimit > 0 {
			fmt.Fprintf(&b, " limit=%d", pushLimit)
		}
		b.WriteByte('\n')
	}
	if len(rest) > 0 {
		b.WriteString("Stages:\n")
		b.WriteString(aggregate.ExplainStages(rest))
	}
	explain.Plan = b.String()
	return explain, nil
}

// sourceStage bridges the access plan's public Iterator into the aggregation
// stage chain. It is the default row owner: one RowArena reset per stored
// row. Closing is left to aggIterator, which owns the inner iterator.
type sourceStage struct {
	iter Iterator
}

func (s *sourceStage) Next(actx *aggregate.Ctx) (*anyenc.Value, error) {
	actx.RowArena.Reset()
	if !s.iter.Next() {
		return nil, s.iter.Err()
	}
	doc, err := s.iter.Doc()
	if err != nil {
		return nil, err
	}
	return doc.Value(), nil
}

func (s *sourceStage) Close() {}

func (s *sourceStage) String() string { return "source" }

// aggDoc is the Doc implementation for aggregation results. Unlike stored
// documents, results are not required to carry an id field ($count output,
// for example), so it does not go through newItem.
type aggDoc struct {
	v *anyenc.Value
}

func (d aggDoc) Value() *anyenc.Value { return d.v }

type aggIterator struct {
	root  aggregate.Stage
	inner Iterator
	// lookupTx is the dedicated $lookup read tx opened when the inner
	// iterator has none of its own (empty-prefix fast path); nil otherwise.
	lookupTx ReadTx
	actx     *aggregate.Ctx
	c        *collection
	buf      *syncpool.DocBuffer
	cur      *anyenc.Value
	err      error
	closed   bool
}

func (it *aggIterator) Next() bool {
	if it.err != nil || it.closed {
		return false
	}
	v, err := it.root.Next(it.actx)
	if err != nil {
		it.err = err
		return false
	}
	if v == nil {
		return false
	}
	it.cur = v
	return true
}

func (it *aggIterator) Doc() (Doc, error) {
	if it.err != nil {
		return nil, it.err
	}
	if it.cur == nil {
		return nil, ErrDocNotFound
	}
	return aggDoc{v: it.cur}, nil
}

func (it *aggIterator) Distance() float32 { return 0 }

func (it *aggIterator) Score() float64 { return 0 }

func (it *aggIterator) Err() error { return it.err }

func (it *aggIterator) Close() error {
	if it.closed {
		return ErrIterClosed
	}
	it.closed = true
	it.root.Close()
	err := it.inner.Close()
	if it.lookupTx != nil {
		err = errors.Join(err, it.lookupTx.Commit())
	}
	it.c.db.syncPool.ReleaseDocBuf(it.buf)
	return err
}
