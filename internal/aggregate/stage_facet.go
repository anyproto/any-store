package aggregate

import (
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// errFacetPause is the internal control-flow sentinel of the facet fan-out: a
// facetFeed returns it when a sub-pipeline pulls past the row it was fed.
// Every stage propagates errors unchanged without corrupting its state, so
// the paused sub-chain resumes exactly where it stopped on the next drive.
// The sentinel never escapes FacetStage.
var errFacetPause = errors.New("aggregate: facet feed paused")

// facetFeed is the source stage of one facet sub-pipeline: it yields the one
// row the facet stage fed it, then pauses the chain until the next row (or
// yields end-of-stream once the shared input is drained).
type facetFeed struct {
	row  *anyenc.Value
	done bool
}

func (f *facetFeed) Next(ctx *Ctx) (*anyenc.Value, error) {
	if f.row != nil {
		v := f.row
		f.row = nil
		// Row owner of the facet's private arena (Build's source contract):
		// the previous round's temporaries are dead — every sub-pipeline
		// output was marshaled into the result buffer before this row was fed.
		ctx.RowArena.Reset()
		return v, nil
	}
	if f.done {
		return nil, nil
	}
	return nil, errFacetPause
}

func (f *facetFeed) Close() {}

func (f *facetFeed) String() string { return "facet-feed" }

// facetPipe is one named sub-pipeline of a FacetStage.
type facetPipe struct {
	name string
	feed *facetFeed
	root Stage

	// ctx is the facet's private execution context: its own arena and scratch
	// buffer (mirroring the root wiring where RowArena is the DocBuffer's
	// arena), so sub-chain arena resets can never free the shared source row
	// or another facet's data. Context and Mem are refreshed from the outer
	// Ctx on every drive.
	ctx Ctx
	buf *syncpool.DocBuffer

	// copyRow: the sub-pipeline mutates its input rows in place, so it is fed
	// a private re-parse of the row instead of the shared document. This keeps
	// mutations facet-local even when a mid-stream $limit finishes the chain
	// before the mutating stage's undo runs.
	copyRow   bool
	rowParser anyenc.Parser

	// res is the facet's result array assembled as raw anyenc bytes: the
	// leading array tag, then every emitted document marshaled in order (the
	// terminating EOS is appended at assembly). Parsed once at emit by the
	// stage-owned parser, so the arrays of all facets are alive together in
	// the single output document.
	res      []byte
	parser   anyenc.Parser
	finished bool // root hit end-of-stream early (a $limit was satisfied)
}

// FacetStage implements $facet: it consumes its entire input stream, feeding
// each row to every sub-pipeline incrementally — streaming sub-stages process
// per-row, blocking sub-stages buffer per their own logic — then emits exactly
// one document {name: [results], ...}.
//
// The fan-out is synchronous pull inversion: each row is pushed by parking it
// in the facet's feed and driving the sub-chain until the feed pauses it
// (errFacetPause). The drive-until-pause protocol guarantees the sub-chain
// quiesces every round — a pause only surfaces through an upstream pull, and
// every in-place mutator undoes its overlay strictly before pulling — so
// non-copying facets always hand the shared row back unmodified. Emitted
// sub-pipeline documents are marshaled into the facet's result buffer
// immediately (they are only valid until the sub-chain is driven again);
// buffered result bytes count against the pipeline's shared memory budget.
//
// A $match at the head of a facet filters the shared stream in-flight;
// per-facet predicate pushdown (one union scan tagging rows per facet) is a
// future optimization.
type FacetStage struct {
	Src    Stage
	Spec   FacetSpec
	facets []facetPipe

	rowBuf []byte // shared row marshaled once per round for copying facets
	built  bool
	done   bool
	err    error // latched failure: repeated Next calls must not resume a half-fed scan
}

func newFacetStage(src Stage, spec FacetSpec, limits Limits, env Env) (*FacetStage, error) {
	s := &FacetStage{Src: src, Spec: spec, facets: make([]facetPipe, len(spec.Names))}
	for i := range spec.Names {
		f := &s.facets[i]
		f.name = spec.Names[i]
		f.feed = &facetFeed{}
		f.copyRow = mutatesInput(spec.Pipelines[i])
		f.buf = &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}}
		f.ctx = Ctx{RowArena: f.buf.Arena, Buf: f.buf}
		root, err := Build(f.feed, spec.Pipelines[i], limits, env)
		if err != nil {
			return nil, err
		}
		f.root = root
		f.res = append(f.res, byte(anyenc.TypeArray))
	}
	return s, nil
}

// mutatesInput reports whether the sub-pipeline mutates the rows it is fed in
// place before any stage replaces them with fresh documents: $addFields,
// $lookup and $unwind overlay/repoint fields of the incoming document, while
// $project/$group/$sort/$count emit facet-owned rows, making later mutators
// internal to the facet.
func mutatesInput(p Pipeline) bool {
	for _, spec := range p {
		switch spec.(type) {
		case AddFieldsSpec, LookupSpec, UnwindSpec:
			return true
		case ProjectSpec, GroupSpec, SortSpec, CountSpec:
			return false
		}
	}
	return false
}

func (s *FacetStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if s.err != nil {
		return nil, s.err
	}
	if !s.built {
		if err := s.run(ctx); err != nil {
			s.err = err
			return nil, err
		}
		s.built = true
	}
	if s.done {
		return nil, nil
	}
	s.done = true
	// The drain is over: nothing below is live, so the stage owns the row
	// arena for its single emission. Facet arrays are parsed from the
	// stage-owned result buffers; the wrapping object lives on RowArena.
	ctx.RowArena.Reset()
	out := ctx.RowArena.NewObject()
	for i := range s.facets {
		f := &s.facets[i]
		f.res = append(f.res, anyenc.EOS)
		arr, err := f.parser.ParseOwned(f.res)
		if err != nil {
			s.err = err
			return nil, err
		}
		out.Set(f.name, arr)
	}
	return out, nil
}

// run drains the shared input, fanning each row out to every unfinished
// facet, then unblocks the feeds so blocking sub-stages emit.
func (s *FacetStage) run(ctx *Ctx) error {
	live := len(s.facets)
	for rows := 1; live > 0; rows++ { // all facets finished early: stop scanning
		v, err := s.Src.Next(ctx)
		if err != nil {
			return err
		}
		if v == nil {
			break
		}
		if rows%ctxCheckEvery == 0 {
			if cerr := ctx.Context.Err(); cerr != nil {
				return cerr
			}
		}
		marshaled := false
		for i := range s.facets {
			f := &s.facets[i]
			if f.finished {
				continue
			}
			row := v
			if f.copyRow {
				if !marshaled {
					s.rowBuf = v.MarshalTo(s.rowBuf[:0])
					marshaled = true
				}
				if row, err = f.rowParser.ParseOwned(s.rowBuf); err != nil {
					return err
				}
			}
			f.feed.row = row
			if err = s.drive(ctx, f); err != nil {
				return err
			}
			if f.finished {
				live--
			}
		}
	}
	for i := range s.facets {
		f := &s.facets[i]
		f.feed.done = true
		if f.finished {
			continue
		}
		if err := s.drive(ctx, f); err != nil {
			return err
		}
	}
	return nil
}

// drive pulls the sub-pipeline until it pauses on its feed (round complete,
// chain quiesced) or ends, marshaling every emitted document into the
// facet's result buffer against the shared memory budget.
func (s *FacetStage) drive(ctx *Ctx, f *facetPipe) error {
	f.ctx.Context, f.ctx.Mem = ctx.Context, ctx.Mem
	for {
		out, err := f.root.Next(&f.ctx)
		if err != nil {
			if err == errFacetPause {
				return nil
			}
			return err
		}
		if out == nil {
			f.finished = true
			f.feed.row = nil // a $limit may finish without consuming the row
			return nil
		}
		prev := len(f.res)
		f.res = out.MarshalTo(f.res)
		if err = ctx.Mem.Add(len(f.res) - prev); err != nil {
			return err
		}
	}
}

func (s *FacetStage) Close() {
	s.Src.Close()
	for i := range s.facets {
		s.facets[i].root.Close()
	}
}

func (s *FacetStage) String() string { return s.Spec.String() }
