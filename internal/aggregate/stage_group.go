package aggregate

import (
	"unsafe"

	"github.com/anyproto/any-store/v2/anyenc"
)

var nullValue = anyenc.MustParseJson(`null`)

// groupRec is one group's state: the key's span in GroupStage.keyBuf and the
// group's accumulator instances.
type groupRec struct {
	keyOff, keyLen uint32
	accs           []Accumulator
}

// GroupStage implements $group: a blocking hash aggregation.
//
// Group keys are the marshaled anyenc bytes of the key expression (missing →
// null, Mongo semantics); the byte encoding is canonical, so byte equality is
// value equality. Lookups are alloc-free (map[string]X with a string(bytes)
// conversion the compiler elides); new keys are interned into a grow-only
// buffer with unsafe.String map keys (the query.In pattern). Group states
// live in one slab slice in first-seen order, which is also the emit order
// (unspecified for callers).
type GroupStage struct {
	Src    Stage
	Spec   GroupSpec
	Limits Limits

	factories []accFactory

	idx    map[string]uint32
	groups []groupRec
	keyBuf []byte

	scratch   []byte
	keyParser anyenc.Parser

	built bool
	emit  int
}

func newGroupStage(src Stage, spec GroupSpec, limits Limits) *GroupStage {
	return &GroupStage{
		Src:       src,
		Spec:      spec,
		Limits:    limits,
		factories: makeAccFactories(spec.Accums, limits),
		idx:       make(map[string]uint32),
	}
}

func (s *GroupStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if !s.built {
		if err := s.drain(ctx); err != nil {
			return nil, err
		}
		s.built = true
	}
	if s.emit >= len(s.groups) {
		return nil, nil
	}
	// The stage is the row owner on its output side: one reset per emitted row.
	ctx.RowArena.Reset()
	g := &s.groups[s.emit]
	s.emit++

	out := ctx.RowArena.NewObject()
	kv, err := s.keyParser.Parse(s.keyBuf[g.keyOff : g.keyOff+g.keyLen])
	if err != nil {
		return nil, err
	}
	out.Set("id", kv)
	for i := range s.Spec.Accums {
		fv, err := g.accs[i].Finalize(ctx.RowArena)
		if err != nil {
			return nil, err
		}
		if fv == nil {
			continue // missing result omits the field ($first/$last of missing)
		}
		out.Set(s.Spec.Accums[i].Name, fv)
	}
	return out, nil
}

func (s *GroupStage) drain(ctx *Ctx) error {
	for rows := 1; ; rows++ {
		more, err := s.stepOne(ctx)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
		if rows%ctxCheckEvery == 0 {
			if cerr := ctx.Context.Err(); cerr != nil {
				return cerr
			}
		}
	}
}

// stepOne consumes one upstream row into the group states; more is false at
// the end of the stream.
func (s *GroupStage) stepOne(ctx *Ctx) (more bool, err error) {
	v, err := s.Src.Next(ctx)
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}

	kv, err := s.Spec.Key.Eval(ctx.RowArena, v)
	if err != nil {
		return false, err
	}
	if kv == nil {
		kv = nullValue
	}
	s.scratch = kv.MarshalTo(s.scratch[:0])

	gi, ok := s.idx[string(s.scratch)]
	if !ok {
		if gi, err = s.newGroup(ctx); err != nil {
			return false, err
		}
	}
	g := &s.groups[gi]
	for i := range s.Spec.Accums {
		var av *anyenc.Value
		if arg := s.Spec.Accums[i].Arg; arg != nil {
			if av, err = arg.Eval(ctx.RowArena, v); err != nil {
				return false, err
			}
		}
		if err = g.accs[i].Step(av); err != nil {
			return false, err
		}
	}
	return true, nil
}

// newGroup interns the key currently in s.scratch and allocates the group's
// accumulators. Returns the new group's slab index.
func (s *GroupStage) newGroup(ctx *Ctx) (uint32, error) {
	if s.Limits.MaxGroups > 0 && len(s.groups) >= s.Limits.MaxGroups {
		return 0, ErrGroupLimitExceeded
	}
	start := len(s.keyBuf)
	s.keyBuf = append(s.keyBuf, s.scratch...)
	span := s.keyBuf[start:]
	key := unsafe.String(unsafe.SliceData(span), len(span))

	accs := make([]Accumulator, len(s.factories))
	for i, f := range s.factories {
		accs[i] = f(ctx.Mem)
	}
	gi := uint32(len(s.groups))
	s.groups = append(s.groups, groupRec{
		keyOff: uint32(start),
		keyLen: uint32(len(span)),
		accs:   accs,
	})
	s.idx[key] = gi
	// Coarse per-group accounting: key bytes plus a fixed estimate for the
	// map entry, slab record and accumulator instances. Packed accumulator
	// buffers ($push/$addToSet) report their own growth.
	return gi, ctx.Mem.Add(len(span) + 96 + 48*len(accs))
}

func (s *GroupStage) Close() { s.Src.Close() }

func (s *GroupStage) String() string { return s.Spec.String() }
