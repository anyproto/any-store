# FTS v2 — Query Operators & Per-Field Weights — Plan

Status: **planned** (branch `fts-phrase-and-prefix` implements Phase 0+1).

Follows `DESIGN.md` (the locked v1 substrate) and `FTS-TODO.md` (the production
assessment). This document is the implementation plan for the next phase of
full-text search, plus the decisions that are expensive to reverse. It was
reviewed with an outside expert (see "Decision record" at the end).

## Motivation (from FTS-TODO.md §3)

v1 is bag-of-words OR only. The gaps that surface in app UX, in leverage order:

1. **Phrase + required-term (AND).** Positions are already persisted, so phrase
   is mostly a search-time change. AND/required fixes the OR-noise problem at the
   source (better than app-side stop-word stripping).
2. **Per-field weights.** Index `title`/`heading` as a boosted field instead of
   the prepend-the-title hack — directly improves "find the doc about X".
3. **Prefix / wildcard (`foo*`).** Recovers much of what stemming would give
   without a language model; the key enabler for search-as-you-type.

Optional / lower priority: configurable BM25 `b`; per-index stemming;
query-time `nProbe` for the IVF vector leg (not FTS, tracked here for
convenience).

## Core decisions

### D1 — BM25F reduces exactly to classic BM25 (keep one oracle)

The v1 correctness anchor is "engine ranking == brute-force BM25 oracle EXACTLY,
same float64 summation order." Canonical (Robertson) BM25F normalizes per-field
TF by `B_f = 1 − b_f + b_f·L_f/avgL_f` **before** the `k1` saturation:

```
TF_combined = Σ_f  boost_f · TF_f / B_f
score       = IDF · TF_combined / (TF_combined + k1)
```

For a single field with `boost = 1`, `b = 0.75`, this collapses algebraically to
today's classic per-term BM25 — bit-for-bit. So per-field weights are a true
**superset**: a single-field/default index keeps the existing ranking and the
existing oracle unchanged; multi-field simply re-oracles against BM25F. This
removes the biggest risk (silently changing every score).

### D2 — decouple the phrase algorithm from the byte layout

Introduce a `ChunkIterator` over a term's postings:

```
Next() (docID uint64, tf uint32, ok bool)   // decodes DocID+TF, skips positions
Positions() []uint32                          // lazily decodes current doc's positions
Err() error
```

The phrase zig-zag merge and the scan loop consume **only** this interface, so
they are blind to whether the underlying chunk is v1 or v2. `V1ChunkIterator`
wraps today's `ChunkReader` (which already has the lazy-positions shape);
`V2ChunkIterator` (Phase 3) reads the `FieldMask` + per-field TFs and still skips
positions during `Next()`. Search-time features ship on v1 now; the format break
lands later with zero change to the merge code.

### D3 — ship order: search-time features first, format break second

Phase 1 (phrase/AND/prefix) needs no format change and no re-index. Phase 3
(per-field weights) bumps the postings + docinfo version bytes and forces a
blocking re-index on upgrade. Ship Phase 1 first for immediate UX; do the single
re-index once, later.

### D4 — per-field weights = Option A (single postings list + FieldMask)

Rejected alternatives: **B** (separate postings keyspace per field — destroys the
hot common-term path with N B-tree scans + N-way merge per term, inflates vocab,
makes IDF per-field-weird, doubles page-dirtying when a term moves field); **C**
(field-band positions — caps field length and forces position decode on the hot
scoring path). Option A keeps one postings list, one global DF, one chunk scan
per term, and cheap incremental updates.

## Query-syntax surface (Phase 0/1)

`$search` carries free text + low-ambiguity inline syntax; boolean require/exclude
live in **typed sub-fields**, not inline string signs. Default operator stays
**OR** (Mongo parity); `$defaultOperator:"and"` makes bare `$search` terms
required.

```json
{
  "$text": {
    "$search": "east tokyo \"new york\" pre*",
    "$defaultOperator": "and",
    "$require": ["critical", "\"east side\""],
    "$exclude": ["draft", "spam*"]
  }
}
```

| Field / syntax              | Meaning                                                          |
|-----------------------------|------------------------------------------------------------------|
| `$search` bare term         | should/OR by default, required when `$defaultOperator:"and"`     |
| `$search` `"east tokyo"`    | phrase — exact adjacency (positional merge)                      |
| `$search` `pre*`            | prefix — expand against vocab (top-M by df), OR the expansions   |
| `$require` (string / array) | each entry required (AND); reuses phrase/prefix syntax           |
| `$exclude` (string / array) | each entry excludes matching docs; reuses phrase/prefix syntax   |

**No inline `+`/`-` boolean syntax** (decided 2026-06-17, with outside-expert
review). Parsing boolean intent out of a raw human search string is a footgun for
an embedded library whose common use is "forward the end-user's search box
straight into `$search`": `-9` (meaning −9°C) or `+1` (a vote) would silently
become an exclusion/require. Here those characters are harmless — the analyzer
drops them, so `-9` is just the term `9`. Require/exclude move to the structured
`$require`/`$exclude` arrays: unambiguous, JSON-native (an "Advanced Search" UI
maps straight to them, no escaping), and run through the same analyzer so they
match the index (a generic `$not:{body:"Crash"}` would force the app to manually
case-fold/normalize, and would execute as a slow two-query intersection instead
of a fast in-accumulator tombstone). An app that wants single-box power-user
syntax can parse `-foo` itself and populate `$exclude`. Rejected alternatives:
digit-guarding `+`/`-` (leaky — fails on `-Apple`, `- foo`, `+A`); an opt-in
operators flag (pushes escaping back onto the app); strict Mongo parity (Mongo's
inline `-` is a known footgun, and `$defaultOperator` already fixes OR-noise).

Traps (enforced):

- **No `*` inside quotes.** Prefix-expansion inside a positional adjacency merge
  is combinatorial; the `*` is left literal for the analyzer to drop.
- **Prefix detection before analysis.** UAX#29 strips `*` as punctuation, so the
  parser strips the trailing `*`, analyzes the stem, and tags the clause prefix.
- **Hard cap on prefix expansion** (top-M by df, `ftsPrefixMaxExpansions = 50`).

## Scoring mechanics (Phase 1)

- **Phrase = synthetic term.** After the zig-zag merge confirms adjacency in a
  doc: `TF = adjacency-confirmed count`, `IDF = Σ idf(termᵢ)`, scored with the
  normal BM25 length term. (Sum-of-constituent-contributions would wildly inflate
  when a constituent is frequent but rarely adjacent.)
- **Required/AND = bitmask post-pass.** Each required clause gets a bit; the
  accumulator slot carries `requiredMask`; the rank/extract pass drops any doc
  whose `requiredMask != (1<<nRequired)-1`. Single pass, no extra structure.
- **Negation = tombstone.** Scan the negated term's chunks; mark existing
  accumulator entries tombstoned (never insert new ones); skip at extract.
- Future opt (not now): seed the accumulator from the lowest-df required clause,
  then only score other clauses against docs already present. Current scale
  doesn't need it.

## On-disk format for per-field weights (Phase 3)

Postings chunk **v2** (version byte bumped), per document:

```
DocIDDelta : uvarint
FieldMask  : uvarint            // bit f set ⇒ term occurs in field f
TF_f       : uvarint × popcount(FieldMask)   // per set bit, in field order
PosDelta   : uvarint × Σ TF_f   // GLOBAL gapped positions (unchanged from v1)
```

- Positions stay **global + field-gapped** — phrase merge unchanged; the gap
  still blocks cross-field phrases. Per-field weighting uses only the TF vector.
- `FieldMask` is 1 byte for ≤7 fields; empty fields cost 0 TF bytes.
  **Cap FTS-indexed fields at 64** (fits a uint64 mask).
- **`fts:vocab` unchanged** — DF stays global per term (corpus rarity drives IDF;
  a term in both title and body of one doc is df=1).
- **`fts:docinfo` v2**: `[Len_field0, Len_field1, …]` varints instead of one len.
- **`fts:meta`**: track `TotalTokens` per field (for `avgL_f`); store the field
  list so `fieldIdx` is stable across reopen.
- **`V2ChunkIterator`** reads `FieldMask`+TFs eagerly, skips positions in `Next()`
  (varint count known) → hot common-term path still never decodes positions.
- **Incremental update** stays one-chunk-per-term: the diff path carries per-field
  TF; moving a term title→body rewrites one chunk, not two.
- **Migration**: a v1 chunk under a weighted index ⇒ blocking re-index on open
  (acceptable in alpha; no FTS on-disk back-compat promise yet).

### Settled API — `FulltextParams` (locked 2026-06-17)

One struct serves both Phase 2 (global `b`/`k1`) and Phase 3 (`Weights`), so
neither phase churns it. `Weights` mirrors MongoDB's text-index `weights` option
(keyed by field name, default 1.0); the scoring is BM25F, not Mongo's heuristic.

```go
type FulltextParams struct {
    // Weights is the per-field BM25F boost, keyed by indexed field name (must
    // match an entry of IndexInfo.Fields). A field absent from the map weighs
    // 1.0. Mongo-compatible shape. Set at index creation; changing a weight
    // re-indexes. Phase 3.
    Weights map[string]float64 `json:"weights,omitempty"`

    // B is the BM25 length-normalization parameter (global). 0 ⇒ 0.75. In BM25F
    // this is the length term applied per field; a single global B is the common
    // variant (per-field b is intentionally NOT exposed — niche, and adding the
    // optional map later is non-breaking). Phase 2.
    B float64 `json:"b,omitempty"`

    // K1 is the BM25 term-frequency saturation parameter (global). 0 ⇒ 1.2.
    // Phase 2.
    K1 float64 `json:"k1,omitempty"`
}
```

- **Field→index resolution:** `Weights` is keyed by the declared field string;
  the writer resolves each to its `fieldIdx` (position in `IndexInfo.Fields`).
  A weight for a field not in `Fields` is a validation error at `EnsureIndex`
  (catches typos) — not silently ignored.
- **Persistence:** a `fulltext` sub-object on the index doc, mirroring the
  `vector` sub-object (`db.go`): `{ "weights": {field: w, …}, "b": …, "k1": … }`.
  The `db.go:969` loader (today `info.Fulltext = &FulltextParams{}`) populates it.
- **Zero-value = today's behavior:** empty map, `B=0→0.75`, `K1=0→1.2` reproduces
  the current uniform-field BM25 exactly (the D1 single-field/weight-1 reduction).
- **Dependency:** Phase 3 does NOT depend on Phase 2 — they only share this struct
  and the `b`/`k1` parameters (Phase 2's global `b` is the scalar case of BM25F's
  length term). Phase 2 may ship first (config-only, no migration) using `B`/`K1`;
  Phase 3 adds `Weights` on the same struct. Or fold `b`/`k1` into Phase 3.

## Optional items

- **Configurable BM25 `b` (+`k1`)** — ✅ done (Phase 2). `FulltextParams.B`/`K1`,
  persisted in the `fulltext` sub-object, resolved per query (`ftsResolveBM25`,
  zero ⇒ default). Even `b≈0.4` materially cuts short-doc bias.
- **Per-index stemming** (`$language` → Snowball in the analyzer) — opt-in, changes
  indexed terms (set at create, re-index to change), adds a dependency. Lowest
  priority; phrase+prefix recover most of its value. Pair with the Phase 3
  re-index if wanted.
- **Query-time IVF `nProbe`** (vector leg, not FTS) — a per-query `vectorEf` knob
  already exists; `vivf.Index.Search` already takes `nprobe` but the index-fixed
  value is passed. Add a `vectorNProbe` query knob mirroring `vectorEf`.

## Phases

- **Phase 0** ✅ — structured query parser + `$defaultOperator`; prefix-before-analyze;
  `ChunkIterator` extraction (`V1ChunkIterator`). No format change.
- **Phase 1** ✅ — accumulator `requiredMask`/tombstone; phrase zig-zag merge;
  CJK→implicit phrase; prefix vocab expansion. No re-index.
- **Phase 2** ✅ — configurable `b`/`k1` (`FulltextParams.B/K1`, `fulltext`
  sub-object, resolved per query; default-param ranking bit-for-bit unchanged).
- **Phase 3** — per-field weights: postings v2 + docinfo v2 + per-field meta +
  `V2ChunkIterator` + BM25F scorer + migration. The one re-index.
- **Phase 4** — stemming; vector `nProbe` (independent).

## Decision record

Reviewed with an outside expert (Gemini 3.1 Pro) on 2026-06-17. Confirmed:
exact BM25→BM25F reduction (D1); iterator decoupling (D2); ship search-time
first (D3); Option A with global gapped positions, global DF, per-field docinfo
(D4); phrase-as-synthetic-term with summed IDF; requiredMask + tombstone in the
existing single-pass accumulator; forbid `*` inside phrases; hard-cap prefix
expansion.
