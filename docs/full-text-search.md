# Full-text search

any-store has a built-in full-text search (FTS) index for ranked text retrieval,
implemented as a btree-resident inverted index scored with BM25. It integrates
with the normal `Find()` query pipeline: a `$text` clause selects and ranks
candidates, ordinary filters and sorting apply on top, and each result carries
its relevance score.

- **Storage-resident** — the inverted index lives in the btree, so it's
  crash-safe, MVCC-consistent across readers, and multiprocess-safe for writes.
  No separate in-memory index to build or invalidate; a `$text` write commits
  atomically with the document.
- **Queried through `Find()`** — there is no separate search call; you express
  the query as `Find({"$text": {"$search": "..."}})` and combine it with any
  other filter, sort, limit, and offset.
- **Pure Go, no CGO, no dictionaries** — the analyzer is ~200 lines over
  `rivo/uniseg` + `golang.org/x/text`; it works across scripts out of the box.

## 1. Create a full-text index

Register an index of kind `IndexKindFulltext` over one or more text fields:

```go
err := coll.EnsureIndex(ctx, anystore.IndexInfo{
    Name:   "fts",                       // index name (auto-generated if empty)
    Kind:   anystore.IndexKindFulltext,
    Fields: []string{"title", "body"},   // the text fields to index (required)
})
```

`CreateIndex` errors if the index already exists; `EnsureIndex` is idempotent.
Creating the index on a non-empty collection backfills it from existing
documents. `Fields` is the only requirement (`FulltextParams` is reserved for
future options — per-field weights, analyzer selection, positionless mode — and
may be left nil).

A query's `$text` clause searches **all** of the index's fields together.
Multiple fields are concatenated with a position gap so a phrase can't match
across a field boundary. Nested paths work (`Fields: ["meta.note"]`), and a
field holding an array of strings is indexed element by element.

> **One text index per collection.** Like MongoDB, a collection is expected to
> have a single full-text index; a `$text` query uses the first one. To change
> the indexed fields, drop and recreate the index.

## 2. Document shape

Text lives in ordinary string fields — no special encoding. A document is
indexed if at least one of the index's fields holds text (string, or array of
strings); documents with no indexable text are simply skipped (and excluded from
the corpus statistics).

```go
coll.Insert(ctx,
    anyenc.MustParseJson(`{"id":"a","title":"Hindenburg","body":"zeppelin disaster, 1937"}`),
    anyenc.MustParseJson(`{"id":"b","title":"Boeing 247","body":"crashes into a mountain"}`),
    anyenc.MustParseJson(`{"id":"c","title":"東京","body":"東京都に住んでいる"}`),
)
```

Inserts are batched: pass many documents to one `Insert(...)` call (or do many
writes inside one `WriteTx`) so they share a single transaction — the FTS write
path buffers postings per-transaction and flushes each touched term once at
commit, so a batch is far cheaper than the same docs inserted one tx at a time.

### The analyzer

Each text field is run through a fixed pipeline:

```
NFKC normalize  →  Unicode case-fold  →  UAX#29 word split  →  CJK bigram
```

- **NFKC + case-fold** make matching robust across platforms/IMEs: full-width
  forms, compatibility ligatures, NFC/NFD spellings, and case all normalize
  (`Straße` ⇄ `strasse`, `Ａｐｐｌｅ` ⇄ `apple`). This is search-oriented folding,
  not `strings.ToLower`.
- **UAX#29** splits spaced scripts (Latin, Cyrillic, Greek, …) into words.
- **CJK bigrams** — runs of Han/Kana/Hangul are indexed as overlapping bigrams
  (`東京都` → `東京`, `京都`); a query is rewritten the same way. A lone CJK
  character is indexed as a unigram.

The analyzer is **multilingual by default** — no per-field language config. It
deliberately does **not** stem (it can't assume a language; prefix-style recall
covers most of that) and does **not** remove stop words (personal notes contain
queries like "to do" or "let it be"; stop words compress well under delta-coding
and BM25's IDF already down-weights them).

## 3. Query

A query becomes a full-text search when it contains a `$text` clause:

```go
// Ranked by relevance (BM25), most relevant first — the default order.
iter, err := coll.Find(`{"$text":{"$search":"zeppelin disaster"}}`).Limit(10).Iter(ctx)
```

The search is **bag-of-words** (OR): a document matches if it contains any query
term, and its score is the sum of the per-term BM25 contributions, so documents
matching more (and rarer) terms rank higher. Everything else in the filter is
applied as a normal predicate on the ranked candidates:

```go
// $text + ordinary filters (implicit AND).
coll.Find(map[string]any{
    "$text":  map[string]any{"$search": "crash"},
    "year":   map[string]any{"$gte": 1940},
    "status": "open",
}).Limit(10).Iter(ctx)
```

`$search` accepts a `$language`/`$caseSensitive`/`$diacriticSensitive` companion
for MongoDB compatibility; they are currently accepted and ignored (the analyzer
is language-neutral).

### Score: the `_score` field

Each result is decorated with a synthetic `_score` field (larger = more
relevant). You can read it and sort by it.

```go
// Read it per row:
for iter.Next() {
    doc, _ := iter.Doc()
    s := iter.Score() // == doc's _score
    _ = doc; _ = s
}

// Relevance is the default order; this is the explicit, MongoDB-compatible form:
coll.Find(`{"$text":{"$search":"crash"}}`).
    Sort(`{"score":{"$meta":"textScore"}}`).Iter(ctx)
```

`Iterator.Score()` returns the row's BM25 score (0 for non-`$text` queries).

### Sorting and pagination

With no sort (or the `{$meta:"textScore"}` relevance sort) results stream in
score order. Sorting on a **real field** overrides relevance and re-orders the
matches:

```go
coll.Find(`{"$text":{"$search":"london"}}`).Sort("year").Iter(ctx)   // oldest first
coll.Find(`{"$text":{"$search":"london"}}`).Sort("-year").Iter(ctx)  // newest first
```

`Limit`/`Offset` apply *after* the residual filter and any sort, so
`Find($text + filter).Limit(10)` returns ten *matching, filtered* rows.

### CJK and multilingual queries

Searching works the same across scripts. A CJK query is split into the same
bigrams used at index time, so `"東京"` matches documents containing 東京:

```go
coll.Find(`{"$text":{"$search":"東京"}}`).Iter(ctx)
```

Because CJK is matched as a bag of bigrams (positional phrase matching is a
future addition), a query for `東京都` matches documents containing both `東京`
and `京都`; at local-first scale the false-positive rate is negligible and
recall is excellent.

## 4. Errors for malformed queries

| Query | Result |
|---|---|
| `$text` on a collection with no full-text index | `ErrNoFulltextIndex` |
| `$text` nested under `$or` / `$nor` / `$not` | error — `$text` must be top-level or inside `$and` |
| More than one `$text` clause in a query | error — only one `$text` per query |
| `$text` with no `$search`, or `$search` not a string | error |

`$text` combined with other predicates via top-level fields or `$and` is the
supported shape (it must be able to *drive* the query).

## 5. Updates, deletes, and operations

Updates and deletes are automatic: re-inserting a document id reindexes its
text; deleting removes it from results. An update **diffs the old and new text**
and touches only the terms that actually changed — editing one word in a long
note rewrites a couple of postings chunks, not the whole document — and the
document keeps its internal id, so the index never accumulates tombstones (no
compaction or vacuum is ever needed).

`$text` is a first-class query, so it works across every operation, not just
`Iter`:

```go
n, _   := coll.Find(`{"$text":{"$search":"crash"}}`).Count(ctx)
res, _ := coll.Find(`{"$text":{"$search":"paris"}}`).Update(ctx, `{"$set":{"tag":"fr"}}`)
res, _ := coll.Find(`{"$text":{"$search":"obsolete"}}`).Delete(ctx)
exp, _ := coll.Find(`{"$text":{"$search":"crash"}}`).Explain(ctx) // plan shows "FtsSearch"
```

## 6. Statistics

`Collection.Stats()` reports per-full-text-index storage and corpus statistics in
`CollectionStats.FtsIndexes`:

```go
st, _ := coll.Stats(ctx)
for _, f := range st.FtsIndexes {
    fmt.Printf("%s: docs=%d vocab=%d tokens=%d avgDocLen=%.1f size=%dB\n",
        f.Name, f.DocCount, f.VocabSize, f.TotalTokens, f.AvgDocLen, f.SizeBytes)
}
```

| Field | Meaning |
|---|---|
| `DocCount` | documents indexed (empty-text docs excluded) |
| `VocabSize` | number of distinct terms |
| `TotalTokens` / `AvgDocLen` | total / mean document length in tokens (the BM25 length-normalization baseline) |
| `PostingsBytes` … `MetaBytes` | on-disk size of each of the five namespaces (postings dominate) |
| `SizeBytes` | total physical size; also rolled into `CollectionStats.TotalSizeBytes` |

The internal stats that drive ranking (document frequency `df`, corpus size `N`,
average document length) are the same counters reported here — kept exact across
every insert, update, and delete.

## 7. Performance notes

- **Batch your writes.** A batch insert (one `Insert(docs...)` call or one
  `WriteTx`) collapses the per-term postings rewrites to once-per-touched-chunk
  at commit — substantially faster and lower-allocation than per-document
  transactions. Each commit also pays the durability flush once for the whole
  batch.
- **Interactive edits are cheap.** The diff-based update touches only changed
  terms, so editing a small note runs at tens of thousands of edits/second.
- **Queries are interactive.** BM25 ranking over the postings is sub-millisecond
  to low-milliseconds at local-first scale (10k–1M docs); a pooled, pointer-free
  score accumulator keeps query allocation flat.
- **Index size.** The inverted index is roughly comparable to the source text
  size (positions are stored, which is ~half of the postings). It is already
  entropy-coded (delta-varint integers); general-purpose byte compression does
  not help and is intentionally not applied.
- **Memory.** The write buffer is per-transaction and spills to the btree past a
  cap, so a huge batch can't grow it without bound; at rest the index holds only
  small scratch buffers.

## 8. End-to-end example

```go
ctx := context.Background()
db, _ := anystore.Open(ctx, ":memory:", nil)
defer db.Close()

coll, _ := db.CreateCollection(ctx, "pages")
coll.EnsureIndex(ctx, anystore.IndexInfo{
    Name:   "fts",
    Kind:   anystore.IndexKindFulltext,
    Fields: []string{"title", "body"},
})

coll.Insert(ctx,
    anyenc.MustParseJson(`{"id":"a","title":"Hindenburg","body":"zeppelin disaster","year":1937}`),
    anyenc.MustParseJson(`{"id":"b","title":"Boeing","body":"london flight crash","year":1950}`),
    anyenc.MustParseJson(`{"id":"c","title":"Comet","body":"london landing","year":1953}`),
)

// Relevance-ranked $text + a residual filter, newest first.
iter, _ := coll.Find(map[string]any{
    "$text": map[string]any{"$search": "london crash"},
    "year":  map[string]any{"$gte": 1950},
}).Sort("-year").Limit(10).Iter(ctx)
defer iter.Close()

for iter.Next() {
    doc, _ := iter.Doc()
    fmt.Printf("score=%.4f doc=%s\n", iter.Score(), doc.Value().String())
}
```
