# vectorbench

A self-contained benchmark for the experimental any-store vector (HNSW) search
package. Builds to a single **static, CGO-free** binary you can copy to slow or
low-power hardware and run without the repo, a network, or the Go toolchain on
the target.

## Build

```bash
# for the host
go build -o vectorbench ./cmd/vectorbench

# cross-compile (no CGO, so this just works from any host)
GOOS=linux GOARCH=amd64 go build -o vectorbench ./cmd/vectorbench
```

The result is a ~4 MB statically-linked ELF with no dynamic dependencies — copy
it to the target box and run it.

## Run

```bash
./vectorbench                       # defaults: n=20000 dim=768 cosine ef=64
./vectorbench -n 75000              # realistic scale
./vectorbench -n 5000 -recall=false # quick, skip the O(n) brute ground truth
./vectorbench -dim 384 -metric l2   # other embedding widths / metrics
./vectorbench -h                    # all flags
```

### Flags

| flag | default | meaning |
|------|---------|---------|
| `-n` | 20000 | number of documents/vectors |
| `-dim` | 768 | embedding dimension |
| `-ef` | 64 | efSearch (HNSW candidate-list size) |
| `-k` | 10 | neighbours per query |
| `-queries` | 200 | queries for recall + latency |
| `-metric` | cosine | `cosine` \| `l2` \| `dot` |
| `-recall` | true | measure recall vs exact brute force (O(n)/query) |
| `-seed` | 2024 | dataset RNG seed |

### Btree-backed modes: `-vectorbench.paged`, `-vectorbench.db`, `-vectorbench.cache`

The btree-paged index, the route-in-RAM + paged-rerank hybrid and the
disk-backed run are the `vector` package's own: they take the module's internal
btree types, so they are unexported and run from that package's tests, with the
flags above under a `vectorbench.` prefix (`-vectorbench.n`, `-vectorbench.dim`,
...) and these:

| flag | default | meaning |
|------|---------|---------|
| `-vectorbench.paged` | false | also measure the btree-paged + hybrid indexes, over an in-memory btree |
| `-vectorbench.db` | | on-disk btree path: the first run builds and persists, a re-run reopens cold from disk |
| `-vectorbench.cache` | 0 | btree page-cache size in pages for `-vectorbench.db` (0 = the default 5000); a small value forces disk I/O |

```bash
go test ./vector -run '^TestVectorbench$' -v -args -vectorbench.paged
go test ./vector -run '^TestVectorbench$' -v -args -vectorbench.db ~/vb.db   # twice: build, then cold reload

# the same as a binary to copy to the target box (no Go needed there)
GOOS=linux GOARCH=amd64 go test -c -o vectorbench-btree ./vector
./vectorbench-btree -test.run '^TestVectorbench$' -test.v -vectorbench.db ~/vb.db -vectorbench.cache 64
```

## What it reports

1. **CPU / SIMD** — `os/arch`, core count, and whether the `internal/simd` asm
   kernels are active for this CPU (AVX2/AVX512 on amd64, NEON/SVE on arm64),
   so distance numbers are interpretable on the target.
2. **Distance kernel** — scalar vs unrolled vs SIMD ns/comparison.
3. **Build** — HNSW construction rate (docs/s), with progress.
4. **Recall** — recall@k vs exact brute force, swept over ef.
5. **Query latency / RAM** — for the in-memory arena (A), and with the btree
   modes the btree-paged index (B) and the route-in-RAM + paged-rerank hybrid
   (B′), plus btree reads/query.

The dataset is synthetic topic-clustered markdown documents embedded with the
feature-hashing trick (deterministic, offline) — representative geometry, not
uniform-random. See `vector/OPTION_B.md` and `vector/README.md` for the analysis
behind the numbers.

## Memory notes (for low-RAM targets)

Peak RAM is a few times the vectors (n·dim·4 bytes), not one copy: the
generated vectors, the in-memory index's arena, which copies them and grows by
appending while the index is built, so that a regrowth briefly holds the old and
the new array, the graph, a brute-force copy for `-recall`, and in the btree
modes the btree's copy (`-vectorbench.paged` holds it in memory;
`-vectorbench.db` in the page cache and the OS cache). The Go collector's
headroom comes on top. On a tight box, run with `-recall=false` to measure just
the in-memory index, or lower `-n`.
```
n=75000 dim=768 (~220 MiB per copy of the vectors), measured on an Apple M2:
  -recall=false   peak heap ~0.8 GiB, peak RSS ~1.0 GiB
  recall on       peak heap ~1.2 GiB, peak RSS ~1.3 GiB
```
