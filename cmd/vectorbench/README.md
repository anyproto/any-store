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
| `-paged` | true | also measure the btree-paged + hybrid indexes |
| `-recall` | true | measure recall vs exact brute force (O(n)/query) |
| `-seed` | 2024 | dataset RNG seed |

## What it reports

1. **CPU / SIMD** — `os/arch`, core count, and whether vek's AVX kernels are
   active (so distance numbers are interpretable on the target).
2. **Distance kernel** — scalar vs unrolled vs SIMD ns/comparison.
3. **Build** — HNSW construction rate (docs/s), with progress.
4. **Recall** — recall@k vs exact brute force, swept over ef.
5. **Query latency / RAM** — for the in-memory arena (A), the btree-paged index
   (B), and the route-in-RAM + paged-rerank hybrid (B′), plus btree reads/query.

The dataset is synthetic topic-clustered markdown documents embedded with the
feature-hashing trick (deterministic, offline) — representative geometry, not
uniform-random. See `vector/OPTION_B.md` and `vector/README.md` for the analysis
behind the numbers.

## Memory notes (for low-RAM targets)

Peak RAM ≈ the arena (n·dim·4) **plus** a brute-force copy (`-recall`) **plus**
the paged index's btree copy (`-paged`). On a tight box, run with
`-recall=false -paged=false` to measure just the in-memory index, or lower `-n`.
```
n=75000 dim=768  → ~220 MiB arena; ~600 MiB peak with recall+paged on
```
