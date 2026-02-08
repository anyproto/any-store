# Benchmark: btree vs SQLite vs bbolt vs badger

Comparative benchmarks for the four KV engines.

## Run all benchmarks

```bash
cd bench
go test -bench . -benchtime 1s -timeout 300s
```

## Run specific benchmarks

```bash
# Write benchmarks (batch sizes: 1, 10, 100, 1000)
go test -bench BenchmarkWrite -benchtime 1s

# Get by ID (thread counts: 1, 4, 10, 32)
go test -bench BenchmarkGetById -benchtime 1s

# Iteration (key counts: 1000, 10000)
go test -bench BenchmarkIterate -benchtime 1s
```

## Engines

| Engine | Description |
|--------|-------------|
| **btree** | `internal/btree` — pure-Go SQLite-style B-tree engine |
| **sqlite** | `internal/driver` — go-sqlite (WAL mode, normal sync) |
| **bbolt** | `go.etcd.io/bbolt` — pure-Go B+ tree |
| **badger** | `github.com/dgraph-io/badger/v4` — LSM-tree based |
