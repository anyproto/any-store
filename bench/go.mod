module github.com/anyproto/any-store/bench

go 1.23.0

toolchain go1.24.1

require (
	github.com/anyproto/any-store v0.0.0
	github.com/anyproto/any-store-sqlite v0.0.0
	github.com/dgraph-io/badger/v4 v4.5.1
	go.etcd.io/bbolt v1.4.0
)

require (
	github.com/anyproto/go-sqlite v1.4.2-any // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.1.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/google/flatbuffers v24.12.23+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/valyala/fastjson v1.6.7 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/exp v0.0.0-20250620022241-b7579e27df2b // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	google.golang.org/protobuf v1.36.3 // indirect
	modernc.org/libc v1.66.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.37.1 // indirect
)

replace github.com/anyproto/any-store => ../

replace github.com/anyproto/any-store-sqlite => /home/dev/work/any-store-main
