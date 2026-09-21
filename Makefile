.PHONY: build test cover deps
SHELL=/bin/bash
export GOPRIVATE=github.com/anyproto
export PATH:=deps:$(PATH)
export CGO_ENABLED:=1
BUILD_GOOS:=$(shell go env GOOS)
BUILD_GOARCH:=$(shell go env GOARCH)

ifeq ($(CGO_ENABLED), 0)
	TAGS:=-tags nographviz
else
	TAGS:=
endif

build:
	cd cmd/any-store-cli2 && GOOS=$(BUILD_GOOS) GOARCH=$(BUILD_GOARCH) go build -v $(TAGS) -o ../../bin/any-store-cli2

test:
	go test ./... --cover $(TAGS)

# statement coverage of the library, counting the test/ suites that drive it
# from outside the package
cover:
	rm -rf .coverdata && mkdir .coverdata
	go test . ./test/ -coverpkg=github.com/anyproto/any-store/v2 $(TAGS) \
		-args -test.gocoverdir=$(CURDIR)/.coverdata
	go tool covdata percent -i=.coverdata

deps:
	go mod download
