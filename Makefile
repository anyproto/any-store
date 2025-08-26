.PHONY: build test deps
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
	cd cmd/any-store-cli && GOOS=$(BUILD_GOOS) GOARCH=$(BUILD_GOARCH) go build -v $(TAGS) -o ../../bin/any-store-cli

test:
	go test ./... --cover $(TAGS)

deps:
	go mod download
