.PHONY: all build test test-race vet fmt-check reproducible-builds local-gates

all: fmt-check vet test build

build:
	GOWORK=off go build ./...

test:
	GOWORK=off go test ./...

test-race:
	GOWORK=off go test -race -count=1 ./...

vet:
	GOWORK=off go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

reproducible-builds:
	./scripts/verify-reproducible-builds.sh

local-gates: fmt-check vet test-race reproducible-builds
