.PHONY: verify fmt-check vet test-race build

verify: fmt-check vet test-race build

fmt-check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

vet:
	GOWORK=off go vet ./...

test-race:
	GOWORK=off go test -race -count=1 ./...

build:
	GOWORK=off go build ./...
