.DEFAULT_GOAL := check
GO_VERSION := $(shell sed -n 's/^go //p' go.mod)
export GOTOOLCHAIN := local

.PHONY: check check-go fmt-check vet test race build run dev dev-down test-launch vuln

check: fmt-check vet test

check-go:
	@test "$$(go env GOVERSION)" = "go$(GO_VERSION)" || { echo "Go $(GO_VERSION) is required; automatic toolchain switching is disabled." >&2; exit 1; }

fmt-check: check-go
	@test -z "$$(gofmt -l cmd internal tests)" || { gofmt -l cmd internal tests; exit 1; }

vet: check-go
	go vet ./...

test: check-go
	go test ./...

race: check-go
	go test -race ./...

build: check-go
	go build -trimpath -o bin/sylloge ./cmd

run: check-go
	go run ./cmd

dev:
	docker compose --env-file .env --profile dev up --build

dev-down:
	docker compose --env-file .env --profile dev down

test-launch: check-go
	go test -tags=compose ./tests/integration/...

# Separate tool install: no application go.mod/go.sum changes.
vuln: check-go
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
