.PHONY: build test test-integration test-e2e test-stand docker-up docker-down proto

GO ?= go
PROTOC ?= tools/protoc/bin/protoc.exe

build:
	$(GO) build -o bin/server ./cmd/server
	$(GO) build -o bin/agent ./cmd/agent
	$(GO) build -o bin/shardman ./cmd/shardman

proto:
	powershell -File scripts/generate-proto.ps1

test:
	$(GO) test ./...

METADATA_PG_DSN ?= postgres://shardman:shardman@127.0.0.1:5433/shardman_meta?sslmode=disable

test-integration:
	METADATA_PG_DSN=$(METADATA_PG_DSN) $(GO) test -tags=integration ./internal/integration/... -count=1 -v

test-e2e:
	$(GO) test -tags=e2e ./internal/e2e/... -count=1 -v -timeout=10m

test-stand: docker-up
	@$(MAKE) test-integration

docker-up:
	docker compose -f deploy/docker-compose.yml up -d --build

docker-down:
	docker compose -f deploy/docker-compose.yml down -v
