.PHONY: build test test-integration test-stand docker-up docker-down

build:
	go build -o bin/server ./cmd/server
	go build -o bin/agent ./cmd/agent
	go build -o bin/shardman ./cmd/shardman

test:
	go test ./...

METADATA_PG_DSN ?= postgres://shardman:shardman@127.0.0.1:5433/shardman_meta?sslmode=disable

test-integration:
	METADATA_PG_DSN=$(METADATA_PG_DSN) go test -tags=integration ./internal/integration/... -count=1 -v

test-stand: docker-up
	@$(MAKE) test-integration

docker-up:
	docker compose -f deploy/docker-compose.yml up -d --build

docker-down:
	docker compose -f deploy/docker-compose.yml down -v
