.PHONY: build test test-integration docker-up docker-down

build:
	go build -o bin/server ./cmd/server
	go build -o bin/agent ./cmd/agent
	go build -o bin/shardman ./cmd/shardman

test:
	go test ./...

test-integration:
	go test -tags=integration ./internal/store/... -count=1

docker-up:
	docker compose -f deploy/docker-compose.yml up -d --build

docker-down:
	docker compose -f deploy/docker-compose.yml down -v
