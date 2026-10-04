.PHONY: run test race burst docker build

URL ?= http://127.0.0.1:8080
ADMIN_TOKEN ?= eventbooker-admin
TOKEN_SECRET ?= dev-token-secret-change-me
DB_PATH ?= /tmp/eventbooker.db

run:
	ADMIN_TOKEN=$(ADMIN_TOKEN) TOKEN_SECRET=$(TOKEN_SECRET) DB_PATH=$(DB_PATH) go run ./cmd/eventbooker

test:
	go test -count=1 -timeout 180s ./...

race:
	go test -race -count=1 -timeout 300s ./...

build:
	CGO_ENABLED=0 go build -o bin/eventbooker ./cmd/eventbooker
	CGO_ENABLED=0 go build -o bin/burst ./cmd/burst

burst: build
	./bin/burst --url $(URL)

docker:
	docker compose up --build
