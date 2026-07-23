.PHONY: help build test lint fmt vet migrate vuln sec run-worker run-producer run-scheduler run-deadletter docker-up docker-down docker-logs clean docker-build-worker docker-build-scheduler docker-build-producer docker-build-deadletter

build:
	go build -o bin/producer.exe ./cmd/producer
	go build -o bin/worker.exe ./cmd/worker
	go build -o bin/scheduler.exe ./cmd/scheduler
	go build -o bin/deadletter.exe ./cmd/deadletter

test:
	go test -v ./...

lint:
	golangci-lint run ./...

fmt:
	goimports -w .

vet:
	go vet ./...

migrate:
	docker cp migrations/0001_init.sql kairos-postgres:/0001_init.sql
	docker exec -it kairos-postgres psql -U kairos -d kairos -f /0001_init.sql

run-worker:
	go run ./cmd/worker

run-producer:
	go run ./cmd/producer

run-scheduler:
	go run ./cmd/scheduler

run-deadletter:
	go run ./cmd/deadletter $(ARGS)

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

clean:
	rm -rf bin/

vuln:
	govulncheck ./...

sec:
	gosec ./...

docker-build-worker:
	docker build --build-arg CMD_PATH=cmd/worker -t kairos-worker .

docker-build-scheduler:
	docker build --build-arg CMD_PATH=cmd/scheduler -t kairos-scheduler .

docker-build-producer:
	docker build --build-arg CMD_PATH=cmd/producer -t kairos-producer .

docker-build-deadletter:
	docker build --build-arg CMD_PATH=cmd/deadletter -t kairos-deadletter .
