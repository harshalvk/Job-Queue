.PHONY: help build test lint fmt vet run-worker run-producer run-scheduler run-deadletter docker-up docker-down docker-logs clean

build:
	go build -o bin/producer.exe ./cmd/producer
	go build -o bin/worker.exe ./cmd/worker
	go build -o bin/scheduler.exe ./cmd/scheduler
	go build -o bin/deadletter.exe ./cmd/deadletter

test:
	go test -race -v ./...

lint:
	golangci-lint run ./...

fmt:
	goimports -w .

vet:
	go vet ./...

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
