.PHONY: build run test test-race test-live coverage lint tidy

build: ## Build the server binary
	go build -o bin/server ./cmd/server

run: ## Run the server with config.json
	go run ./cmd/server --config config.json

test: ## Run all unit tests (short mode)
	go test ./... -short -count=1

test-race: ## Run tests with race detection
	go test ./... -race -count=1

test-live: ## Run integration tests (requires docker-compose)
	docker compose up -d
	go test -tags=live ./... -count=1

coverage: ## Generate a coverage report
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

lint: ## Run the linter
	golangci-lint run ./...

tidy: ## Tidy module dependencies
	go mod tidy
