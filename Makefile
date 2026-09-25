.PHONY: all test build up down logs worker-logs clean restart

# Run automated tests
test:
	@echo "\033[1;36m🚀 Running automated tests inside Docker\033[0m"
	@docker run --rm -v $(PWD):/app -w /app -v go_mod_cache:/go/pkg/mod golang:alpine sh -c "go test -v ./... | awk '\
	/^=== RUN/ {print \"\033[1;36m▶ \" \$$0 \"\033[0m\"} \
	/^--- PASS/ {print \"\033[1;32m✔ \" \$$0 \"\033[0m\"} \
	/^--- FAIL/ {print \"\033[1;31m✖ \" \$$0 \"\033[0m\"} \
	/^PASS/ {print \"\n\033[1;32m✅ ALL TESTS PASSED SUCCESSFULLY!\033[0m\n\"} \
	/^FAIL/ {print \"\n\033[1;31m❌ TESTS FAILED!\033[0m\n\"; failed=1} \
	!/^=== RUN/ && !/^--- PASS/ && !/^--- FAIL/ && !/^PASS/ && !/^FAIL/ {print} \
	END {exit failed}'"

# Default target
all: build up

# Build Docker images
build:
	@echo "Building Docker images..."
	@docker-compose --env-file deployments/.env -f deployments/docker-compose.yml build
	@echo "Build complete."

# Start the Docker containers
up:
	@echo "Starting services..."
	@docker-compose --env-file deployments/.env -f deployments/docker-compose.yml up -d
	@echo "Services are running."

# Stop the Docker containers
down:
	@echo "Stopping services..."
	@docker-compose --env-file deployments/.env -f deployments/docker-compose.yml down
	@echo "Services stopped."

# View logs from all containers
logs:
	@docker-compose --env-file deployments/.env -f deployments/docker-compose.yml logs -f

# View logs from only the SMS workers
worker-logs:
	@docker-compose --env-file deployments/.env -f deployments/docker-compose.yml logs worker-express worker-bulk -f

# Completely reset the environment (deletes database, redis, kafka data)
clean:
	@echo "Wiping all data and stopping services..."
	@docker-compose --env-file deployments/.env -f deployments/docker-compose.yml down -v
	@rm -rf bin
	@echo "Environment is completely clean."

# Restart the application
restart: down up