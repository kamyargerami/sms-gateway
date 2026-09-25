.PHONY: build up down logs worker-logs clean restart

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
	@docker-compose -f deployments/docker-compose.yml down
	@echo "Services stopped."

# View logs from all containers
logs:
	@docker-compose -f deployments/docker-compose.yml logs -f

# View logs from only the SMS workers
worker-logs:
	@docker-compose -f deployments/docker-compose.yml logs worker-express worker-bulk -f

# Completely reset the environment (deletes database, redis, kafka data)
clean:
	@echo "Wiping all data and stopping services..."
	@docker-compose -f deployments/docker-compose.yml down -v
	@rm -rf bin
	@echo "Environment is completely clean."

# Restart the application
restart: down up
