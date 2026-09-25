# High-Performance SMS Gateway

A blazing-fast, asynchronous SMS Gateway built with **Go (Golang)**, **Kafka**, **Redis**, and **MySQL**. It utilizes **Clean Architecture** principles and advanced concurrency patterns to handle over **50,000 requests per second** (RPS).

For the full design rationale — how each business requirement drove a specific
technical decision, the Clean Architecture layering, request-flow diagrams,
idempotency guarantees, and scaling strategy — see
**[ARCHITECTURE.md](./ARCHITECTURE.md)**.

---

## 🚀 Setup & Installation

This project includes a `Makefile` that automates building the Docker images and running the stack.

### 1. Configure Environment Variables
First, make a copy of the example config:
```bash
cp deployments/.env.example deployments/.env
```
Open `deployments/.env` and adjust the variables (ports, database credentials, SMS cost, replica counts) to your liking.

### 2. Build and Start the Application
To build the Docker images and start all containers (MySQL, Redis, Zookeeper, Kafka, API, and Workers), simply run:
```bash
make
```
*(This is equivalent to running `make build` followed by `make up`).*

### 3. Verify the Services
To check the logs of your running containers:
```bash
make logs
```
Once you see that Kafka has created the `sms_express` and `sms_bulk` topics, and the API/Workers are connected, the system is ready!

---

## 🛠️ Makefile Commands Reference

- **`make test`**: Runs the automated Go unit tests inside a Docker container.
- **`make build`**: Builds the Docker images via docker-compose.
- **`make up`**: Starts the Docker Compose stack in the background.
- **`make down`**: Stops the running Docker Compose stack.
- **`make restart`**: Restarts the entire stack (`down` then `up`).
- **`make logs`**: Tails the logs of all running containers.
- **`make worker-logs`**: Tails the logs of **only** the `worker-express` and `worker-bulk` containers. Useful for tracking SMS processing.
- **`make clean`**: **⚠️ WARNING:** Destroys all containers and **wipes all database/queue volumes**. Use this for a fresh factory reset.

---

## 📡 API Endpoints

### 1. Top Up Balance
```bash
curl -X POST http://localhost:8080/api/v1/users/charge \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "amount": 1000}'
```

### 2. Send SMS
```bash
curl -X POST http://localhost:8080/api/v1/sms/send \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "to_number": "09123456789",
    "text": "Hello from Go!",
    "is_express": true
  }'
```

### 3. Get User SMS Reports
```bash
curl http://localhost:8080/api/v1/users/1/report
```

## 🧪 Automated Testing

This project includes automated **Unit Tests** that mock the domain interfaces, allowing you to test the API and business logic independently of the database or Kafka.

To run all test suites across the project inside an isolated Docker container:
```bash
make test
```
This will automatically execute `go test -v ./...` in a temporary `golang:1.26-alpine` container and ensure that the HTTP Handlers, Mock Operators, and Core logic return the expected results without modifying your host system.

## 🚀 Load Testing (Benchmarking)

To verify the **50,000+ RPS** capability of this asynchronous architecture, you can use the [hey](https://github.com/rakyll/hey) load-testing tool.

Run the following command to send a massive spike of requests (e.g., 400 total requests, 20 concurrent workers) to the SMS endpoint:

```bash
hey -n 400 -c 20 -m POST -T "application/json" \
  -d '{"user_id": 1, "to_number": "09123456789", "text": "Load Test", "is_express": true}' \
  http://localhost:8080/api/v1/sms/send
```
