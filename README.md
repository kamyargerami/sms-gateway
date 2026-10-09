# High-Performance SMS Gateway

A blazing-fast, asynchronous SMS Gateway built with **Go (Golang)**, **Kafka**, **Redis**, and **MySQL**. It utilizes **Clean Architecture** principles and advanced concurrency patterns to handle over **30,000 requests per second** (RPS) and it can send over **1,300 SMS per second**.

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

Express SMS get a delivery deadline (`EXPRESS_SMS_TTL`, default `2m`), returned as `expires_at` in
the response; an express SMS still queued after it is marked `EXPIRED`, not sent
and not charged (see ARCHITECTURE.md). Bulk SMS have no deadline.

### 3. Get User SMS Reports
Fetch SMS reports for a user with optional `limit` and `offset` query parameters for pagination (default `limit=50`, `offset=0`).

```bash
curl "http://localhost:8080/api/v1/users/1/report?limit=50&offset=0"
```
The response will include the `reports` array along with the `limit` and `offset` used.

## 🧪 Automated Testing

This project includes automated **Unit Tests** that mock the domain interfaces, allowing you to test the API and business logic independently of the database or Kafka.

To run all test suites across the project inside an isolated Docker container:
```bash
make test
```
This will automatically execute `go test -v ./...` in a temporary `golang:alpine` container and ensure that the HTTP Handlers, Mock Operators, and Core logic return the expected results without modifying your host system.

## 🚀 Load Testing (Benchmarking)

To verify the capability of this asynchronous architecture, you can use the [hey](https://github.com/rakyll/hey) load-testing tool.

Run the following command to send a massive spike of requests (e.g., 400 total requests, 20 concurrent workers) to the SMS endpoint:

```bash
hey -n 400 -c 20 -m POST -T "application/json" \
  -d '{"user_id": 1, "to_number": "09123456789", "text": "Load Test", "is_express": true}' \
  http://localhost:8080/api/v1/sms/send
```

### Spreading the load over many users
`scripts/loadtest` creates users, tops them up through the API, sends SMS
round-robin across them, waits for the workers and reports both API and
worker throughput:

```bash
go run ./scripts/loadtest -express -first-user 3000 -users 1000 -n 70000 -c 350 &
go run ./scripts/loadtest          -first-user 5000 -users 1000 -n 30000 -c 150 &
wait
```

Useful flags: 

`-express` (send express SMS; expired ones don't count as
throughput)

`-first-user` (first user id that will be created in DB, default 1000)

`-dsn` (MySQL DSN, default matches `.env.example` on `localhost:3306`)

`-api` (default`http://localhost:8080`).
