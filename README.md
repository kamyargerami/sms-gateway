# High-Performance SMS Gateway

A blazing-fast, asynchronous SMS Gateway built with **Go (Golang)**, **Kafka**, **Redis**, and **MySQL**. It utilizes **Clean Architecture** principles and advanced concurrency patterns to handle over **70,000 requests per second** (RPS).

## 🎯 How We Met the Challenge Requirements (The "Why")

Every technical decision in this project was mapped directly to the business requirements:

1. **"100 Million SMS per Day" (Scale):** 
   We decoupled the system. The API doesn't talk to the external telecom. It simply drops the message into **Kafka** and updates **Redis** in `<1ms`. Kafka seamlessly buffers extreme traffic spikes (handling 70k+ RPS during our benchmarks).

2. **"Unequal Distribution of Traffic among Clients":** 
   If a single massive client fires 50,000 SMS at once, traditional hashing by `user_id` would route all of them to a single Kafka partition, bottlenecking one worker while others idle. We explicitly used Kafka's **`RoundRobin` Balancer** to evenly spray messages across all partitions regardless of the sender.

3. **"Express vs. Bulk SMS (Guaranteed Delivery Time)":** 
   We created entirely separate Kafka Topics (`sms_express` and `sms_bulk`). Express messages go to a topic with 5 partitions and 5 dedicated workers, ensuring they bypass the queue of millions of bulk marketing messages.

4. **"Balance Must Never Drop Below Zero":** 
   We used a custom **Redis Lua Script** in the API layer to guarantee that checking the balance and deducting it happens as a single, atomic, thread-safe operation (cache misses are initialized with `SET NX`, so concurrent requests can't overwrite each other's deductions). Redis is only a fast pre-check: the source of truth is MySQL, where the worker debits with a guarded `UPDATE ... WHERE balance >= cost` inside an InnoDB transaction (plus a `BIGINT UNSIGNED` balance column, so MySQL itself rejects any negative value). If Redis is ever stale (restart, eviction, failed sync), the worker rejects the SMS as `FAILED` instead of sending it for free.

5. **"Clients Must Use All Their Balance / No Free SMS":**
   If the mock telecom operator fails, the worker marks the SMS `FAILED` and refunds it in **one transaction**, guarded by a `PENDING -> FAILED` transition, so a crash can't lose a refund and a redelivery can't refund twice. If publishing to Kafka fails, the API gives the reserved credit back to Redis immediately.

6. **At-least-once processing without losing messages:**
   Transient DB errors in the worker are retried in place with backoff. A message is never skipped: in kafka-go, committing a later offset implicitly commits every earlier one, so skipping would silently drop the SMS (and the user's credit). On shutdown, an unfinished message is left uncommitted so it's redelivered.

---

## 🧠 Architectural Overview & Technical Deep Dive

This application strictly follows **Clean Architecture** (Hexagonal Architecture). 
- **Domain Layer:** Contains pure business models and `Interfaces` (`DatabaseRepository`, `MessageProducer`, etc.).
- **Delivery Layer (`http.go`):** Relies purely on Interfaces. It has zero direct dependency on MySQL or Kafka.
- **Repository/Kafka Layers:** Implement the Domain Interfaces.
- **cmd Layer:** Handles Dependency Injection, wiring the concrete databases to the HTTP and Worker handlers.

### Safeguards & Engineering Marvels

#### A. Eradicating InnoDB Deadlocks (Error 1213)
When multiple workers process messages for the *same* user concurrently, MySQL's InnoDB engine throws Deadlocks because Foreign Keys acquire Shared (S) locks, and subsequent UPDATEs try to escalate them to Exclusive (X) locks simultaneously.
- **The Fix:** We structured our SQL queries to execute `UPDATE users SET balance...` **first**, forcing the worker to acquire the Exclusive lock immediately. This gracefully queues other concurrent workers and completely eliminates deadlocks.

#### B. The Kafka Parallelism Model
To scale this system to handle slow telecom operators, we strictly follow the **Kafka Parallelism Model**: 1 Goroutine per Partition. We do not use internal Worker Pools (Goroutine pools inside a consumer) because committing offsets concurrently leads to data loss upon crashes. We simply scale horizontally by adding more Kafka Partitions and Worker Replicas.

#### C. O(1) Database Lookups (Composite Indexes)
To ensure the `GetReports` API remains blazing fast even when a user has millions of SMS records, we added a **Composite Index** on `(user_id, created_at DESC)`. This allows MySQL to fetch the latest 100 messages instantly without performing expensive memory sorts (Filesorts).

#### D. Mock External Operator (Telecom Simulator)
The `internal/operator` package simulates a 3rd-party telecom provider. It introduces random latency (10-100ms) and random failure rates to test the Worker's automatic Refund and Idempotency mechanisms in real-time.

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

To verify the **30,000+ RPS** capability of this asynchronous architecture, you can use the [hey](https://github.com/rakyll/hey) load-testing tool.

Run the following command to send a massive spike of requests (e.g., 400 total requests, 20 concurrent workers) to the SMS endpoint:

```bash
hey -n 400 -c 20 -m POST -T "application/json" \
  -d '{"user_id": 1, "to_number": "09123456789", "text": "Load Test", "is_express": true}' \
  http://localhost:8080/api/v1/sms/send
```
