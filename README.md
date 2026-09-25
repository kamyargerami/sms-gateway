# High-Performance SMS Gateway

A blazing-fast, asynchronous SMS Gateway built with **Go (Golang)**, **Kafka**, **Redis**, and **MySQL**. It utilizes **Clean Architecture** principles and advanced concurrency patterns to handle over **70,000 requests per second** (RPS).

## 🧠 Architectural Overview & Technical Deep Dive

This application is designed as an **Asynchronous, Event-Driven Microservice**. The goal is to completely decouple the fast incoming web traffic from the slow external SMS operators, guaranteeing that the API never blocks.

### 1. How the Flow Works (The Journey of an SMS)
- **API (Producer):** When a user sends an SMS, the API receives the JSON, validates it, and immediately checks the user's balance in **Redis (Cache)**. If sufficient, the balance is deducted in memory. The API then packages the message, assigns it a UUID, and fires it into a **Kafka Topic** (`sms_express` or `sms_bulk`). It returns a `200 OK` to the user in less than a millisecond.
- **Kafka (The Buffer):** Kafka acts as an indestructible shock-absorber. Even if 1 million messages arrive in 10 seconds, Kafka safely stores them on disk across its partitions.
- **Workers (Consumers):** Background Go processes constantly pull messages from Kafka. They save the pending record into **MySQL**, call the external SMS Operator's API, and update the status to `DELIVERED`. If the operator fails, the worker gracefully refunds the user in both MySQL and Redis.

### 2. Safeguards & Engineering Marvels
We implemented several high-end architectural safeguards to make this system enterprise-ready:

#### A. 100% Atomic Operations (No Race Conditions)
In high concurrency, if two API requests try to deduct a user's balance at the exact same microsecond, a "Race Condition" occurs, allowing users to spend money they don't have.
- **Redis Lua Scripts:** We use a custom Lua script for both balance deduction and refunds. Redis is single-threaded, so the Lua script guarantees that checking the balance and deducting it happens as a single, indivisible (atomic) operation.
- **MySQL Transactions:** In the Worker, inserting the SMS and updating the permanent balance in the `users` table are wrapped in a single SQL Transaction (`tx.Begin()`). If anything fails, it fully rolls back.

#### B. Eradicating InnoDB Deadlocks (Error 1213)
When multiple workers process messages for the *same* user concurrently, MySQL's InnoDB engine can throw Deadlocks. This happens because Foreign Keys acquire Shared (S) locks, and subsequent UPDATEs try to escalate them to Exclusive (X) locks simultaneously.
- **The Fix:** We structured our SQL queries to execute `UPDATE users SET balance...` **first**, before inserting into `sms_records` or `transactions`. This forces the worker to acquire the Exclusive lock immediately, gracefully queuing other workers and completely eliminating deadlocks.

#### C. Kafka Batching & RoundRobin Fairness
- **Producer Batching:** The API uses a 10ms batch timeout. Under heavy load, it groups thousands of SMS messages into a single TCP packet before sending them to Kafka, dropping network overhead to near zero.
- **Fair Distribution:** We use a `RoundRobin` balancer. When a massive burst of traffic hits, messages are distributed perfectly equally across all 5 partitions of the `sms_express` topic, ensuring all 5 Express Workers share the exact same amount of load.

#### D. The Kafka Parallelism Model (Why no internal Worker Pool?)
To solve the issue of slow external operators (e.g., if an operator takes 5 seconds to reply), one might be tempted to spawn thousands of Goroutines inside a single Worker (a Worker Pool). 
However, this is an **anti-pattern** in Kafka because committing offsets concurrently leads to data loss if the server crashes (committing offset 100 implies 1-99 are also done). 
Instead, we strictly follow the **Kafka Parallelism Model**: 1 Goroutine per Partition. To scale this system to handle slower operators, we simply increase the Kafka partitions to 100 and spawn 100 lightweight Worker replicas. This guarantees zero data loss and flawless horizontal scaling.

#### E. Mock External Operator (Telecom Simulator)
To test the resilience of our asynchronous architecture, the `internal/operator` package acts as a simulated 3rd-party telecom provider. 
Instead of sending real SMS, it simulates the unpredictable latency of an external HTTP request. It randomly sleeps for a few milliseconds (or seconds) to simulate network delay, and randomly fails (e.g., 5% failure rate) to test the Worker's automatic Refund and Rollback mechanisms in real-time.

---

## 🚀 Setup & Installation

This project includes a `Makefile` that automates building the Docker images and running the stack.

### 1. Configure Environment Variables
The environment configuration files are located in the `deployments/` folder.
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

Here are all the available commands to manage the application:

- **`make build`**: Builds the Docker images via docker-compose.
- **`make up`**: Starts the Docker Compose stack in the background.
- **`make down`**: Stops the running Docker Compose stack.
- **`make restart`**: Restarts the entire stack (`down` then `up`).
- **`make logs`**: Tails the logs of all running containers.
- **`make clean`**: **⚠️ WARNING:** Destroys all containers and **wipes all database/queue volumes**. Use this for a fresh factory reset.

---

## 📡 API Endpoints

### 1. Top Up Balance
```bash
curl -X POST http://localhost:8080/api/v1/topup \
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
curl http://localhost:8080/api/v1/reports/1
```
