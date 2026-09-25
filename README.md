# High-Performance SMS Gateway

A blazing-fast, asynchronous SMS Gateway built with **Go (Golang)**, **Kafka**, **Redis**, and **MySQL**. It uses Clean Architecture principles and a concurrent Worker Pool to handle over **70,000 requests per second**.

## Features
- **Clean Architecture**: Domain, Delivery, Repository, Kafka, and Operator layers strictly decoupled.
- **Asynchronous Processing**: HTTP API instantly queues messages to Kafka. Background Workers process them to prevent blocking.
- **Atomic Transactions**: Lua scripts in Redis and InnoDB transactions in MySQL ensure zero race conditions when deducting or refunding balances.
- **Dynamic Configuration**: Fully parameterized ports, replica counts, and business logic (like `SMS_COST`) via `.env`.

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
