# Distributed API Gateway & Rate Limiter From Scratch

A production-grade, high-throughput API Gateway and Distributed Rate Limiter built completely from scratch using **Go (Golang)** and **Redis**. 

This system acts as a reverse proxy capable of routing incoming traffic to distinct upstream microservices while enforcing strict rate limits via the **Token Bucket Algorithm**. By leveraging centralized **Redis Lua scripts**, the architecture ensures atomic operations, completely eliminating race conditions across distributed gateway instances without active background resource overhead.

---

## ⚙️ Architecture & Design Choices

### 1. Token Bucket via Lazy Evaluation
Instead of spawning resource-heavy background worker threads (Go tickers) per user to constantly refresh token counts, this gateway utilizes **lazy evaluation**. The bucket state is calculated dynamically only when a request actually arrives, dropping the time complexity to $O(1)$ and preserving CPU cycles.

### 2. Distributed Concurrency Control (Atomic Lua Scripts)
In a multi-node deployment, in-memory rate limiting fails due to state isolation. This project offloads the core math to Redis. To prevent concurrent race conditions (e.g., two simultaneous requests exploiting a single remaining token), the evaluation is wrapped in an atomic Lua script executed entirely within Redis.

### 3. Reverse Proxy Routing
Leveraging Go's `net/http/httputil` package, the gateway inspects incoming URL paths dynamically, rewrites headers, and proxies requests to isolated backend services seamlessly.

---

## 🛠️ Technology Stack

* **Language:** Go (Golang)
* **Storage/Caching:** Redis (Alpine)
* **Containerization:** Docker & Docker Compose
* **Testing:** Go `testing` suite

---

## 📂 Project Structure

```text
├── docker-compose.yml     # Infrastructure blueprint (Redis, dummy upstream services)
├── go.mod                 # Go module definition
├── go.sum                 # Go dependency checksums
├── main.go                # Gateway entrypoint, proxy routing, and Redis connection
└── main_test.go           # Integration testing suite for rate limiting math
