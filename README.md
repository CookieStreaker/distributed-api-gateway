# ⚡ Distributed API Gateway & Traffic Regulator

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://golang.org/)
[![Redis](https://img.shields.io/badge/Redis-Alpine-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![k6 Verified](https://img.shields.io/badge/k6-Load%20Tested-7D64FF?style=for-the-badge&logo=k6&logoColor=white)](https://k6.io/)
[![Latency](https://img.shields.io/badge/P95%20Latency-%3C1.3ms-success?style=for-the-badge)](https://k6.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

A high-throughput, enterprise-grade distributed API Gateway and Reverse Proxy engineered from scratch in Go. Designed for microservice architectures requiring strict multi-tenant traffic regulation, atomic Token Bucket rate limiting via centralized Redis Lua scripts, real-time Prometheus telemetry, and sub-2ms routing overhead with zero distributed race conditions.

---

## 📸 Visual Showcase

| Admin Telemetry Dashboard (`:8081`) | Concurrency & Stress Proof (`k6`) |
| :---: | :---: |
| ![Admin Dashboard](assets/dashboard.png) | ![Benchmark Results](assets/benchmark.png) |

---

## 🏛️ Core Architectural Highlights

```mermaid
flowchart TD
    Client(["🌐 Client Applications"]) -->|HTTP Request| Gateway["⚡ Distributed Go API Gateway (:8080)"]
    
    subgraph Gateway Core
        direction TB
        Matcher["1. Route Matcher (YAML-driven)"]
        Limiter["2. Atomic Token Bucket (Lua Engine)"]
        Metrics["3. Telemetry Interceptor (Prometheus / slog)"]
        Proxy["4. Reverse Proxy Pool (Keep-Alive Transport)"]
        
        Matcher --> Limiter
        Limiter --> Metrics
        Metrics --> Proxy
    end

    Limiter <-->|Atomic Lua Exec $O(1)$| Redis[("🧠 Centralized Redis Cluster")]
    Proxy -->|Proxy /users| SvcUsers["👤 User Microservice"]
    Proxy -->|Proxy /products| SvcProducts["📦 Product Microservice"]
    
    Prometheus["📊 Prometheus Server (:9090)"] -.->|Scrape /metrics| Gateway
    AdminUI["💻 Dark Mode Admin Dashboard (:8081)"] <-->|REST Polling| Gateway
```

### 1. Atomic Token Bucket via Lazy Evaluation ($O(1)$ Complexity)
Traditional in-memory rate limiters suffer from distributed state drift when scaled horizontally, while naive Redis implementations introduce race conditions via multi-step read-modify-write calls (`GET` $\to$ compute $\to$ `SET`). 
- **Atomic Lua Engine:** The entire token calculation executes inside a single Redis Lua script, guaranteeing atomic execution without mutex lock contention or distributed race conditions.
- **Lazy Mathematical Refill:** Rather than using resource-heavy background worker tickers to continuously replenish tokens for millions of keys, the bucket state is computed dynamically on-demand when a request arrives:
  $$\Delta t = \text{now} - \text{last\_updated}$$
  $$\text{tokens} = \min(\text{capacity}, \text{tokens} + \Delta t \times \text{refill\_rate})$$
- **Automatic Key Expiry:** Buckets utilize dynamic TTLs ($2 \times \text{time to refill}$) to prevent key leaks and unbounded memory growth in Redis.

### 2. Dynamic Reverse Proxying & Connection Pooling
- Completely YAML-configurable routing topology via `config.yaml`.
- Uses optimized `net/http/httputil.ReverseProxy` configurations with customized `http.Transport` connection pools (`MaxIdleConns: 100`, `MaxIdleConnsPerHost: 20`, `IdleConnTimeout: 90s`) to eliminate TCP socket exhaustion under burst traffic.
- Built-in circuit/error handling with structured 502 Bad Gateway responses and automatic upstream error logging.

### 3. Strict RFC-Compliant Rate Limit Headers
Every response passing through the gateway is injected with standardized HTTP quota headers:
* `X-RateLimit-Limit`: Maximum bucket burst capacity.
* `X-RateLimit-Remaining`: Tokens currently remaining in the user's bucket.
* `X-RateLimit-Reset`: Unix timestamp when the token bucket will be replenished.
* `Retry-After`: Time in seconds the client must wait before retrying (injected on `429 Too Many Requests`).

### 4. Full Observability & Admin UI
* **Structured Logging:** Go `log/slog` structured JSON logs with context-rich fields (`latency_ms`, `user_id`, `route`, `upstream`, `status`).
* **Prometheus Metrics:** Native instrumentation exposing request counters partitioned by method/route/status, latency histograms with standard quantile buckets, and rate-limit drops at `/metrics`.
* **Zero-Dependency Admin Dashboard:** An embedded, responsive dark-mode dashboard styled with Tailwind CSS running on an isolated administrative port (`:8081`) with live TPS calculation, blocked counter animations, and active route introspection.

---

## 📊 Empirical Benchmarks & Load Testing

The gateway was benchmarked using **Grafana k6** running 50 concurrent Virtual Users (VUs) executing sustained burst traffic across `/users` and `/products` endpoints.

```text
          /\      Grafana   /‾‾/
     /\  /  \     |\  __   /  /
    /  \/    \    | |/ /  /   ‾‾\
   /          \   |   (  |  (‾)  |
  / __________ \  |_|\_\  \_____/

  scenarios: (100.00%) 1 scenario, 50 max VUs, 35s duration
  
  ╔══════════════════════════════════════════════════╗
  ║       API GATEWAY LOAD TEST RESULTS              ║
  ╠══════════════════════════════════════════════════╣
  ║  Total Requests:        24819                  ║
  ║  Allowed (200):          1173                  ║
  ║  Rate Limited (429):    23646                  ║
  ║  P95 Latency:          1.20ms                  ║
  ╚══════════════════════════════════════════════════╝
```

### Performance Summary Table

| Metric | Result | Benchmark Significance |
| :--- | :---: | :--- |
| **Total Processed Requests** | **24,819** | High-density load executed across 35s stress window |
| **Peak Throughput** | **~709.08 req/s** | Zero socket drops or deadlocks |
| **Average Latency** | **0.81 ms** | Sub-millisecond baseline overhead |
| **P90 Latency** | **1.08 ms** | Consistent high-percentile latency |
| **P95 Latency** | **1.20 ms** | Guaranteed sub-2ms SLA under maximum contention |
| **5xx Server Errors** | **0 (0.00%)** | Zero gateway failures or Lua runtime exceptions |
| **Header Integrity Checks** | **120,576 / 120,576 (100%)** | 100% pass rate validating RFC headers & response codes |

> [!TIP]
> **What This Proves:** Despite 50 concurrent threads aggressively targeting a finite pool of tokens in Redis simultaneously, **zero race conditions occurred**, **zero 500 errors were thrown**, and rate limits were enforced with sub-2ms latency overhead.

---

## ⚙️ Configuration Reference

The gateway is entirely dynamic. Modify `config.yaml` to register new upstream routes, adjust timeouts, or override rate limit policies without recompiling Go code:

```yaml
server:
  port: 8080            # Reverse proxy ingress
  admin_port: 8081      # Admin dashboard & internal telemetry APIs
  read_timeout: 30s
  write_timeout: 30s

redis:
  addr: "redis:6379"
  password: ""
  db: 0

# Default global rate limiting policy
rate_limit:
  capacity: 10          # Maximum burst bucket capacity
  refill_rate: 2        # Tokens added per second (lazy calculation)

# Route-to-Upstream definitions
routes:
  - path_prefix: "/users"
    upstream_url: "http://user-service:80"
    # Inherits global rate limit (10 capacity, 2/s refill)

  - path_prefix: "/products"
    upstream_url: "http://product-service:80"
    # Route-specific rate limit override
    rate_limit:
      capacity: 5
      refill_rate: 1
```

---

## 🚀 Quickstart & Deployment

### Prerequisites
- [Docker & Docker Compose](https://docs.docker.com/get-docker/) (v20.10+)
- [Go](https://go.dev/dl/) (v1.24+ - optional for local development)
- [k6](https://k6.io/docs/get-started/installation/) (optional for running load tests)

### 1. One-Command Stack Startup
Clone the repository and launch the full 5-container architecture (Gateway, Redis, Prometheus, and 2 Upstream Services):

```bash
docker compose up --build -d
```

### 2. Verify Port Mappings

| Service | Port | Endpoint / Purpose |
| :--- | :---: | :--- |
| **API Gateway Ingress** | `:8080` | `http://localhost:8080/{route}` (Main API Reverse Proxy) |
| **Prometheus Telemetry** | `:8080` | `http://localhost:8080/metrics` (Scrape Endpoint) |
| **Admin Dashboard UI** | `:8081` | `http://localhost:8081` (Live Telemetry & Route Viewer) |
| **Prometheus Dashboard** | `:9090` | `http://localhost:9090` (PromQL Query Console) |
| **Redis Cache** | `:6379` | `localhost:6379` (Distributed Token Bucket Storage) |

### 3. Send Sample Requests
Send test requests with user identification to trigger token consumption and header injection:

```bash
# Request 1: Route to /users upstream (Allowed - 200 OK)
curl -i -H "X-User-Id: alice" http://localhost:8080/users

# Response Headers:
# HTTP/1.1 200 OK
# X-Ratelimit-Limit: 10
# X-Ratelimit-Remaining: 9
# X-Ratelimit-Reset: 1771285891
```

```bash
# Rapidly fire requests to exceed bucket capacity:
for i in {1..12}; do curl -i -H "X-User-Id: alice" http://localhost:8080/users; done

# Rate Limited Response:
# HTTP/1.1 429 Too Many Requests
# Retry-After: 1
# X-Ratelimit-Limit: 10
# X-Ratelimit-Remaining: 0
# X-Ratelimit-Reset: 1771285892
```

---

## 🧪 Testing & Validation

### Run Unit & Integration Tests
Runs test cases including the atomic Lua script validation, refill cycle verifications, and `net/http/httptest` proxy integration:

```bash
go test -v ./...
```

### Execute k6 Load Test
Verify the gateway's throughput and atomic rate limiter on your own hardware:

```bash
k6 run load_test.js
```

---

## 📁 Repository Structure

```text
.
├── config.yaml          # Dynamic gateway routing & rate-limiting definitions
├── dashboard/           # Admin UI web assets
│   └── index.html       # Single-file Tailwind CSS dashboard with dark theme
├── docker-compose.yml   # Multi-container orchestration specification
├── Dockerfile           # Multi-stage minimal Alpine production build
├── go.mod               # Go module dependencies
├── go.sum               # Cryptographic module checksums
├── load_test.js         # k6 high-concurrency stress testing script
├── main.go              # Core gateway engine (proxy, limiter, metrics, admin API)
├── main_test.go         # Integration test suite using httptest & Redis mock
├── prometheus.yml       # Prometheus scraping configuration
└── README.md            # Architectural documentation
```

---

## 🛡️ License

This project is open-source software licensed under the [MIT License](LICENSE).
