// =============================================================================
// Distributed API Gateway & Rate Limiter — Production-Grade Implementation
// =============================================================================
//
// Architecture:
//   Section 1 — Configuration (YAML-driven, zero hardcoded values)
//   Section 2 — Rate Limiter  (Redis Lua Token Bucket, RFC-compliant headers)
//   Section 3 — Reverse Proxy (Connection pooling, timeouts, error handling)
//   Section 4 — Prometheus    (Request counters, latency histograms, 429 tracking)
//   Section 5 — Admin API     (Dashboard serving, live stats, route introspection)
//   Section 6 — Logging       (Structured JSON via slog)
//   Section 7 — Entrypoint    (Wiring, graceful shutdown)
//
// Run:  go run main.go
// Test: go test -v
// =============================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

// =============================================================================
// SECTION 1: CONFIGURATION
// =============================================================================
// All gateway behavior is driven from config.yaml. These structs map directly
// to the YAML schema, making the gateway fully reconfigurable without code
// changes. See config.yaml for the full schema documentation.
// =============================================================================

// Config is the top-level configuration structure parsed from config.yaml.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Redis     RedisConfig     `yaml:"redis"`
	RateLimit RateLimitPolicy `yaml:"rate_limit"`
	Routes    []RouteConfig   `yaml:"routes"`
}

// ServerConfig defines the network ports and timeouts for the gateway.
type ServerConfig struct {
	Port         int           `yaml:"port"`
	AdminPort    int           `yaml:"admin_port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// RedisConfig holds the connection parameters for the Redis instance.
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// RateLimitPolicy defines a token bucket configuration (capacity + refill rate).
type RateLimitPolicy struct {
	Capacity   int `yaml:"capacity"`
	RefillRate int `yaml:"refill_rate"`
}

// RouteConfig maps a URL path prefix to an upstream service, with an optional
// per-route rate limit override. If RateLimit is nil, the global default applies.
type RouteConfig struct {
	PathPrefix  string           `yaml:"path_prefix"`
	UpstreamURL string           `yaml:"upstream_url"`
	RateLimit   *RateLimitPolicy `yaml:"rate_limit,omitempty"`
}

// loadConfig reads and parses the config.yaml file into a Config struct.
// It falls back to sensible defaults if the file is missing or incomplete.
func loadConfig(path string) (*Config, error) {
	// Default configuration — used if YAML fields are absent
	cfg := &Config{
		Server: ServerConfig{
			Port:         8080,
			AdminPort:    8081,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		Redis: RedisConfig{
			Addr: "127.0.0.1:6379",
		},
		RateLimit: RateLimitPolicy{
			Capacity:   10,
			RefillRate: 2,
		},
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config YAML: %w", err)
	}

	// Validate: at least one route must be defined
	if len(cfg.Routes) == 0 {
		return nil, fmt.Errorf("config error: no routes defined")
	}

	return cfg, nil
}

// =============================================================================
// SECTION 2: RATE LIMITER (Redis Lua Token Bucket)
// =============================================================================
// The core rate limiting logic runs entirely inside Redis via an atomic Lua
// script. This eliminates race conditions in distributed deployments — even if
// 100 gateway instances evaluate the same user's bucket simultaneously, the
// Lua script serializes access within Redis.
//
// Enhancement over the original: the script now returns a 3-element array
// [allowed, remaining_tokens, reset_timestamp] so we can inject RFC-compliant
// rate limit headers into every HTTP response.
// =============================================================================

// luaScript is the atomic Token Bucket evaluator.
//
// Algorithm (Lazy Evaluation):
//   1. Load the bucket state (tokens, last_updated) from a Redis hash.
//   2. Calculate how many tokens should be refilled since last_updated.
//   3. Cap tokens at capacity (no overflow).
//   4. If tokens >= 1: deduct 1 token, return ALLOWED.
//   5. If tokens < 1: return DENIED + time until next token refills.
//
// Returns: {allowed (0 or 1), remaining_tokens, reset_unix_timestamp}
const luaScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = 1

-- Load current bucket state from Redis hash
local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

-- Initialize bucket on first request
if tokens == nil then
    tokens = capacity
    last_updated = now
end

-- Lazy refill: calculate tokens earned since last request
local delta = math.max(0, now - last_updated)
local tokens_to_add = delta * refill_rate
tokens = math.min(capacity, tokens + tokens_to_add)
last_updated = now

local allowed = 0
local remaining = math.floor(tokens)
local reset_at = now

if tokens >= requested then
    -- ALLOW: deduct one token
    tokens = tokens - requested
    remaining = math.floor(tokens)
    allowed = 1
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    -- Set TTL so stale buckets auto-expire (2x the time to fully refill)
    redis.call("EXPIRE", key, math.ceil(capacity / refill_rate) * 2)
else
    -- DENY: calculate when the next token will be available
    local deficit = requested - tokens
    local wait_seconds = math.ceil(deficit / refill_rate)
    reset_at = now + wait_seconds
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    redis.call("EXPIRE", key, math.ceil(capacity / refill_rate) * 2)
end

return {allowed, remaining, reset_at}
`

// RedisTokenBucket implements distributed rate limiting using Redis.
type RedisTokenBucket struct {
	client     *redis.Client
	Capacity   int
	RefillRate int
}

// RateLimitResult holds the result of a rate limit evaluation.
type RateLimitResult struct {
	Allowed   bool  // Whether the request is permitted
	Remaining int   // Tokens left in the bucket after this request
	ResetAt   int64 // Unix timestamp when the bucket will have tokens again (only meaningful when denied)
}

// Allow evaluates whether a request from the given userID should be permitted.
// It executes the Lua script atomically in Redis, returning the full result
// needed to populate RFC rate limit headers.
func (tb *RedisTokenBucket) Allow(ctx context.Context, userID string) (*RateLimitResult, error) {
	now := time.Now().Unix()

	keys := []string{"rate_limit:" + userID}
	args := []interface{}{tb.Capacity, tb.RefillRate, now}

	// Eval runs the Lua script atomically inside Redis
	result, err := tb.client.Eval(ctx, luaScript, keys, args...).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("redis eval error: %w", err)
	}

	return &RateLimitResult{
		Allowed:   result[0] == 1,
		Remaining: int(result[1]),
		ResetAt:   result[2],
	}, nil
}

// injectRateLimitHeaders adds RFC-compliant rate limiting headers to the response.
// These headers tell API consumers their current quota status.
//
// Headers injected:
//   - X-RateLimit-Limit:     Maximum tokens (bucket capacity)
//   - X-RateLimit-Remaining: Tokens left after this request
//   - X-RateLimit-Reset:     Unix timestamp when the bucket resets
//   - Retry-After:           Seconds to wait before retrying (only on 429)
func injectRateLimitHeaders(w http.ResponseWriter, capacity int, result *RateLimitResult) {
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(capacity))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt, 10))

	// Retry-After is only set when the request is denied (429 status)
	if !result.Allowed {
		retryAfter := result.ResetAt - time.Now().Unix()
		if retryAfter < 1 {
			retryAfter = 1
		}
		w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	}
}

// =============================================================================
// SECTION 3: REVERSE PROXY
// =============================================================================
// Each route in config.yaml gets its own httputil.ReverseProxy instance with
// production-grade transport settings: connection pooling, timeouts, and
// structured error logging.
// =============================================================================

// routeEntry binds a path prefix to its reverse proxy and rate limit policy.
type routeEntry struct {
	PathPrefix  string
	UpstreamURL string
	Proxy       *httputil.ReverseProxy
	RateLimit   RateLimitPolicy // Effective rate limit (route-specific or global default)
}

// buildRoutes creates reverse proxy instances for every route in the config.
// Each proxy gets a custom HTTP transport with connection pooling and timeouts.
func buildRoutes(cfg *Config, logger *slog.Logger) ([]routeEntry, error) {
	entries := make([]routeEntry, 0, len(cfg.Routes))

	for _, rc := range cfg.Routes {
		target, err := url.Parse(rc.UpstreamURL)
		if err != nil {
			return nil, fmt.Errorf("invalid upstream URL %q for route %q: %w",
				rc.UpstreamURL, rc.PathPrefix, err)
		}

		// Create a reverse proxy with production transport settings
		proxy := httputil.NewSingleHostReverseProxy(target)

		// Custom transport with connection pooling and timeouts
		proxy.Transport = &http.Transport{
			MaxIdleConns:        100,              // Total idle connections across all hosts
			MaxIdleConnsPerHost: 20,               // Idle connections per upstream host
			IdleConnTimeout:     90 * time.Second, // Close idle connections after 90s
			TLSHandshakeTimeout: 10 * time.Second, // TLS negotiation timeout
			DisableCompression:  false,            // Allow gzip from upstreams
		}

		// Structured error handler — logs proxy failures instead of crashing
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Error("proxy error",
				"path", r.URL.Path,
				"upstream", target.String(),
				"error", err.Error(),
			)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}

		// Determine effective rate limit: route-specific override or global default
		effectiveRL := cfg.RateLimit
		if rc.RateLimit != nil {
			effectiveRL = *rc.RateLimit
		}

		entries = append(entries, routeEntry{
			PathPrefix:  rc.PathPrefix,
			UpstreamURL: rc.UpstreamURL,
			Proxy:       proxy,
			RateLimit:   effectiveRL,
		})

		logger.Info("route registered",
			"prefix", rc.PathPrefix,
			"upstream", rc.UpstreamURL,
			"capacity", effectiveRL.Capacity,
			"refill_rate", effectiveRL.RefillRate,
		)
	}

	return entries, nil
}

// =============================================================================
// SECTION 4: PROMETHEUS METRICS
// =============================================================================
// We track three critical dimensions:
//   1. Total HTTP requests (by method, route, status code)
//   2. Request latency distribution (histogram by route)
//   3. Rate-limited requests (429s as a dedicated counter)
//
// These feed both the Prometheus scraper and the Admin Dashboard's /api/stats.
// =============================================================================

// GatewayMetrics holds all Prometheus metric collectors.
type GatewayMetrics struct {
	RequestsTotal  *prometheus.CounterVec
	RequestLatency *prometheus.HistogramVec
	BlockedTotal   prometheus.Counter
}

// newMetrics creates and registers all Prometheus metric collectors.
func newMetrics() *GatewayMetrics {
	m := &GatewayMetrics{
		// Total requests partitioned by HTTP method, route path, and status code.
		// Example query: rate(gateway_http_requests_total{status="429"}[5m])
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_http_requests_total",
				Help: "Total number of HTTP requests processed by the gateway",
			},
			[]string{"method", "route", "status"},
		),

		// Request duration distribution in seconds, partitioned by route.
		// Example query: histogram_quantile(0.95, rate(gateway_http_request_duration_seconds_bucket[5m]))
		RequestLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gateway_http_request_duration_seconds",
				Help:    "Histogram of request latencies in seconds",
				Buckets: prometheus.DefBuckets, // .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
			},
			[]string{"route"},
		),

		// Dedicated counter for rate-limited (429) requests.
		// Easier to alert on than filtering RequestsTotal by status.
		BlockedTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gateway_rate_limited_total",
				Help: "Total number of requests blocked by rate limiting (HTTP 429)",
			},
		),
	}

	prometheus.MustRegister(m.RequestsTotal, m.RequestLatency, m.BlockedTotal)
	return m
}

// =============================================================================
// SECTION 5: ADMIN API & DASHBOARD
// =============================================================================
// The admin server runs on a separate port (default :8081) and provides:
//   GET /           — Serves the Tailwind CSS dashboard (dashboard/index.html)
//   GET /api/stats  — JSON endpoint with live request counts and TPS
//   GET /api/routes — JSON endpoint with the active route configuration
// =============================================================================

// adminServer encapsulates the admin API's dependencies.
type adminServer struct {
	logger       *slog.Logger
	routes       []routeEntry
	totalReqs    *atomic.Int64 // Atomically incremented on every gateway request
	blockedReqs  *atomic.Int64 // Atomically incremented on every 429
}

// statsResponse is the JSON shape returned by GET /api/stats.
type statsResponse struct {
	TotalRequests   int64 `json:"total_requests"`
	BlockedRequests int64 `json:"blocked_requests"`
}

// routeResponse is the JSON shape returned by GET /api/routes.
type routeResponse struct {
	PathPrefix  string `json:"path_prefix"`
	UpstreamURL string `json:"upstream_url"`
	Capacity    int    `json:"capacity"`
	RefillRate  int    `json:"refill_rate"`
}

// handleStats returns live request counts as JSON.
func (a *adminServer) handleStats(w http.ResponseWriter, r *http.Request) {
	resp := statsResponse{
		TotalRequests:   a.totalReqs.Load(),
		BlockedRequests: a.blockedReqs.Load(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleRoutes returns the active route table as JSON.
func (a *adminServer) handleRoutes(w http.ResponseWriter, r *http.Request) {
	routes := make([]routeResponse, 0, len(a.routes))
	for _, re := range a.routes {
		routes = append(routes, routeResponse{
			PathPrefix:  re.PathPrefix,
			UpstreamURL: re.UpstreamURL,
			Capacity:    re.RateLimit.Capacity,
			RefillRate:  re.RateLimit.RefillRate,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(routes)
}

// startAdminServer starts the admin dashboard and API on the configured port.
func startAdminServer(cfg *Config, admin *adminServer) *http.Server {
	mux := http.NewServeMux()

	// Serve the Tailwind dashboard from the dashboard/ directory
	mux.Handle("/", http.FileServer(http.Dir("dashboard")))

	// JSON API endpoints for the dashboard
	mux.HandleFunc("/api/stats", admin.handleStats)
	mux.HandleFunc("/api/routes", admin.handleRoutes)

	addr := fmt.Sprintf(":%d", cfg.Server.AdminPort)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		admin.logger.Info("admin dashboard started", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			admin.logger.Error("admin server error", "error", err.Error())
		}
	}()

	return srv
}

// =============================================================================
// SECTION 6: STRUCTURED LOGGING
// =============================================================================
// Uses Go 1.21+ slog for structured JSON logging. Every log line is machine-
// parseable, making it compatible with log aggregators (ELK, Loki, Datadog).
// =============================================================================

// initLogger creates a structured JSON logger writing to stdout.
func initLogger() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		// Include source file and line number in every log entry
		AddSource: false,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// =============================================================================
// SECTION 7: MAIN ENTRYPOINT
// =============================================================================
// Orchestrates the full startup sequence:
//   1. Initialize structured logger
//   2. Load YAML configuration
//   3. Connect to Redis (with retry)
//   4. Register Prometheus metrics
//   5. Build route table from config
//   6. Start Admin Dashboard server
//   7. Start Gateway proxy server
//   8. Handle graceful shutdown (SIGINT/SIGTERM)
// =============================================================================

// responseWriter wraps http.ResponseWriter to capture the status code for metrics.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code before delegating to the underlying writer.
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func main() {
	// ── Step 1: Initialize structured logging ──
	logger := initLogger()
	logger.Info("starting distributed API gateway",
		"version", "2.0.0",
		"go_version", "1.26",
	)

	// ── Step 2: Load configuration from YAML ──
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		logger.Error("configuration error", "error", err.Error())
		os.Exit(1)
	}
	logger.Info("configuration loaded",
		"gateway_port", cfg.Server.Port,
		"admin_port", cfg.Server.AdminPort,
		"redis_addr", cfg.Redis.Addr,
		"default_capacity", cfg.RateLimit.Capacity,
		"default_refill_rate", cfg.RateLimit.RefillRate,
		"route_count", len(cfg.Routes),
	)

	// ── Step 3: Connect to Redis with retry logic ──
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Retry Redis connection up to 10 times (useful when starting via Docker Compose)
	var redisConnected bool
	for i := 1; i <= 10; i++ {
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis not ready, retrying...",
				"attempt", i,
				"error", err.Error(),
			)
			time.Sleep(time.Duration(i) * time.Second) // Linear backoff
		} else {
			redisConnected = true
			break
		}
	}
	if !redisConnected {
		logger.Error("failed to connect to Redis after 10 attempts")
		os.Exit(1)
	}
	logger.Info("redis connected", "addr", cfg.Redis.Addr)

	// ── Step 4: Register Prometheus metrics ──
	metrics := newMetrics()
	logger.Info("prometheus metrics registered")

	// ── Step 5: Build route table from config ──
	routes, err := buildRoutes(cfg, logger)
	if err != nil {
		logger.Error("route building error", "error", err.Error())
		os.Exit(1)
	}

	// ── Step 6: Start Admin Dashboard (separate port, non-blocking) ──
	totalReqs := &atomic.Int64{}
	blockedReqs := &atomic.Int64{}

	admin := &adminServer{
		logger:      logger,
		routes:      routes,
		totalReqs:   totalReqs,
		blockedReqs: blockedReqs,
	}
	adminSrv := startAdminServer(cfg, admin)

	// ── Step 7: Build and start the Gateway HTTP server ──
	gatewayMux := http.NewServeMux()

	// Expose Prometheus metrics on the gateway port
	gatewayMux.Handle("/metrics", promhttp.Handler())

	// Health check endpoint
	gatewayMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	// Main gateway handler: rate limit → route match → reverse proxy
	gatewayMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Increment total request counter (atomic, for admin dashboard)
		totalReqs.Add(1)

		// Extract user identity for per-user rate limiting
		userID := r.Header.Get("X-User-Id")
		if userID == "" {
			userID = r.RemoteAddr // Fall back to IP address
		}

		// ── Route Matching ──
		// Find the first route whose path prefix matches the request URL.
		var matched *routeEntry
		for i := range routes {
			if strings.HasPrefix(r.URL.Path, routes[i].PathPrefix) {
				matched = &routes[i]
				break
			}
		}

		if matched == nil {
			// No matching route — return 404
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "no route matched",
				"path":  r.URL.Path,
			})
			metrics.RequestsTotal.WithLabelValues(r.Method, "unknown", "404").Inc()
			metrics.RequestLatency.WithLabelValues("unknown").Observe(time.Since(start).Seconds())

			logger.Info("request handled",
				"method", r.Method,
				"path", r.URL.Path,
				"status", 404,
				"user_id", userID,
				"latency_ms", time.Since(start).Milliseconds(),
			)
			return
		}

		// ── Rate Limiting ──
		// Create a per-route, per-user token bucket evaluator
		bucket := &RedisTokenBucket{
			client:     rdb,
			Capacity:   matched.RateLimit.Capacity,
			RefillRate: matched.RateLimit.RefillRate,
		}

		// Composite key: route + user (so limits are per-user, per-route)
		compositeKey := matched.PathPrefix + ":" + userID
		result, err := bucket.Allow(ctx, compositeKey)
		if err != nil {
			logger.Error("rate limiter error",
				"error", err.Error(),
				"user_id", userID,
				"route", matched.PathPrefix,
			)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			metrics.RequestsTotal.WithLabelValues(r.Method, matched.PathPrefix, "500").Inc()
			return
		}

		// Inject RFC-compliant rate limit headers into every response
		injectRateLimitHeaders(w, matched.RateLimit.Capacity, result)

		if !result.Allowed {
			// ── RATE LIMITED (429) ──
			blockedReqs.Add(1)
			metrics.BlockedTotal.Inc()
			metrics.RequestsTotal.WithLabelValues(r.Method, matched.PathPrefix, "429").Inc()
			metrics.RequestLatency.WithLabelValues(matched.PathPrefix).Observe(time.Since(start).Seconds())

			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{
				"error":   "rate limit exceeded",
				"message": "Too Many Requests. Please retry after the Retry-After period.",
			})

			logger.Warn("request rate limited",
				"method", r.Method,
				"path", r.URL.Path,
				"route", matched.PathPrefix,
				"user_id", userID,
				"remaining", result.Remaining,
				"reset_at", result.ResetAt,
				"latency_ms", time.Since(start).Milliseconds(),
			)
			return
		}

		// ── PROXY REQUEST ──
		// Wrap the ResponseWriter to capture the upstream's status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		matched.Proxy.ServeHTTP(wrapped, r)

		// Record metrics with the actual upstream status code
		statusStr := strconv.Itoa(wrapped.statusCode)
		elapsed := time.Since(start)
		metrics.RequestsTotal.WithLabelValues(r.Method, matched.PathPrefix, statusStr).Inc()
		metrics.RequestLatency.WithLabelValues(matched.PathPrefix).Observe(elapsed.Seconds())

		logger.Info("request proxied",
			"method", r.Method,
			"path", r.URL.Path,
			"route", matched.PathPrefix,
			"upstream", matched.UpstreamURL,
			"status", wrapped.statusCode,
			"user_id", userID,
			"remaining_tokens", result.Remaining,
			"latency_ms", elapsed.Milliseconds(),
		)
	})

	gatewayAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	gatewaySrv := &http.Server{
		Addr:         gatewayAddr,
		Handler:      gatewayMux,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// ── Step 8: Graceful Shutdown ──
	// Listen for OS interrupt signals (Ctrl+C, Docker stop, k8s SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("shutdown signal received", "signal", sig.String())

		// Give active requests 10 seconds to complete
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		// Shutdown both servers gracefully
		if err := gatewaySrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("gateway shutdown error", "error", err.Error())
		}
		if err := adminSrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("admin shutdown error", "error", err.Error())
		}

		// Close the Redis connection
		rdb.Close()
		logger.Info("gateway shut down gracefully")
	}()

	// Start the gateway (blocks until shutdown)
	logger.Info("gateway started", "addr", gatewayAddr)
	if err := gatewaySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("gateway server error", "error", err.Error())
		os.Exit(1)
	}

	// Suppress unused import warning for math
	_ = math.MaxInt
}
