package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

// Config holds top-level gateway configuration parsed from YAML.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Redis     RedisConfig     `yaml:"redis"`
	RateLimit RateLimitPolicy `yaml:"rate_limit"`
	Routes    []RouteConfig   `yaml:"routes"`
}

// ServerConfig defines gateway network listeners and operational timeouts.
type ServerConfig struct {
	Port         int           `yaml:"port"`
	AdminPort    int           `yaml:"admin_port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// RedisConfig defines connection credentials for the shared Redis cache.
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// RateLimitPolicy defines token bucket burst capacity and refill velocity.
type RateLimitPolicy struct {
	Capacity   int `yaml:"capacity"`
	RefillRate int `yaml:"refill_rate"`
}

// RouteConfig maps an incoming URL prefix to a target backend service.
type RouteConfig struct {
	PathPrefix  string           `yaml:"path_prefix"`
	UpstreamURL string           `yaml:"upstream_url"`
	RateLimit   *RateLimitPolicy `yaml:"rate_limit,omitempty"`
}

func loadConfig(path string) (*Config, error) {
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
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config yaml: %w", err)
	}

	if len(cfg.Routes) == 0 {
		return nil, fmt.Errorf("config error: at least one route must be defined")
	}

	return cfg, nil
}

// Atomic Token Bucket using lazy evaluation.
// Returns {allowed (0/1), remaining_tokens, reset_timestamp}.
const luaScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = 1

local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
end

-- Lazy refill based on elapsed time since previous request
local delta = math.max(0, now - last_updated)
local tokens_to_add = delta * refill_rate
tokens = math.min(capacity, tokens + tokens_to_add)
last_updated = now

local allowed = 0
local remaining = math.floor(tokens)
local reset_at = now

if tokens >= requested then
    tokens = tokens - requested
    remaining = math.floor(tokens)
    allowed = 1
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    -- TTL set to 2x refill duration to auto-prune idle buckets
    redis.call("EXPIRE", key, math.ceil(capacity / refill_rate) * 2)
else
    local deficit = requested - tokens
    local wait_seconds = math.ceil(deficit / refill_rate)
    reset_at = now + wait_seconds
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    redis.call("EXPIRE", key, math.ceil(capacity / refill_rate) * 2)
end

return {allowed, remaining, reset_at}
`

// RedisTokenBucket coordinates distributed rate limiting via Redis Lua scripts.
type RedisTokenBucket struct {
	client     *redis.Client
	Capacity   int
	RefillRate int
}

// RateLimitResult contains quota decision data and header payload values.
type RateLimitResult struct {
	Allowed   bool
	Remaining int
	ResetAt   int64
}

// Allow executes atomic token deduction in Redis and returns quota state.
func (tb *RedisTokenBucket) Allow(ctx context.Context, userID string) (*RateLimitResult, error) {
	now := time.Now().Unix()
	keys := []string{"rate_limit:" + userID}
	args := []interface{}{tb.Capacity, tb.RefillRate, now}

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

func injectRateLimitHeaders(w http.ResponseWriter, capacity int, result *RateLimitResult) {
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(capacity))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt, 10))

	if !result.Allowed {
		retryAfter := result.ResetAt - time.Now().Unix()
		if retryAfter < 1 {
			retryAfter = 1
		}
		w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	}
}

type routeEntry struct {
	PathPrefix  string
	UpstreamURL string
	Proxy       *httputil.ReverseProxy
	RateLimit   RateLimitPolicy
}

func buildRoutes(cfg *Config, logger *slog.Logger) ([]routeEntry, error) {
	entries := make([]routeEntry, 0, len(cfg.Routes))

	for _, rc := range cfg.Routes {
		target, err := url.Parse(rc.UpstreamURL)
		if err != nil {
			return nil, fmt.Errorf("invalid upstream URL %q for route %q: %w", rc.UpstreamURL, rc.PathPrefix, err)
		}

		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.Transport = &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableCompression:  false,
		}

		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Error("proxy error",
				"path", r.URL.Path,
				"upstream", target.String(),
				"error", err.Error(),
			)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}

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

// GatewayMetrics registers Prometheus collectors for telemetry and alerting.
type GatewayMetrics struct {
	RequestsTotal  *prometheus.CounterVec
	RequestLatency *prometheus.HistogramVec
	BlockedTotal   prometheus.Counter
}

func newMetrics() *GatewayMetrics {
	m := &GatewayMetrics{
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gateway_http_requests_total",
				Help: "Total number of HTTP requests processed by the gateway",
			},
			[]string{"method", "route", "status"},
		),
		RequestLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gateway_http_request_duration_seconds",
				Help:    "Histogram of request latencies in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"route"},
		),
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

type adminServer struct {
	logger      *slog.Logger
	routes      []routeEntry
	totalReqs   *atomic.Int64
	blockedReqs *atomic.Int64
}

type statsResponse struct {
	TotalRequests   int64 `json:"total_requests"`
	BlockedRequests int64 `json:"blocked_requests"`
}

type routeResponse struct {
	PathPrefix  string `json:"path_prefix"`
	UpstreamURL string `json:"upstream_url"`
	Capacity    int    `json:"capacity"`
	RefillRate  int    `json:"refill_rate"`
}

func (a *adminServer) handleStats(w http.ResponseWriter, r *http.Request) {
	resp := statsResponse{
		TotalRequests:   a.totalReqs.Load(),
		BlockedRequests: a.blockedReqs.Load(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

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

func startAdminServer(cfg *Config, admin *adminServer) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("dashboard")))
	mux.HandleFunc("/api/stats", admin.handleStats)
	mux.HandleFunc("/api/routes", admin.handleRoutes)

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Server.AdminPort)
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

func initLogger() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: false,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func main() {
	logger := initLogger()
	logger.Info("starting distributed API gateway", "version", "2.0.0")

	cfg, err := loadConfig("config.yaml")
	if err != nil {
		logger.Error("configuration error", "error", err.Error())
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for Redis readiness with backoff before accepting traffic
	var redisConnected bool
	for i := 1; i <= 10; i++ {
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis not ready, retrying...", "attempt", i, "error", err.Error())
			time.Sleep(time.Duration(i) * time.Second)
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

	metrics := newMetrics()
	routes, err := buildRoutes(cfg, logger)
	if err != nil {
		logger.Error("route building error", "error", err.Error())
		os.Exit(1)
	}

	totalReqs := &atomic.Int64{}
	blockedReqs := &atomic.Int64{}

	admin := &adminServer{
		logger:      logger,
		routes:      routes,
		totalReqs:   totalReqs,
		blockedReqs: blockedReqs,
	}
	adminSrv := startAdminServer(cfg, admin)

	gatewayMux := http.NewServeMux()
	gatewayMux.Handle("/metrics", promhttp.Handler())
	gatewayMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	gatewayMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		totalReqs.Add(1)

		userID := r.Header.Get("X-User-Id")
		if userID == "" {
			userID = r.RemoteAddr
		}

		var matched *routeEntry
		for i := range routes {
			if strings.HasPrefix(r.URL.Path, routes[i].PathPrefix) {
				matched = &routes[i]
				break
			}
		}

		if matched == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "no route matched",
				"path":  r.URL.Path,
			})
			metrics.RequestsTotal.WithLabelValues(r.Method, "unknown", "404").Inc()
			metrics.RequestLatency.WithLabelValues("unknown").Observe(time.Since(start).Seconds())
			return
		}

		bucket := &RedisTokenBucket{
			client:     rdb,
			Capacity:   matched.RateLimit.Capacity,
			RefillRate: matched.RateLimit.RefillRate,
		}

		compositeKey := matched.PathPrefix + ":" + userID
		result, err := bucket.Allow(ctx, compositeKey)
		if err != nil {
			logger.Error("rate limiter error", "error", err.Error(), "user_id", userID, "route", matched.PathPrefix)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			metrics.RequestsTotal.WithLabelValues(r.Method, matched.PathPrefix, "500").Inc()
			return
		}

		injectRateLimitHeaders(w, matched.RateLimit.Capacity, result)

		if !result.Allowed {
			blockedReqs.Add(1)
			metrics.BlockedTotal.Inc()
			metrics.RequestsTotal.WithLabelValues(r.Method, matched.PathPrefix, "429").Inc()
			metrics.RequestLatency.WithLabelValues(matched.PathPrefix).Observe(time.Since(start).Seconds())

			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{
				"error":   "rate limit exceeded",
				"message": "Too Many Requests. Please retry after the Retry-After period.",
			})
			return
		}

		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		matched.Proxy.ServeHTTP(wrapped, r)

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
			"latency_ms", elapsed.Milliseconds(),
		)
	})

	gatewayAddr := fmt.Sprintf("0.0.0.0:%d", cfg.Server.Port)
	gatewaySrv := &http.Server{
		Addr:         gatewayAddr,
		Handler:      gatewayMux,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("shutdown signal received", "signal", sig.String())

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		_ = gatewaySrv.Shutdown(shutdownCtx)
		_ = adminSrv.Shutdown(shutdownCtx)
		rdb.Close()
		logger.Info("gateway shut down gracefully")
	}()

	logger.Info("gateway started", "addr", gatewayAddr)
	if err := gatewaySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("gateway server error", "error", err.Error())
		os.Exit(1)
	}
}
