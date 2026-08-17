package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// =============================================================================
// Integration Test for the Distributed Rate Limiter
// =============================================================================
// Prerequisites: A running Redis instance on 127.0.0.1:6379
//   docker run -d -p 6379:6379 redis:alpine
//
// Run: go test -v -run TestRateLimiter
// =============================================================================

func getRedisClient(t *testing.T) *redis.Client {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis is not running on 127.0.0.1:6379 — start it with: docker run -d -p 6379:6379 redis:alpine\nError: %v", err)
	}
	return rdb
}

func TestRateLimiter(t *testing.T) {
	ctx := context.Background()
	rdb := getRedisClient(t)

	// Clean slate
	rdb.Del(ctx, "rate_limit:test_user")

	bucket := &RedisTokenBucket{
		client:     rdb,
		Capacity:   5,
		RefillRate: 1,
	}

	for i := 1; i <= 6; i++ {
		result, err := bucket.Allow(ctx, "test_user")
		if err != nil {
			t.Fatalf("Request %d: unexpected Redis error: %v", i, err)
		}

		if i <= 5 {
			if !result.Allowed {
				t.Errorf("Request %d: was BLOCKED but should have been ALLOWED (remaining: %d)", i, result.Remaining)
			}
			expectedRemaining := 5 - i
			if result.Remaining != expectedRemaining {
				t.Errorf("Request %d: remaining=%d, want=%d", i, result.Remaining, expectedRemaining)
			}
		} else {
			if result.Allowed {
				t.Errorf("Request %d: was ALLOWED but should have been RATE LIMITED", i)
			}
			if result.Remaining != 0 {
				t.Errorf("Request %d: remaining=%d, want=0", i, result.Remaining)
			}
			if result.ResetAt <= time.Now().Unix() {
				t.Errorf("Request %d: reset_at=%d should be in the future", i, result.ResetAt)
			}
		}
	}
}

func TestRateLimiterRefill(t *testing.T) {
	ctx := context.Background()
	rdb := getRedisClient(t)

	rdb.Del(ctx, "rate_limit:refill_test_user")

	bucket := &RedisTokenBucket{
		client:     rdb,
		Capacity:   2,
		RefillRate: 2, // 2 tokens per second
	}

	for i := 0; i < 2; i++ {
		result, err := bucket.Allow(ctx, "refill_test_user")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !result.Allowed {
			t.Fatalf("Request %d should have been allowed", i+1)
		}
	}

	result, err := bucket.Allow(ctx, "refill_test_user")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Allowed {
		t.Fatal("Request should have been blocked (bucket empty)")
	}

	time.Sleep(1100 * time.Millisecond) // Wait for tokens to refill

	result, err = bucket.Allow(ctx, "refill_test_user")
	if err != nil {
		t.Fatalf("Unexpected error after refill: %v", err)
	}
	if !result.Allowed {
		t.Error("Request should have been allowed after token refill")
	}
}

// TestHTTPServerIntegration tests the HTTP handler using httptest to ensure
// reverse proxying and RFC-compliant rate limit headers are functioning correctly.
func TestHTTPServerIntegration(t *testing.T) {
	// 1. Setup mock upstream server
	upstreamCalls := 0
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("upstream response"))
	}))
	defer upstreamSrv.Close()

	// 2. Prepare Config mapping a route to the mock upstream
	cfg := &Config{
		RateLimit: RateLimitPolicy{Capacity: 3, RefillRate: 1},
		Routes: []RouteConfig{
			{
				PathPrefix:  "/api",
				UpstreamURL: upstreamSrv.URL,
				RateLimit:   &RateLimitPolicy{Capacity: 3, RefillRate: 1},
			},
		},
	}

	// 3. Setup dependencies (Logger, Redis, Metrics)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	routes, err := buildRoutes(cfg, logger)
	if err != nil {
		t.Fatalf("buildRoutes failed: %v", err)
	}
	rdb := getRedisClient(t)
	// Make sure we have a fresh registry for the test so we don't panic on duplicate metric registrations
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	metrics := newMetrics()

	// Reset rate limit for our test user
	ctx := context.Background()
	testUserID := "http_integration_user"
	rdb.Del(ctx, "/api:"+testUserID)

	// 4. Create the Gateway HTTP Handler (reproducing the logic from main.go)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		userID := r.Header.Get("X-User-Id")
		if userID == "" {
			userID = "anonymous"
		}

		matched := &routes[0] // Simple match for test
		bucket := &RedisTokenBucket{
			client:     rdb,
			Capacity:   matched.RateLimit.Capacity,
			RefillRate: matched.RateLimit.RefillRate,
		}

		compositeKey := matched.PathPrefix + ":" + userID
		result, err := bucket.Allow(r.Context(), compositeKey)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		injectRateLimitHeaders(w, matched.RateLimit.Capacity, result)

		if !result.Allowed {
			metrics.BlockedTotal.Inc()
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("Too Many Requests"))
			return
		}

		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		matched.Proxy.ServeHTTP(wrapped, r)
		metrics.RequestsTotal.WithLabelValues(r.Method, matched.PathPrefix, strconv.Itoa(wrapped.statusCode)).Inc()
		metrics.RequestLatency.WithLabelValues(matched.PathPrefix).Observe(time.Since(start).Seconds())
	})

	// 5. Run tests against the handler
	for i := 1; i <= 4; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
		req.Header.Set("X-User-Id", testUserID)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		res := rec.Result()

		// Verify RFC Headers are always present
		if res.Header.Get("X-RateLimit-Limit") != "3" {
			t.Errorf("Request %d: Expected X-RateLimit-Limit=3, got %s", i, res.Header.Get("X-RateLimit-Limit"))
		}
		if res.Header.Get("X-RateLimit-Remaining") == "" {
			t.Errorf("Request %d: Missing X-RateLimit-Remaining header", i)
		}
		if res.Header.Get("X-RateLimit-Reset") == "" {
			t.Errorf("Request %d: Missing X-RateLimit-Reset header", i)
		}

		if i <= 3 {
			// First 3 requests should be 200 OK
			if res.StatusCode != http.StatusOK {
				t.Errorf("Request %d: Expected 200 OK, got %d", i, res.StatusCode)
			}
			if res.Header.Get("Retry-After") != "" {
				t.Errorf("Request %d: Did not expect Retry-After header on 200 OK", i)
			}
		} else {
			// 4th request should be 429 Too Many Requests
			if res.StatusCode != http.StatusTooManyRequests {
				t.Errorf("Request %d: Expected 429 Too Many Requests, got %d", i, res.StatusCode)
			}
			if res.Header.Get("Retry-After") == "" {
				t.Errorf("Request %d: Missing Retry-After header on 429", i)
			}
		}
	}

	if upstreamCalls != 3 {
		t.Errorf("Expected upstream to be called exactly 3 times, was called %d times", upstreamCalls)
	}
}

func TestConfigLoader(t *testing.T) {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config.yaml: %v", err)
	}
	if cfg.Server.Port == 0 || cfg.Server.AdminPort == 0 {
		t.Error("Server ports should not be 0")
	}
	if cfg.RateLimit.Capacity <= 0 || cfg.RateLimit.RefillRate <= 0 {
		t.Error("Default rate limit values must be positive")
	}
	if len(cfg.Routes) == 0 {
		t.Error("At least one route must be configured")
	}
}