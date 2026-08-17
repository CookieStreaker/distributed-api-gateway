package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func getRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis unavailable on 127.0.0.1:6379: %v", err)
	}
	return rdb
}

func TestRateLimiter(t *testing.T) {
	ctx := context.Background()
	rdb := getRedisClient(t)

	rdb.Del(ctx, "rate_limit:test_user")

	bucket := &RedisTokenBucket{
		client:     rdb,
		Capacity:   5,
		RefillRate: 1,
	}

	for i := 1; i <= 6; i++ {
		result, err := bucket.Allow(ctx, "test_user")
		if err != nil {
			t.Fatalf("request %d: redis error: %v", i, err)
		}

		if i <= 5 {
			if !result.Allowed {
				t.Errorf("request %d: expected allowed, got blocked (remaining: %d)", i, result.Remaining)
			}
			expectedRemaining := 5 - i
			if result.Remaining != expectedRemaining {
				t.Errorf("request %d: remaining = %d, want %d", i, result.Remaining, expectedRemaining)
			}
		} else {
			if result.Allowed {
				t.Errorf("request %d: expected rate limit block, got allowed", i)
			}
			if result.Remaining != 0 {
				t.Errorf("request %d: remaining = %d, want 0", i, result.Remaining)
			}
			if result.ResetAt <= time.Now().Unix() {
				t.Errorf("request %d: reset timestamp %d should be in the future", i, result.ResetAt)
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
		RefillRate: 2,
	}

	for i := 0; i < 2; i++ {
		result, err := bucket.Allow(ctx, "refill_test_user")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result.Allowed {
			t.Fatalf("request %d should have been allowed", i+1)
		}
	}

	result, err := bucket.Allow(ctx, "refill_test_user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allowed {
		t.Fatal("bucket should be exhausted")
	}

	// wait for tokens to refill (1s should add 2 tokens)
	time.Sleep(1100 * time.Millisecond)

	result, err = bucket.Allow(ctx, "refill_test_user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Allowed {
		t.Error("expected request to pass after token refill")
	}
}

func TestHTTPServerIntegration(t *testing.T) {
	upstreamCalls := 0
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("upstream response"))
	}))
	defer upstreamSrv.Close()

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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	routes, err := buildRoutes(cfg, logger)
	if err != nil {
		t.Fatalf("buildRoutes: %v", err)
	}

	rdb := getRedisClient(t)
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	metrics := newMetrics()

	ctx := context.Background()
	testUserID := "http_integration_user"
	rdb.Del(ctx, "/api:"+testUserID)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		userID := r.Header.Get("X-User-Id")
		if userID == "" {
			userID = "anonymous"
		}

		matched := &routes[0]
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
			_, _ = w.Write([]byte("Too Many Requests"))
			return
		}

		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		matched.Proxy.ServeHTTP(wrapped, r)
		metrics.RequestsTotal.WithLabelValues(r.Method, matched.PathPrefix, strconv.Itoa(wrapped.statusCode)).Inc()
		metrics.RequestLatency.WithLabelValues(matched.PathPrefix).Observe(time.Since(start).Seconds())
	})

	for i := 1; i <= 4; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
		req.Header.Set("X-User-Id", testUserID)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		res := rec.Result()

		if res.Header.Get("X-RateLimit-Limit") != "3" {
			t.Errorf("request %d: want X-RateLimit-Limit=3, got %s", i, res.Header.Get("X-RateLimit-Limit"))
		}
		if res.Header.Get("X-RateLimit-Remaining") == "" {
			t.Errorf("request %d: missing X-RateLimit-Remaining", i)
		}
		if res.Header.Get("X-RateLimit-Reset") == "" {
			t.Errorf("request %d: missing X-RateLimit-Reset", i)
		}

		if i <= 3 {
			if res.StatusCode != http.StatusOK {
				t.Errorf("request %d: expected 200, got %d", i, res.StatusCode)
			}
			if res.Header.Get("Retry-After") != "" {
				t.Errorf("request %d: unexpected Retry-After header on 200", i)
			}
		} else {
			if res.StatusCode != http.StatusTooManyRequests {
				t.Errorf("request %d: expected 429, got %d", i, res.StatusCode)
			}
			if res.Header.Get("Retry-After") == "" {
				t.Errorf("request %d: missing Retry-After header on 429", i)
			}
		}
	}

	if upstreamCalls != 3 {
		t.Errorf("expected 3 upstream calls, got %d", upstreamCalls)
	}
}

func TestConfigLoader(t *testing.T) {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Server.Port == 0 || cfg.Server.AdminPort == 0 {
		t.Error("ports must not be 0")
	}
	if cfg.RateLimit.Capacity <= 0 || cfg.RateLimit.RefillRate <= 0 {
		t.Error("rate limits must be positive")
	}
	if len(cfg.Routes) == 0 {
		t.Error("routes must not be empty")
	}
}