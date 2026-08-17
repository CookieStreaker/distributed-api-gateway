package main

import (
	"context"
	"testing"
	"time"

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

func TestRateLimiter(t *testing.T) {
	ctx := context.Background()

	// 1. Connect to the local testing Redis container
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	// Verify Redis is reachable
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis is not running on 127.0.0.1:6379 — start it with: docker run -d -p 6379:6379 redis:alpine\nError: %v", err)
	}

	// Clean slate: remove any leftover test data
	rdb.Del(ctx, "rate_limit:test_user")

	// 2. Create a rate limiter with capacity=5, refill=1/sec
	bucket := &RedisTokenBucket{
		client:     rdb,
		Capacity:   5,
		RefillRate: 1,
	}

	// 3. Fire 6 requests instantly — first 5 should pass, 6th should be blocked
	for i := 1; i <= 6; i++ {
		result, err := bucket.Allow(ctx, "test_user")
		if err != nil {
			t.Fatalf("Request %d: unexpected Redis error: %v", i, err)
		}

		if i <= 5 {
			// Requests 1-5: SHOULD be allowed
			if !result.Allowed {
				t.Errorf("Request %d: was BLOCKED but should have been ALLOWED (remaining: %d)",
					i, result.Remaining)
			}
			// Remaining tokens should decrement: 4, 3, 2, 1, 0
			expectedRemaining := 5 - i
			if result.Remaining != expectedRemaining {
				t.Errorf("Request %d: remaining=%d, want=%d",
					i, result.Remaining, expectedRemaining)
			}
		} else {
			// Request 6: SHOULD be blocked (bucket empty)
			if result.Allowed {
				t.Errorf("Request %d: was ALLOWED but should have been RATE LIMITED", i)
			}
			if result.Remaining != 0 {
				t.Errorf("Request %d: remaining=%d, want=0", i, result.Remaining)
			}
			// ResetAt should be in the future
			if result.ResetAt <= time.Now().Unix() {
				t.Errorf("Request %d: reset_at=%d should be in the future", i, result.ResetAt)
			}
		}
	}
}

// TestRateLimiterRefill verifies that tokens refill over time.
func TestRateLimiterRefill(t *testing.T) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis not available: %v", err)
	}

	// Clean slate
	rdb.Del(ctx, "rate_limit:refill_test_user")

	bucket := &RedisTokenBucket{
		client:     rdb,
		Capacity:   2,
		RefillRate: 2, // 2 tokens per second
	}

	// Exhaust all tokens
	for i := 0; i < 2; i++ {
		result, err := bucket.Allow(ctx, "refill_test_user")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !result.Allowed {
			t.Fatalf("Request %d should have been allowed", i+1)
		}
	}

	// Verify bucket is empty
	result, err := bucket.Allow(ctx, "refill_test_user")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Allowed {
		t.Fatal("Request should have been blocked (bucket empty)")
	}

	// Wait for tokens to refill (1 second = 2 tokens at refill_rate=2)
	time.Sleep(1100 * time.Millisecond) // Slightly over 1 second for safety

	// Should be allowed again after refill
	result, err = bucket.Allow(ctx, "refill_test_user")
	if err != nil {
		t.Fatalf("Unexpected error after refill: %v", err)
	}
	if !result.Allowed {
		t.Error("Request should have been allowed after token refill")
	}
}

// TestConfigLoader validates the YAML configuration parser.
func TestConfigLoader(t *testing.T) {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config.yaml: %v", err)
	}

	// Validate server config
	if cfg.Server.Port == 0 {
		t.Error("Server port should not be 0")
	}
	if cfg.Server.AdminPort == 0 {
		t.Error("Admin port should not be 0")
	}

	// Validate rate limit
	if cfg.RateLimit.Capacity <= 0 {
		t.Error("Default rate limit capacity must be positive")
	}
	if cfg.RateLimit.RefillRate <= 0 {
		t.Error("Default rate limit refill rate must be positive")
	}

	// Validate routes
	if len(cfg.Routes) == 0 {
		t.Error("At least one route must be configured")
	}

	for _, route := range cfg.Routes {
		if route.PathPrefix == "" {
			t.Error("Route path_prefix cannot be empty")
		}
		if route.UpstreamURL == "" {
			t.Error("Route upstream_url cannot be empty")
		}
	}
}