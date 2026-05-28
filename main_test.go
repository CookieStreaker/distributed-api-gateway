package main

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRateLimiter(t *testing.T) {
	// 1. Connect to the local testing Redis container
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	
	// Clear out any old rate limit tracking data before running the test
	rdb.Del(ctx, "rate_limit:test_user")

	// 2. Instantiate our bucket logic
	bucket := &RedisTokenBucket{
		client:     rdb,
		Capacity:   5,
		RefillRate: 1,
	}

	// 3. Simulate hitting the endpoint 6 times in a row instantly
	for i := 1; i <= 6; i++ {
		allowed, err := bucket.Allow("test_user")
		if err != nil {
			t.Fatalf("Redis error during testing: %v", err)
		}

		if i <= 5 {
			// The first 5 requests SHOULD be allowed
			if !allowed {
				t.Errorf("Request %d was blocked, but it should have been allowed", i)
			}
		} else {
			// The 6th request SHOULD be blocked (429 status match)
			if allowed {
				t.Errorf("Request %d was allowed, but it should have been rate limited!", i)
			}
		}
	}
}