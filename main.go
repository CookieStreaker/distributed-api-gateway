package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// 1. The Atomic Lua Script
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

local delta = math.max(0, now - last_updated)
local tokens_to_add = delta * refill_rate
tokens = math.min(capacity, tokens + tokens_to_add)
last_updated = now

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    return 1 
else
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    return 0 
end
`

// 2. The Distributed Gateway Token Bucket Struct
type RedisTokenBucket struct {
	client     *redis.Client
	Capacity   int
	RefillRate int
}

// 3. The Distributed Allow Method
func (tb *RedisTokenBucket) Allow(userID string) (bool, error) {
	now := time.Now().Unix() 

	keys := []string{"rate_limit:" + userID}
	args := []interface{}{tb.Capacity, tb.RefillRate, now}

	result, err := tb.client.Eval(ctx, luaScript, keys, args...).Result()
	if err != nil {
		return false, err
	}

	return result.(int64) == 1, nil
}

func main() {
	// Initialize Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	// Test the connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	bucket := &RedisTokenBucket{
		client:     rdb,
		Capacity:   5,
		RefillRate: 1,
	}

	// Define target URLs for our backend services
	userURL, _ := url.Parse("http://127.0.0.1:8081")
	productURL, _ := url.Parse("http://127.0.0.1:8082")

	// Create reverse proxies
	userProxy := httputil.NewSingleHostReverseProxy(userURL)
	productProxy := httputil.NewSingleHostReverseProxy(productURL)

	// The main handler for our Gateway
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get("X-User-Id")
		if userID == "" {
			userID = "anonymous"
		}

		allowed, err := bucket.Allow(userID)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		if !allowed {
			http.Error(w, "Too Many Requests - Rate Limit Exceeded", http.StatusTooManyRequests)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/users") {
			userProxy.ServeHTTP(w, r)
			return
		} else if strings.HasPrefix(r.URL.Path, "/products") {
			productProxy.ServeHTTP(w, r)
			return
		}

		http.Error(w, "Resource Not Found", http.StatusNotFound)
	})

	fmt.Println("API Gateway is running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
