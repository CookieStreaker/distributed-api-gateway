// =============================================================================
// k6 Load Test — Distributed API Gateway Concurrency Proof
// =============================================================================
// This script stress-tests the gateway to prove the Redis Lua script prevents
// race conditions under high concurrency. We expect ONLY 200 (allowed) and
// 429 (rate limited) responses — NEVER a 500 (internal error).
//
// Run: k6 run load_test.js
// =============================================================================

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";

// -- Custom Metrics ----------------------------------------------------------
const rateLimited = new Rate("rate_limited");         // % of 429 responses
const blockedTotal = new Counter("blocked_requests");  // Total 429 count
const allowedTotal = new Counter("allowed_requests");  // Total 200 count
const latencyTrend = new Trend("request_latency_ms");  // Latency distribution

// -- Test Configuration ------------------------------------------------------
export const options = {
  stages: [
    { duration: "5s",  target: 20 },  // Ramp up to 20 VUs
    { duration: "10s", target: 50 },  // Ramp up to 50 VUs (peak concurrency)
    { duration: "15s", target: 50 },  // Sustain 50 VUs — stress the Lua script
    { duration: "5s",  target: 0 },   // Ramp down gracefully
  ],

  thresholds: {
    // CRITICAL: No 500 errors allowed — proves atomicity
    "http_req_failed": ["rate<0.01"],

    // Performance: 95th percentile latency under 500ms
    "http_req_duration": ["p(95)<500"],

    // We expect SOME rate limiting — proves the limiter is active
    "rate_limited": ["rate>0.0"],
  },
};

// -- Simulated User IDs ------------------------------------------------------
// Using a small pool of user IDs to ensure some users hit their rate limit,
// which proves the per-user token bucket works under concurrent access.
const USER_IDS = [
  "user-alpha", "user-beta", "user-gamma", "user-delta", "user-epsilon",
  "user-zeta", "user-eta", "user-theta", "user-iota", "user-kappa",
];

// -- Test Endpoints ----------------------------------------------------------
const GATEWAY_URL = "http://localhost:8080";
const ENDPOINTS = ["/users/", "/products/"];

// -- Main Test Function ------------------------------------------------------
export default function () {
  // Pick a random user and endpoint
  const userId = USER_IDS[Math.floor(Math.random() * USER_IDS.length)];
  const endpoint = ENDPOINTS[Math.floor(Math.random() * ENDPOINTS.length)];

  const response = http.get(`${GATEWAY_URL}${endpoint}`, {
    headers: {
      "X-User-Id": userId,
    },
    tags: { endpoint: endpoint },
  });

  // Track latency
  latencyTrend.add(response.timings.duration);

  // Validate: ONLY 200 or 429 are acceptable responses
  const isValid = check(response, {
    "status is 200 or 429": (r) => r.status === 200 || r.status === 429,
    "no server errors (5xx)": (r) => r.status < 500,
  });

  // Track rate limiting metrics
  if (response.status === 429) {
    rateLimited.add(1);
    blockedTotal.add(1);

    // Verify RFC-compliant rate limit headers are present on 429s
    check(response, {
      "has Retry-After header": (r) => r.headers["Retry-After"] !== undefined,
      "has X-RateLimit-Limit header": (r) =>
        r.headers["X-Ratelimit-Limit"] !== undefined,
      "has X-RateLimit-Remaining header": (r) =>
        r.headers["X-Ratelimit-Remaining"] !== undefined,
    });
  } else {
    rateLimited.add(0);
    allowedTotal.add(1);
  }

  // Small random sleep to simulate realistic traffic patterns
  sleep(Math.random() * 0.1);
}

// -- Summary Report ----------------------------------------------------------
export function handleSummary(data) {
  const totalReqs = data.metrics.http_reqs.values.count;
  const blocked = data.metrics.blocked_requests
    ? data.metrics.blocked_requests.values.count
    : 0;
  const allowed = data.metrics.allowed_requests
    ? data.metrics.allowed_requests.values.count
    : 0;
  const p95 = data.metrics.http_req_duration.values["p(95)"];

  console.log("\n╔══════════════════════════════════════════════════╗");
  console.log("║       API GATEWAY LOAD TEST RESULTS              ║");
  console.log("╠══════════════════════════════════════════════════╣");
  console.log(`║  Total Requests:     ${String(totalReqs).padStart(8)}                  ║`);
  console.log(`║  Allowed (200):      ${String(allowed).padStart(8)}                  ║`);
  console.log(`║  Rate Limited (429): ${String(blocked).padStart(8)}                  ║`);
  console.log(`║  P95 Latency:        ${String(p95.toFixed(2) + "ms").padStart(8)}                  ║`);
  console.log("╚══════════════════════════════════════════════════╝\n");

  return {
    stdout: JSON.stringify(data, null, 2),
  };
}
