import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";

const rateLimited = new Rate("rate_limited");
const blockedTotal = new Counter("blocked_requests");
const allowedTotal = new Counter("allowed_requests");
const latencyTrend = new Trend("request_latency_ms");

export const options = {
  stages: [
    { duration: "5s", target: 20 },
    { duration: "10s", target: 50 },
    { duration: "15s", target: 50 },
    { duration: "5s", target: 0 },
  ],
  thresholds: {
    "http_req_failed": ["rate<0.01"],
    "http_req_duration": ["p(95)<500"],
    "rate_limited": ["rate>0.0"],
  },
};

const USER_IDS = [
  "user-alpha", "user-beta", "user-gamma", "user-delta", "user-epsilon",
  "user-zeta", "user-eta", "user-theta", "user-iota", "user-kappa",
];

const GATEWAY_URL = "http://localhost:8080";
const ENDPOINTS = ["/users/", "/products/"];

export default function () {
  const userId = USER_IDS[Math.floor(Math.random() * USER_IDS.length)];
  const endpoint = ENDPOINTS[Math.floor(Math.random() * ENDPOINTS.length)];

  const res = http.get(`${GATEWAY_URL}${endpoint}`, {
    headers: { "X-User-Id": userId },
    tags: { endpoint: endpoint },
  });

  latencyTrend.add(res.timings.duration);

  check(res, {
    "status is 200 or 429": (r) => r.status === 200 || r.status === 429,
    "no server errors (5xx)": (r) => r.status < 500,
  });

  if (res.status === 429) {
    rateLimited.add(1);
    blockedTotal.add(1);

    check(res, {
      "has Retry-After header": (r) => r.headers["Retry-After"] !== undefined,
      "has X-RateLimit-Limit header": (r) => r.headers["X-Ratelimit-Limit"] !== undefined,
      "has X-RateLimit-Remaining header": (r) => r.headers["X-Ratelimit-Remaining"] !== undefined,
    });
  } else {
    rateLimited.add(0);
    allowedTotal.add(1);
  }

  sleep(Math.random() * 0.1);
}

export function handleSummary(data) {
  const totalReqs = data.metrics.http_reqs.values.count;
  const blocked = data.metrics.blocked_requests ? data.metrics.blocked_requests.values.count : 0;
  const allowed = data.metrics.allowed_requests ? data.metrics.allowed_requests.values.count : 0;
  const p95 = data.metrics.http_req_duration.values["p(95)"];

  console.log("\n--- Load Test Summary ---");
  console.log(`Total Requests:  ${totalReqs}`);
  console.log(`Allowed (200):   ${allowed}`);
  console.log(`Throttled (429): ${blocked}`);
  console.log(`P95 Latency:     ${p95.toFixed(2)}ms\n`);

  return {
    stdout: JSON.stringify(data, null, 2),
  };
}
