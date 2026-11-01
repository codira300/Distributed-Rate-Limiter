# Distributed Rate Limiter

Three stateless application servers, one Redis instance, one exact global quota.
This is a C++20 rate limiter built around atomic Redis Lua scripts, with two
algorithms behind one interface:

- **Token bucket** with burst capacity and fractional refill.
- **Sliding window log** with exact per-window semantics.

The hot path is one `EVALSHA` round trip per check. Redis reads its own clock,
refills/counts, consumes, writes state, and refreshes the key TTL inside one
script. There is no check-then-act window between application servers.

This is a production-oriented reference implementation, not a claim of
production readiness. The measured limits, failure modes, and remaining gaps are
documented below.

---

## Results at a glance

| Signal | Result |
|---|---|
| Exact global limit, 3 independent servers | **100 / 240** token-bucket checks allowed; **50 / 240** sliding-window checks allowed |
| Exact global limit, 4 servers behind a load balancer | **100 / 160** token-bucket requests; **100 / 128** sliding-window requests |
| Peak measured throughput, token bucket | **11,939 checks/s** (1 server, 2 threads) |
| Peak measured throughput, sliding window | **8,131 checks/s** (1 server, 2 threads) |
| Redis-side Lua execution | **40.62 µs/call** token bucket; **52.53 µs/call** sliding window |
| State size | **150 B** token bucket key; **~106 B/request** sliding-window log |
| Test suite | **4/4 CTest targets pass** in Debug and Release |

Measured on Windows loopback with Redis 5.0.14.1, a Release build, and a shared
key. These are representative numbers, not a controlled production benchmark.
The full methodology and caveats are in
[`docs/ENGINEERING_DESIGN.md`](docs/ENGINEERING_DESIGN.md).

---

## What this demonstrates

- **Distributed correctness:** the global limit is enforced by Redis, not by
  per-process maps, so adding app servers does not multiply the quota.
- **Concurrency reasoning:** every check is one atomic script; tests fire
  synchronized bursts from 12, 32, and 40 threads and assert exact allow counts.
- **Performance analysis:** throughput and latency are measured, the bottleneck
  is identified (Redis serializes same-key scripts), and scaling options are
  evaluated rather than assumed.
- **Failure design:** fail-closed is the default; fail-open is explicit,
  observable, and marked in responses.
- **Operational maturity:** health endpoints, Prometheus metrics, Redis pool
  stats, TTLs, connection failure cooldown, and `NOSCRIPT` recovery.
- **Tradeoff literacy:** token bucket versus sliding window, hiredis versus
  redis-plus-plus, atomic round trip versus prefetch, single Redis versus
  cluster.

---

## Architecture

```text
             +-----------+   +-----------+   +-----------+
 clients --> |  app-1    |   |  app-2    |   |  app-3    |
             | stateless |   | stateless |   | stateless |
             | pool: 16  |   | pool: 16  |   | pool: 16  |
             +-----+-----+   +-----+-----+   +-----+-----+
                   |               |               |
                   +---------------+---------------+
                                   |
                            one EVALSHA/check
                                   |
                            +------+------+
                            |    Redis    |
                            | Lua scripts |
                            | source of   |
                            | truth       |
                            +-------------+
```

Request flow:

1. HTTP server validates `client_id`, `policy_id`, and `cost`.
2. Service resolves the policy and leases a Redis connection.
3. One Lua script atomically reads Redis `TIME`, updates state, consumes, and
   sets a TTL.
4. The decision is returned as JSON plus `X-RateLimit-*` headers.
5. Redis errors are handled by the configured failure policy.

No application server stores authoritative limiter state. A server can restart
without changing the global quota.

---

## Algorithms and Redis schema

### Token bucket

```text
rl:v1:tb:{policy_id:client_id}  -> HASH
  tokens_u : integer microtokens (1 token = 1,000,000)
  last_ms  : Redis server time in milliseconds
```

The script:

1. Reads Redis `TIME`.
2. Initializes a missing bucket to full capacity.
3. Refills with `elapsed * rate`, capped at capacity.
4. Carries fractional refill forward instead of discarding it.
5. Consumes the cost atomically.
6. Returns `allowed`, `limit`, `remaining`, `retry_after`, `reset_after`.
7. Sets a TTL: idle TTL when full, otherwise time-to-full plus 1 second.

**Best for:** burst-tolerant APIs, user-facing quotas, high-cardinality keys.
**State:** O(1). **Measured:** 150 B for a live key.

### Sliding window log

```text
rl:v1:sw:{policy_id:client_id}:log  -> ZSET
  score  = Redis time in milliseconds
  member = "<milliseconds>:<sequence>"

rl:v1:sw:{policy_id:client_id}:seq  -> STRING
  monotonic sequence counter
```

The script:

1. Reads Redis `TIME`.
2. Removes entries outside `[now - window, now]` with `ZREMRANGEBYSCORE`.
3. Counts the remaining units with `ZCARD`.
4. If `count + cost <= limit`, inserts `cost` unique members and allows.
5. On denial, finds the entry whose expiry unblocks the request and returns an
   exact `retry_after`.
6. Refreshes both key TTLs.

Both keys share the `{policy:client}` hash tag, so the script is slot-safe for
Redis Cluster. The token-bucket script touches exactly one key.

**Best for:** strict "N per window" contracts, billing/security limits, cases
where boundary bursts are unacceptable.
**State:** O(requests in window). **Measured:** ~106 B per request unit.

### Complexity and TTL summary

| Property | Token bucket | Sliding window log |
|---|---|---|
| Memory per key | O(1) | O(requests in window) |
| Redis network operations | 1 `EVALSHA` | 1 `EVALSHA` |
| Internal Redis commands | `TIME`, `HMGET`, `HSET`, `PEXPIRE` | `TIME`, `ZREMRANGEBYSCORE`, `ZCARD`, `INCRBY`, `ZADD`×cost, optional `ZRANGE`, `PEXPIRE`×2 |
| Burst tolerance | Up to `capacity` | None beyond the window |
| Clock source | Redis `TIME` | Redis `TIME` |
| TTL | idle when full; deficit-based otherwise | `window + 1 s`; sequence key `window + 61 s` |
| `cost > 1` | One subtraction | `cost` zset members, bounded by `max_cost` |

TTLs are written in the same script as the state mutation. Without them, every
unique client/policy pair would remain in Redis forever.

---

## Correctness properties

- **No check-then-act race:** read, refill/count, consume, write, and expire all
  happen in one Redis script.
- **No app-clock dependence:** Lua uses Redis `TIME`, so skew between app servers
  does not change decisions.
- **No floating-point drift:** token bucket state is integer microtokens.
- **Exact weighted cost:** `cost > 1` is handled atomically in both algorithms.
- **Safe expiry:** a partially drained bucket cannot expire early and reset to
  full; the TTL is derived from the current deficit.
- **Slot safety:** sliding-window keys share a hash tag; token bucket uses one key.
- **Recoverable scripts:** `EVALSHA` with `NOSCRIPT` falls back to `EVAL` and
  reloads the script after a Redis restart.
- **Bounded failure:** a broken connection triggers a 500 ms cooldown instead of
  a reconnect storm.

### Failure policy

| Mode | Behavior | When to use |
|---|---|---|
| **Fail-closed** (default) | `503`, `retryable=true`, no invented decision | Protecting a finite downstream; availability is secondary |
| **Fail-open** (`--fail-open`) | Allow with `decision_status=fail_open`, `X-RateLimit-Fallback: fail-open` | Low-risk/read-only paths where availability dominates |

The default is fail-closed because "allow everything when the guard is blind" is
the more dangerous failure for most rate-limited systems. Fail-open is explicit
and visible in headers, logs, health, and metrics.

---

## Measured performance

### Methodology

- Windows development host, MSYS2 GCC 16.2.0, Release build.
- Redis 5.0.14.1 (Windows port) on loopback.
- `rl-redis-bench`, 5-second runs, one shared key.
- Capacity/limit large enough that all checks are allowed; this isolates limiter
  cost from `429` handling.
- Latency includes pool acquisition, Redis round trip, Lua execution, and reply
  parsing.
- Connections were warm; the host was not CPU-pinned.
- The same Redis served all servers, which is both the intended architecture and
  the throughput ceiling.

### Throughput and latency

**Token bucket:**

| Servers | Threads/server | Checks/s | p50 µs | p95 µs | p99 µs |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 5,678 | 153 | 272 | 765 |
| 1 | 2 | **11,939** | 132 | 294 | 619 |
| 1 | 3 | 9,759 | 239 | 606 | 1,181 |
| 3 | 1 | 8,630 | 294 | 687 | 1,291 |
| 3 | 2 | 9,895 | 469 | 1,292 | 2,365 |
| 3 | 3 | 9,877 | 744 | 1,993 | 3,095 |

**Sliding window log:**

| Servers | Threads/server | Checks/s | p50 µs | p95 µs | p99 µs |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 5,088 | 173 | 303 | 801 |
| 1 | 2 | **8,131** | 226 | 409 | 890 |
| 1 | 3 | 5,890 | 364 | 1,007 | 2,152 |
| 3 | 1 | 6,776 | 346 | 859 | 1,744 |
| 3 | 2 | 5,892 | 789 | 2,339 | 4,391 |
| 3 | 3 | 6,005 | 1,199 | 3,156 | 5,274 |

### Redis-side cost

| Algorithm | Command | Calls | µs/call |
|---|---|---:|---:|
| Token bucket | `EVALSHA` | 44,856 | 40.62 |
| Token bucket | `HMGET` | 44,856 | 1.52 |
| Token bucket | `PEXPIRE` | 44,856 | 0.95 |
| Token bucket | `TIME` | 44,856 | 0.90 |
| Sliding window | `EVALSHA` | 42,042 | 52.53 |
| Sliding window | `ZREMRANGEBYSCORE` | 42,042 | 1.57 |
| Sliding window | `ZADD` | 42,042 | 6.10 |
| Sliding window | `ZCARD` | 42,042 | 0.57 |
| Sliding window | `PEXPIRE` | 84,084 | 0.77 |
| Sliding window | `TIME` | 42,042 | 0.74 |

### Memory

| Algorithm | Contents | Bytes |
|---|---|---:|
| Token bucket | hash with `tokens_u`, `last_ms` | 150 |
| Sliding window | 42,042 zset members | 4,462,432 |
| Sliding window | sequence string | 106 |

Sliding-window memory is about **106 bytes per request unit**. At 1,000 requests
per window and one million active keys, that is roughly **106 GB** of Redis
memory. This is why the sliding-window log is a strict-quota tool, not a default
for every endpoint.

### Interpretation

- Throughput improves from one to two threads, then plateaus or degrades.
- Redis executes one script at a time per shard; all checks use the same key, so
  more app threads add queueing rather than parallelism.
- Three servers do **not** multiply throughput; they share one Redis
  serialization point.
- The practical scaling path is sharding keys across Redis instances or Redis
  Cluster, not adding application threads.
- At 1 ms RTT, one connection is limited to roughly 1,000 checks/s; a
  16-connection pool has a theoretical ceiling of roughly 16,000 checks/s per
  server and 48,000/s across three servers, if Redis can keep up.
- At 0.5 ms RTT, the corresponding figures are roughly 2,000/s per connection
  and 32,000/s per server.

The HTTP path adds JSON parsing, `httplib` request handling, and response
serialization. A Debug smoke run measured roughly 1.5–1.9k requests/s with 4–8
client threads on one server; those numbers are not comparable to the Release
library benchmark and are not used to claim limiter throughput.

---

## Build

Requirements:

- CMake 3.24+
- C++20 compiler (tested with MSYS2 GCC 16.2.0 on Windows)
- hiredis 1.3+ and pkg-config, or `HIREDIS_INCLUDE_DIR` / `HIREDIS_LIBRARY`
- Redis 5+
- nlohmann/json and cpp-httplib are vendored under `third_party/`

```sh
cmake --preset release
cmake --build --preset release
```

Equivalent without presets:

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

---

## Run

Start Redis, then run the three-server demonstration:

```sh
./build/release/rl-demo --redis-host 127.0.0.1 --redis-port 6379
```

Expected verification:

```text
token bucket: global allowed=100 denied=140
sliding window: global allowed=50 denied=190
demo passed: three independent servers enforced one global limit
```

Run one application server:

```sh
./build/release/rl-server \
  --instance-id app-1 \
  --bind 0.0.0.0 --port 8081 \
  --policies config/policies.json \
  --key-prefix rl:v1 \
  --redis-host 127.0.0.1 --redis-port 6379 \
  --redis-pool-size 16 \
  --fail-closed
```

Check a request:

```sh
curl -s -X POST http://127.0.0.1:8081/api/v1/check \
  -H 'Content-Type: application/json' \
  -d '{"client_id":"alice","policy_id":"api-standard","cost":1}'
```

Endpoints:

- `POST /api/v1/check`
- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`

Response headers:

- `X-RateLimit-Limit`
- `X-RateLimit-Remaining`
- `X-RateLimit-Reset-Ms`
- `X-RateLimit-Algorithm`
- `X-RateLimit-Policy`
- `Retry-After` on `429`
- `X-RateLimit-Fallback: fail-open` when applicable
- `X-RateLimit-Failure-Policy` on dependency failure

The optional round-robin proxy is `rl-lb`. See `tests/cluster_test.cpp` for a
four-server example. If the proxy uses persistent connections, backend
`--http-threads` must be at least the proxy worker count, or persistent
connections can occupy every backend worker and cause head-of-line stalls.

---

## Tests

With Redis on `127.0.0.1:6379`:

```sh
ctest --test-dir build/release --output-on-failure
```

Current result: **4/4 tests pass in Debug and Release**, including:

| Target | What it proves |
|---|---|
| `rl_tests` | Burst capacity, refill, weighted cost, TTL, validation, policy loading, Redis stats, and exact concurrent allow counts from 32/40 threads |
| `rl_cluster_test` | 4 independent server processes behind a load balancer share one global quota; round-robin distribution and metrics |
| `rl_failure_test` | Fail-closed returns `503`; fail-open allows and marks the decision |
| `rl_demo` | 3 independent `RedisClient` instances enforce exactly 100/240 token-bucket and 50/240 sliding-window checks |

Measured CTest wall time: about **4.5 s Release**, **4.8 s Debug** on the
development host.

---

## Benchmarks

Direct Redis path, which is the limiter overhead that matters:

```sh
./build/release/rl-redis-bench \
  --algorithm tb --servers 3 --threads-per-server 3 --duration 5

./build/release/rl-redis-bench \
  --algorithm sw --servers 3 --threads-per-server 3 --duration 5
```

HTTP path through one server:

```sh
./build/release/rl-bench \
  --port 8081 --policy tb-burst-100 --threads 8 --duration 10
```

Redis command statistics:

```sh
redis-cli config resetstat
./build/release/rl-redis-bench --algorithm tb --servers 3 --threads-per-server 3 --duration 5
redis-cli info commandstats
```

---

## Design decisions

| Decision | Alternative | Why |
|---|---|---|
| Redis Lua atomic script | Read/compute/write in C++ | Removes the distributed check-then-act race |
| Redis `TIME` inside Lua | Application `now_ms` | App-server clock skew cannot change decisions |
| Integer microtokens | Floating-point tokens | Avoids drift and makes refill carry explicit |
| Exact sliding-window log | `INCR` + `EXPIRE` fixed window | Prevents boundary bursts and gives exact edge semantics |
| One atomic round trip | Local prefetch/batching | Preserves a hard global limit; prefetch has an `N*(B-1)` overshoot bound |
| hiredis + custom pool | redis-plus-plus | One single-key `EVALSHA` does not need pipelining; explicit reply/pool lifetime |
| Fail-closed default | Fail-open default | A blind guard should not silently allow all traffic |
| Single Redis baseline | Redis Cluster | Simpler correctness proof; scripts are already slot-safe, cluster client is future work |
| Deficit-based TTL | Fixed 24-hour TTL | A fixed TTL can reset a partially drained bucket to full |

The full rationale, prior-art attribution, and rejected alternatives are in
[`docs/ENGINEERING_DESIGN.md`](docs/ENGINEERING_DESIGN.md).

---

## Observability

The `/metrics` endpoint exposes:

- `rl_decisions_total`
- `rl_decision_latency_seconds`
- `rl_errors_total`
- `rl_http_requests_total`
- `rl_active_requests`
- `rl_redis_commands_total`
- `rl_redis_command_errors_total`
- `rl_redis_command_latency_seconds`
- `rl_redis_connections_created_total`
- `rl_redis_connection_failures_total`
- `rl_redis_pool_size`
- `rl_redis_pool_idle`
- `rl_redis_pool_in_use`
- `rl_redis_pool_acquire_timeouts_total`
- `rl_redis_noscript_recoveries_total`
- `rl_instance_info`
- `rl_uptime_seconds`
- `rl_lb_requests_total`
- `rl_lb_backend_requests_total`
- `rl_lb_backend_failures_total`

Health:

- `/health/live` — process is alive.
- `/health/ready` — Redis ping succeeds and the configured failure policy is
  reported.

---

## Repository layout

```text
include/ratelimiter/       Public library headers
src/algorithms/            Token bucket and sliding window limiters
src/redis/                 hiredis pool, RAII replies, EVALSHA/NOSCRIPT
src/service/               Policy registry and failure-policy boundary
src/transport/             HTTP server and optional round-robin proxy
src/config/                Policy/service configuration
src/observability/         Metrics and structured logging
scripts/*.lua              Atomic server-side algorithms
tests/                     Redis integration, cluster, and failure tests
demo/three_server_demo.cpp Three independent servers against one Redis key
benchmarks/                HTTP load generator and direct Redis benchmark
docs/ENGINEERING_DESIGN.md Prior art, design, measurements, limitations
```

---

## Limitations and next steps

- **Single Redis is a SPOF.** Sentinel/Cluster failover is future work; hiredis
  does not follow `MOVED`/`ASK` redirects.
- **Network partitions** are fail-closed by default. A reservation-based fallback
  that preserves the global limit is not implemented.
- **Redis Cluster** requires a cluster-aware client or proxy; current scripts are
  slot-safe but the client is not cluster-aware.
- **Redis clock steps:** backward steps are clamped; forward steps can add at most
  one bucket of tokens. App clocks are irrelevant.
- **Sliding-window memory** is O(requests in window); a weighted counter variant
  would reduce memory at the cost of boundary approximation.
- **No TLS/ACL support** in the Redis client; use a private network or add
  hiredis SSL.
- **No dynamic policy reload**; policies are loaded at startup and the registry
  is sealed for lock-free reads.
- **No async API or local prefetch**; every check pays one Redis round trip.
- **Proxy keep-alive caveat:** size backend HTTP threads above the proxy's
  persistent connection count.

The next engineering steps, in priority order: Redis Sentinel/Cluster support,
an opt-in prefetch layer with a quantified overshoot bound, async/pipelined
checks, dynamic policy reload, and a sliding-window counter variant for
memory-constrained deployments.
