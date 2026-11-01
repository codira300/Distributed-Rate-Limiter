# Distributed Rate Limiter — Engineering Design Document

This document is the narrative deliverable for the implementation in this
repository. It records the prior art that shaped the design, the algorithm and
coordination decisions, the implementation choices, and the measured behavior.
The code is the normative artifact; this document explains why it looks the way
it does.

---

## 1. Prior Art Synthesis (Phase 1)

### 1.1 `Deepak-2413/distributed-rate-limiter`

This project implements Sliding Window and Token Bucket behind a common
`RateLimiterInterface`, with configuration-driven policies and a multithreaded
simulation. Its state is process-local and mutex-protected; there is no Redis
coordination. **Borrowed:** the interface boundary and the concurrency-test idea.
**Rejected:** local state as the source of truth, because three servers with
three maps enforce three independent limits, not one global limit.

### 1.2 `makr-code/ThemisDB`, `src/server/rate_limiter_v2.cpp`

A production-oriented file that combines hiredis, a Lua check-and-consume token
bucket, local buckets as a fallback, and priority lanes. The Lua script refills
from `tokens`/`last_ms`, consumes, writes both fields, and sets a TTL; the C++
side mutexes one connection and passes an application `now_ms`. **Borrowed:** the
atomic check-and-consume shape, `SCRIPT LOAD`/`EVALSHA`, refill-before-consume,
and priority lanes as future work. **Rejected:** app-supplied time (clock skew)
and one mutexed connection (throughput bottleneck); this implementation uses
Redis `TIME` and a pool. Its implicit local fallback is also rejected as a
default because it admits traffic the global limiter would deny; here fallback
is explicit and observable.

### 1.3 `mfycheng/ratelimiter`

A thread-safe C++ token-bucket library with blocking `acquire()`/`try_acquire()`.
**Borrowed:** the refill/consume state machine and thread-safe object design,
strengthened here with integer microtokens instead of floating permits.
**Rejected:** blocking acquisition, because a distributed API limiter should
return a decision and `retry_after`, not pin an application thread.

### 1.4 `harshal24-chavan/RateLimiter-CPP`

A C++20 gRPC + Redis project reporting 180k+ checks/s via L1/L2 tiers, lock-free
SPSC queues, sharded hot-path caching, and 50 ms aggregated Redis pipelines. The
lesson is that the headline comes from batching and local aggregation, not one
atomic round trip per check. **Borrowed:** sharding and hot-path caching as
future optimizations. **Rejected:** batching as the baseline, because a 50 ms
aggregation window permits traffic beyond the global limit; the hard-limit
requirement needs one atomic script per check.

### 1.5 `uppnrise/distributed-rate-limiter`

A Redis-backed service with Token Bucket, Sliding/Fixed Window, Leaky Bucket,
and Composite algorithms. Its token-bucket script uses a Redis hash,
`HMGET`/`HMSET`, and `EXPIRE`, passes application `current_time`, uses a fixed
24-hour TTL, and defaults to fail-open. **Borrowed:** the hash schema, atomic
Lua pattern, per-check TTL refresh, and namespace discipline. **Rejected:** app
timestamps, a fixed TTL unrelated to the policy (which can reset a partially
drained bucket to full), and fail-open as the default. Here TTL is derived from
the refill deficit, Redis `TIME` is authoritative, and fail-closed is the
default.

### 1.6 Sliding-window Lua patterns and Stripe/AWS token buckets

Go/Python services commonly implement sliding windows with one Lua script doing
`ZREMRANGEBYSCORE`, `ZCARD`, and `ZADD`; that is the exact-log pattern used here.
It removes the count/insert race and gives true edge-of-window behavior. The
alternative `INCR` + `EXPIRE` fixed window was rejected because a client can
spend the full quota at the end of one window and again at the start of the
next. Stripe/AWS-style token buckets supply the burst model: capacity is the
burst budget, refill is the long-term rate, and state is O(1). The microtoken
representation here avoids floating-point drift.

### 1.7 Client libraries: hiredis versus redis-plus-plus

`redis-plus-plus` offers RAII connections, a built-in pool, type-safe replies,
and pipelining; `hiredis` is the lower-level C client with manual reply lifetime
and no pool. This implementation chose **hiredis** because the hot path is a
single single-key `EVALSHA` that does not benefit from pipelining. The control is
used for a bounded pool, RAII reply ownership, a script-SHA cache, `NOSCRIPT`
recovery, a failure cooldown, and explicit stats. The cost is code to review; the
benefit is a small, predictable path to `redisCommandArgv`. The often-quoted
10–20% RAII overhead is plausible but was **not** measured here.

---

## 2. Architecture and Algorithm Design (Phase 2)

### 2.1 Common interface

```cpp
struct CheckRequest {
    std::string client_id;
    std::uint32_t cost_tokens{0};   // 0 selects the policy default
};

class RateLimiter {
public:
    virtual ~RateLimiter() = default;
    virtual const Policy& policy() const noexcept = 0;
    virtual Result<Decision> check(const CheckRequest& request) = 0;
};
```

`Decision` carries `allowed`, `limit`, `remaining`, `retry_after`,
`reset_after`, `algorithm`, `policy_id`, and a status
(`allowed`, `rate_limited`, or `fail_open`). Errors are returned as
`Result<T>` with an `ErrorCode`; dependency errors are classified separately so
the service layer can apply the fail-open/fail-closed policy.

The limiter object is immutable after construction: policy, key prefix, and a
reference to a shared `RedisClient`. There is no mutable per-key state in C++.
All concurrency is in the Redis client pool and in Redis itself.

### 2.2 Token bucket

State per key:

```
rl:v1:tb:{policy_id:client_id}  ->  HASH
    tokens_u  : integer microtokens (1 token = 1,000,000)
    last_ms   : Redis server time in milliseconds at the last refill/consume
```

On each check, one Lua script:

1. Reads `TIME` from Redis.
2. Reads `tokens_u` and `last_ms` with `HMGET`.
3. Initializes a new bucket to `capacity` at `now_ms`.
4. Computes `elapsed = max(0, now_ms - last_ms)`.
5. Adds `floor(elapsed * rate_microtokens_per_second / 1000)` tokens, capped at
   capacity. If the bucket is not full, `last_ms` is advanced only by the
   millisecond value actually converted into tokens, so fractional refill is
   carried forward instead of being discarded.
6. If `tokens >= cost`, subtracts the cost and allows; otherwise computes
   `retry_after = ceil((cost - tokens) * 1000 / rate)`.
7. Writes the new state with `HSET` and refreshes the TTL with `PEXPIRE`.
8. Returns `{allowed, limit, remaining, retry_after, reset_after, reserved}`.

The script is loaded with `SCRIPT LOAD` and invoked with `EVALSHA`; a
`NOSCRIPT` reply triggers `EVAL` and a reload, so a Redis restart does not
permanently break the limiter. Using Redis `TIME` means app-server clock skew is
irrelevant to the decision.

### 2.3 Sliding window log

State per key (two keys, same hash tag):

```
rl:v1:sw:{policy_id:client_id}:log  ->  ZSET
    score  = Redis time in milliseconds
    member = "<milliseconds>:<sequence>"   (unique even at the same millisecond)

rl:v1:sw:{policy_id:client_id}:seq  ->  STRING (monotonic sequence)
```

One Lua script:

1. Reads `TIME`.
2. `ZREMRANGEBYSCORE log -inf (now - window)` removes entries outside the
   window.
3. `ZCARD log` counts the remaining request units.
4. If `count + cost <= limit`, `INCRBY seq cost` and insert `cost` unique
   members at `now`; otherwise deny.
5. On denial, `ZRANGE` finds the entry that must expire before the request
   fits (the needed-th oldest) and returns the wait time as `retry_after`.
6. Refreshes both TTLs.

This is the exact sliding-window-log baseline. It is O(requests_per_window)
memory per key, versus O(1) for the token bucket. It is the right default when
"no more than N in any window of W" is a hard requirement and boundary bursts
are unacceptable. A sliding-window counter (two fixed windows plus weighted
interpolation) is the obvious memory-efficient alternative; it was not adopted
because it is approximate at the window boundary and the requirement explicitly
asks for exact semantics as the baseline.

### 2.4 Redis schema and TTL management

| Algorithm | Key | Type | Data | TTL |
|---|---|---|---|---|
| Token bucket | `rl:v1:tb:{policy:client}` | Hash | `tokens_u`, `last_ms` | full: `idle_ttl` (default 60 s); partial: `ceil((capacity-tokens)*1000/rate)+1000 ms` |
| Sliding window log | `rl:v1:sw:{policy:client}:log` | ZSet | score = ms, member = `ms:seq` | `window + 1000 ms` |
| Sliding window sequence | `rl:v1:sw:{policy:client}:seq` | String | monotonic counter | `window + 61000 ms` |

TTL is not optional. Without it, every unique client/policy pair ever seen would
remain in Redis forever; a public API with millions of client identifiers becomes
an unbounded memory leak. The TTLs are set inside the same Lua script as the
state mutation, so a key cannot be written without an expiry.

For the token bucket, the partial-bucket TTL is the time needed to refill the
current deficit plus a 1-second grace. Expiring earlier would reset a partially
drained bucket to full and allow an extra burst. When the bucket is full, the
state is equivalent to a fresh bucket, so the shorter idle TTL is safe. For the
sliding window, the log TTL is one window plus grace; after that, all members
are outside the window and the key has no semantic value. The sequence key has a
longer TTL (one extra minute) to avoid reusing a sequence while a delayed log
member could still exist.

### 2.5 Three-server coordination model

The three application servers are stateless with respect to rate-limiting state.
Each server owns a `RedisClient` with a bounded connection pool (default 16).
Every `check()` leases a connection, runs `EVALSHA`, parses the integer array,
and returns the lease. There is no local token cache in the baseline.

Alternatives considered:

1. **Pure Redis round trip per check (adopted).** One RTT and one atomic script.
   Highest consistency, highest latency. This is the only baseline that
   guarantees a hard global limit across all servers.
2. **Local token prefetch with periodic reconciliation (rejected for baseline).**
   Each server reserves a batch of `B` tokens from Redis and serves from local
   memory, reconciling every `T`. If `N` servers each hold up to `B-1` unspent
   local tokens, the worst-case overshoot above the global limit before
   reconciliation is `N * (B - 1)` requests. There is also a time-based window
   of up to `T` during which the servers' views differ. This can be made safe
   only by treating the local cache as a reservation that is deducted from the
   global quota, and even then the cluster can serve stale decisions during a
   partition. It is a legitimate optimization for latency-sensitive, bursty
   traffic; it is not the correct default for a hard global limit.
3. **Redis Cluster / sharded Redis (deferred).** A single Redis is a throughput
   and availability ceiling. Sharding scales both, but a Lua script that touches
   multiple keys must keep them in one slot. The token-bucket script touches one
   key and is slot-safe. The sliding-window script touches `:log` and `:seq`
   with the same hash tag `{policy:client}`, so it is also slot-safe. What is
   missing is a cluster-aware client: hiredis does not follow `MOVED`/`ASK`
   redirects. A production cluster deployment would use a cluster-aware client
   or a proxy and keep the same hash-tag discipline.

The cluster test (`tests/cluster_test.cpp`) runs four server processes behind a
round-robin proxy and verifies that a shared 100-token bucket allows exactly 100
of 160 requests, and a shared sliding window allows exactly 100 of 128. The
three-server demo (`rl-demo`) uses three independent `RedisClient` instances and
verifies the same invariant without the HTTP layer.

### 2.6 Failure policy

The limiter distinguishes validation errors from dependency errors
(`redis_unavailable`, `redis_timeout`, `redis_protocol`, `pool_exhausted`).
Dependency errors are handled by the service layer according to a configurable
policy:

- **Fail-closed (default):** return `503` with `retryable=true` and do not make
  a rate-limit decision. This protects downstream systems from a flood when the
  limiter is blind. The cost is that a Redis outage becomes an application
  outage.
- **Fail-open (`--fail-open` / `RL_FAIL_OPEN=1`):** allow the request with
  `decision_status=fail_open` and `X-RateLimit-Fallback: fail-open`. This keeps
  availability but silently permits traffic beyond the intended limit.

The default is fail-closed because the limiter is usually protecting a finite
downstream resource; "allow everything when the guard is down" is the more
dangerous failure for that use case. A read-only or low-risk endpoint can opt
into fail-open explicitly. The choice is visible in health responses, response
headers, logs, and metrics.

---

## 3. Implementation (Phase 3)

### 3.1 Source map

| Path | Responsibility |
|---|---|
| `include/ratelimiter/rate_limiter.hpp` | Common interface and registry |
| `include/ratelimiter/decision.hpp` | `Decision`, status, chrono fields |
| `include/ratelimiter/policy.hpp` | Policy variants and validation |
| `include/ratelimiter/redis_client.hpp` | Pooled hiredis client API |
| `src/redis/redis_client.cpp` | Pool, RAII replies, EVALSHA/NOSCRIPT, cooldown |
| `src/algorithms/token_bucket.cpp` | Token-bucket argument marshalling |
| `src/algorithms/sliding_window.cpp` | Sliding-window argument marshalling |
| `scripts/token_bucket.lua` | Atomic refill/consume |
| `scripts/sliding_window.lua` | Atomic trim/count/insert |
| `src/service/rate_limiter_service.cpp` | Fail-open/fail-closed boundary |
| `src/transport/http_server.cpp` | JSON API, headers, health, metrics |
| `src/transport/load_balancer.cpp` | Optional round-robin proxy |
| `demo/three_server_demo.cpp` | Three-server global-limit demonstration |
| `benchmarks/redis_bench.cpp` | Direct Redis throughput/latency benchmark |
| `tests/test_main.cpp` | Algorithm and concurrency integration tests |
| `tests/cluster_test.cpp` | Multi-process shared-limit test |
| `tests/failure_test.cpp` | Fail-open/fail-closed behavior |

### 3.2 hiredis pool and reply lifetime

`RedisClient::Impl` owns a `std::vector<std::unique_ptr<RedisConnection>>` of
idle connections, a `total` count, a condition variable, and a failure cooldown.
`acquire()` either pops an idle connection, creates one if below the pool size,
or waits up to `acquire_timeout`. A `Lease` object owns the connection and
returns it to the pool in its destructor; if the connection is marked unhealthy
(socket error), it is destroyed instead of returned.

Replies are wrapped in `std::unique_ptr<redisReply, ReplyDeleter>` so every
early return frees the reply. The client maintains a `script_name -> SHA` map
protected by a mutex; the SHA is loaded once per client process. `EVALSHA`
failure with `NOSCRIPT` falls back to `EVAL`, then reloads and updates the SHA.
A broken connection triggers a 500 ms failure cooldown so a dead Redis does not
turn every request into a fresh connect attempt.

### 3.3 Concurrency model

`check()` does not lock the limiter object. The limiter holds only immutable
configuration and a reference to the shared `RedisClient`. Concurrent checks
from many threads share the connection pool; each call leases one connection for
the duration of the script. The registry is populated at startup and sealed;
after sealing, lookups are read-only and safe for concurrent readers. Metrics
use atomics; the logger uses a mutex only for output serialization.

This is the intended contention model: the limiter object is not the
serialization point, and neither is a global mutex. The serialization point is
Redis, which executes scripts one at a time per shard. Adding app threads cannot
make Redis faster; it can only increase queueing and latency. The measurements
below show exactly that.

### 3.4 Build and tests

`CMakeLists.txt` builds:

- `rl_core` — algorithms, Redis client, policy, metrics, logger;
- `rl_service` — policy registry, service config, fail-policy boundary;
- `rl-server`, `rl-lb` — HTTP service and optional proxy;
- `rl-demo` — three-server demonstration;
- `rl_tests`, `rl_cluster_test`, `rl_failure_test`, `rl_demo` CTest entries;
- `rl-bench`, `rl-redis-bench` — HTTP and direct Redis benchmarks.

The tests require a real Redis instance (no mock), because the atomicity claim is
about Redis script execution. `tests/test_main.cpp` covers:

- burst at capacity for both algorithms;
- refill over time for token bucket;
- `cost > 1` and cost above `max_cost`;
- sliding-window edge behavior (deny at the limit, allow after the window);
- concurrent checks from 32–40 threads against one key, asserting exactly the
  configured number of allows;
- TTL presence and bounds;
- policy file loading and registry dispatch;
- Redis pool statistics.

`tests/cluster_test.cpp` starts four server processes and a proxy, checks shared
token-bucket and sliding-window quotas, verifies round-robin distribution, and
checks metrics endpoints. `tests/failure_test.cpp` starts servers pointed at a
dead Redis port and verifies `503` for fail-closed and `200` + fail-open header
for fail-open.

---

## 4. Engineering Design Document (Phase 4)

### 4.1 Tradeoff analysis

| Dimension | Token bucket | Sliding window log |
|---|---|---|
| Burst tolerance | High: up to `capacity` immediately, then smooth refill | None: at most `limit` in any window |
| Memory per key | O(1): two hash fields. Measured 150 B for a live key | O(requests in window). Measured 4,462,432 B for 42,042 entries, ~106 B/entry, plus 106 B for the sequence key |
| Redis network ops per check | 1 `EVALSHA` (internally `TIME`, `HMGET`, `HSET`, `PEXPIRE`) | 1 `EVALSHA` (internally `TIME`, `ZREMRANGEBYSCORE`, `ZCARD`, `INCRBY`, `ZADD`×cost, optional `ZRANGE`, `PEXPIRE`×2) |
| Accuracy under concurrency | Exact with Lua; no check-then-act window | Exact with Lua; no count/insert race |
| Clock sensitivity | Uses Redis `TIME`; app clocks irrelevant. Backward Redis clock steps are clamped; forward steps can add at most capacity | Uses Redis `TIME`; same residual risk. Scores are Redis-time based |
| Cost > 1 | One subtraction | `cost` zset members; bounded by `max_cost` (10,000) but a large cost can block Redis longer |
| Reset semantics | Continuous refill | Entry expiry; `retry_after` is exact for the entry whose expiry unblocks the request |
| Best fit | Burst-tolerant APIs, user-facing quotas | Strict "N per window" contracts, billing/security limits |

Client-library tradeoff:

| Dimension | hiredis (chosen) | redis-plus-plus |
|---|---|---|
| Abstraction | C API, manual reply lifetime | C++ wrapper, RAII, type-safe replies |
| Pool | Implemented here (bounded, condition variable, cooldown) | Built-in pool |
| Pipelining | Available but not used on the single-key path | First-class pipeline support |
| Per-call overhead | Lower; direct `redisCommandArgv` | Small RAII overhead; the claimed 10–20% is for pipelined workloads and was not measured here |
| Failure handling | Explicit; implemented here | More built-in behavior, less control |
| Choice reason | One `EVALSHA` per check; pool and script recovery are small enough to own | Better for complex multi-command or pipelined clients |

Per-check round-trip cost: on loopback, the measured p50 was roughly 130–240 µs
for one server at low concurrency, rising to roughly 0.7–1.2 ms with three
servers × three threads. Redis-side `EVALSHA` execution was around 41 µs (token
bucket) and 53 µs (sliding window) per call on the test host. Over a real network with 1 ms RTT, a single connection is limited to
about 1,000 checks/s by latency alone; a pool of 16 connections has a theoretical
ceiling of about 16,000 checks/s per server, and three servers about 48,000
checks/s, provided Redis itself can execute the scripts that fast. The measured
single-Redis ceiling on this host was about 10,000 token-bucket checks/s and
6,000 sliding-window checks/s for the same key, which is consistent with Redis
being the serialization point.

Fail-open/fail-closed: fail-closed is the default. The limiter is a guard; when
the guard cannot see, allowing all traffic is the more dangerous default for a
finite downstream. Fail-open is a deliberate availability trade, exposed as a
flag and marked in the response.

### 4.2 Design evolution and rationale

**From GET/SET to Lua.** The first mental model was "read the hash, compute in
C++, write the hash." With three servers, that is a textbook check-then-act
race: two servers can read the same token count and both allow. The prior-art
repos (ThemisDB, uppnrise) confirmed the fix: move the entire read-refill-consume-
write sequence into one Lua script. The C++ side now only marshals arguments and
parses the reply. This was the single most important design change.

**From fixed window to sliding-window log.** A fixed window with `INCR` and
`EXPIRE` is the cheapest possible limiter, and it was the initial candidate for
the "sliding window" requirement. It is not a sliding window: it permits a
boundary burst of up to `2 * limit` around a window edge. Since the requirement
explicitly calls for exact edge semantics, the implementation uses a sorted-set
log with `ZREMRANGEBYSCORE` + `ZCARD` + `ZADD` in one script. The cost is memory;
the benchmark quantified it at roughly 106 bytes per request unit, which is
acceptable for strict quotas and unacceptable for millions of high-volume keys.
That measurement is why the token bucket remains the default for high-cardinality
traffic.

**From floating tokens to microtokens.** Early token-bucket pseudocode used
`double` tokens and wall-clock seconds. Two problems appear in production: float
drift over many refills, and clock skew between app servers. The implementation
uses integer microtokens and Redis `TIME`. The Lua script advances `last_ms` only
by the milliseconds actually converted into tokens, so sub-token refill is not
lost. This is a small change with a large correctness payoff.

**Rejecting local prefetch as the baseline.** The attractive optimization is to
reserve a batch of tokens per server and serve most checks from memory. The
analysis is simple: with `N` servers and batch size `B`, the worst-case overshoot
is `N * (B - 1)` requests, plus decisions made during the reconciliation interval
`T`. For a "hard global limit" requirement, that is not a tradeoff that can be
made silently. The baseline therefore pays one RTT per check. The design leaves
room for a future opt-in prefetch layer with an explicit reservation protocol
and a published overshoot bound.

**Keeping hiredis instead of redis-plus-plus.** The single-key `EVALSHA` path
does not need pipelining. redis-plus-plus would remove some pool code but add an
abstraction layer and a dependency; the measured hot path is dominated by Redis
execution and RTT, not by C++ wrapper overhead. hiredis also makes the
`NOSCRIPT` recovery and failure cooldown explicit, which matters for a limiter
that must behave deterministically when Redis restarts.

**A measurement-driven test fix.** The first cluster test used four backend HTTP
threads and eight proxy threads with persistent proxy-to-backend connections.
Under load, the proxy's persistent connections occupied all backend workers and
caused 5-second stalls and transport errors. The fix was to size backend workers
above the number of proxy connections (16 backend threads for 8 proxy threads)
and to document the requirement. The limiter itself was not the bottleneck; the
lesson is that a synchronous per-request limiter inherits the concurrency
properties of its transport, and those must be measured, not assumed.

### 4.3 Efficiency and results

**Methodology.** Measurements were taken on a Windows development host with an
MSYS2 GCC 16.2.0 Release build, Redis 5.0.14.1 (Windows port) listening on
loopback, and the `rl-redis-bench` executable. Each run lasted 5 seconds, used
one shared key, and used a capacity/limit large enough that all checks were
allowed; this isolates limiter cost from 429 handling. Latency is measured around
`RateLimiter::check()` and therefore includes connection-pool acquisition, Redis
round trip, Lua execution, and reply parsing. Connections were warm. The host was
not CPU-pinned, so these are order-of-magnitude numbers, not a controlled
production benchmark. The same Redis instance served all servers; this is the
intended architecture, and it is also the throughput ceiling.

**Token bucket (Release, 5 s, shared key):**

| Servers | Threads/server | Checks/s | p50 µs | p95 µs | p99 µs |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 5,678 | 153 | 272 | 765 |
| 1 | 2 | 11,939 | 132 | 294 | 619 |
| 1 | 3 | 9,759 | 239 | 606 | 1,181 |
| 3 | 1 | 8,630 | 294 | 687 | 1,291 |
| 3 | 2 | 9,895 | 469 | 1,292 | 2,365 |
| 3 | 3 | 9,877 | 744 | 1,993 | 3,095 |

**Sliding window log (Release, 5 s, shared key):**

| Servers | Threads/server | Checks/s | p50 µs | p95 µs | p99 µs |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 5,088 | 173 | 303 | 801 |
| 1 | 2 | 8,131 | 226 | 409 | 890 |
| 1 | 3 | 5,890 | 364 | 1,007 | 2,152 |
| 3 | 1 | 6,776 | 346 | 859 | 1,744 |
| 3 | 2 | 5,892 | 789 | 2,339 | 4,391 |
| 3 | 3 | 6,005 | 1,199 | 3,156 | 5,274 |

**Redis-side command time (5 s, 3 servers × 3 threads, same key):**

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

The `EVALSHA` number is the whole script, including the internal commands; the
individual command rows show where the time goes. The sliding-window script is
more expensive because it maintains a sorted set and inserts one member per
request unit.

**Memory per key (measured with `MEMORY USAGE` after the runs):**

| Algorithm | Key contents | Bytes |
|---|---|---:|
| Token bucket | hash with `tokens_u`, `last_ms` | 150 |
| Sliding window | 42,042 zset members | 4,462,432 |
| Sliding window | sequence string | 106 |

The token bucket is effectively O(1). The sliding window is O(requests in
window): about 106 bytes per request unit including zset overhead. At 1,000
requests per window per key and one million active keys, the log approach is
roughly 106 GB of Redis memory, which is why it is a strict-quota tool rather
than a default for every endpoint. A sliding-window counter would reduce this to
O(1) at the cost of boundary approximation.

**Concurrency behavior.** Throughput improves from one to two threads and then
plateaus or degrades. This is expected: Redis executes one script at a time per
shard, and all checks use the same key, so additional threads add queueing and
context switching rather than parallelism. Three servers do not multiply
throughput; they share the same Redis serialization point. The practical scaling
strategy is to shard keys across Redis instances (or Redis Cluster) and to keep
the per-key scripts slot-local, not to add app threads. Latency grows with
contention: p99 for 3×3 is roughly 4× the single-thread p99 for the token bucket
and roughly 6.5× for the sliding window.

**Analytical WAN estimate.** At a 1 ms RTT, one connection can sustain about
1,000 checks/s; a 16-connection pool can sustain about 16,000 checks/s per
server; three servers about 48,000 checks/s if Redis and the network can keep up.
At 0.5 ms RTT the corresponding figures are 2,000/s per connection and 32,000/s
per server. Lua execution adds tens of microseconds, not milliseconds; the RTT
dominates. This is why the baseline is simple and why local prefetch is tempting
for latency, but the global-limit requirement keeps the atomic round trip.

**HTTP path.** The library benchmark is the right measure of limiter overhead.
The HTTP server adds JSON parsing, `httplib` request handling, and response
serialization; a debug-build smoke run measured roughly 1.5–1.9k requests/s with
4–8 client threads on one server. Those numbers are not directly comparable to
the Release library benchmark and are not used to claim limiter throughput.

### 4.4 Limitations and future work

- **Redis is a single point of failure.** The baseline has one Redis. Mitigation
  paths are Redis Sentinel for automatic failover, Redis Cluster for sharding and
  availability, or a bounded local fallback. Sentinel/cluster support requires a
  cluster-aware client or proxy; hiredis alone does not follow redirects.
- **Network partitions.** The service returns `503` by default (fail-closed).
  With `--fail-open`, it serves traffic and marks the decision as `fail_open`.
  Neither is a substitute for a reservation-based fallback that preserves the
  global limit; that is future work.
- **Multi-key atomicity in Redis Cluster.** The sliding-window script touches
  two keys, so they share the hash tag `{policy:client}`. The token-bucket script
  touches one key. Any future script that spans keys must use hash tags or be
  restructured; cross-slot Lua is not supported.
- **Clock skew.** App clocks are irrelevant because Lua uses Redis `TIME`.
  Residual risk is a step in the Redis server clock: a backward step is clamped
  to zero elapsed and does not add tokens; a forward step can add up to the
  bucket capacity. Sliding-window scores inherit the same Redis clock.
- **Sliding-window memory.** O(requests in window), and `cost > 1` inserts
  `cost` members. The bounded `max_cost` (10,000) prevents an unbounded single
  script, but a large cost can still block Redis. A weighted counter variant
  would fix both memory and insert cost at the price of boundary approximation.
- **No local prefetch/async API.** Every check pays a Redis round trip. The
  synchronous API is deliberate; an async path and an opt-in prefetch layer with
  a quantified overshoot bound are future work.
- **Transport caveat.** The optional proxy uses persistent backend connections.
  If proxy worker threads exceed backend HTTP threads, persistent connections can
  occupy all backend workers and cause stalls. Size backend threads above the
  proxy's connection count, or disable keep-alive on the proxy path.
- **No TLS/ACL support in the Redis client.** hiredis SSL is not linked here.
  Production deployments should use a private network, TLS, ACLs, and Redis
  `maxmemory`/eviction policies appropriate to rate-limit keys.
- **No dynamic policy reload.** Policies are loaded at startup. A control plane
  that updates limits without restart is future work; the registry is sealed to
  make the read path lock-free.
- **No per-key sharding inside the process.** All keys share the pool. Key
  sharding and hot-path caching (as in the C++20 gRPC project) are future
  optimizations; they do not change the Redis-side atomicity requirement.

---

## Appendix A — Reproduce the measurements

```sh
# Redis on loopback, Release build
cmake --preset release
cmake --build --preset release
ctest --test-dir build/release --output-on-failure

./build/release/rl-demo
./build/release/rl-redis-bench --algorithm tb --servers 3 --threads-per-server 3 --duration 5
./build/release/rl-redis-bench --algorithm sw --servers 3 --threads-per-server 3 --duration 5

redis-cli config resetstat
./build/release/rl-redis-bench --algorithm tb --servers 3 --threads-per-server 3 --duration 5
redis-cli info commandstats | grep -E 'evalsha|hmget|pexpire|time'
redis-cli memory usage 'rl:bench:<run>:tb:{bench-token-bucket:bench-client}'
```

## Appendix B — Key files

- `scripts/token_bucket.lua`, `scripts/sliding_window.lua`
- `src/redis/redis_client.cpp`
- `src/algorithms/token_bucket.cpp`, `src/algorithms/sliding_window.cpp`
- `src/service/rate_limiter_service.cpp`
- `demo/three_server_demo.cpp`
- `benchmarks/redis_bench.cpp`
- `tests/test_main.cpp`, `tests/cluster_test.cpp`, `tests/failure_test.cpp`
- `CMakeLists.txt`
