# 10 · Clients & configuration

[← 09 Hooks & streams](./09-hooks-and-streams.md) · [Index](./README.md) · [Next: 11 Deployment →](./11-deployment.md)

---

`world-redirect` ships three Redis client backends and a single
`createWorld()` factory that picks one based on environment variables
or explicit config. This page documents the client selection logic,
each client's trade-offs, and the complete env-var matrix.

---

## 10.1 The three clients

```
                          ┌─────────────────────────────┐
                          │   RedisClient (interface)   │
                          │                             │
                          │   get / set / setnx         │
                          │   del / hset / hgetall      │
                          │   rpush / lrange / lrem     │
                          │   zadd / zrem               │
                          │   zrangebyscore / zrange    │
                          │   publish / subscribe       │
                          │   eval (Lua, for atomic ops)│
                          │   disconnect (RESP only)    │
                          └──────────┬──────────────────┘
                                     │
              ┌──────────────────────┼─────────────────────────┐
              │                      │                         │
              ▼                      ▼                         ▼
   ┌──────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
   │ UpstashRedis     │  │ NodeRedisClient      │  │ MemoryRedisClient    │
   │ Client (REST)    │  │ (RESP / TCP)         │  │ (in-process Map)     │
   │                  │  │                      │  │                      │
   │ HTTP per op      │  │ persistent TCP conn  │  │ no I/O at all        │
   │ stateless        │  │ pipelined commands   │  │ for tests / dev      │
   │ pub/sub: ❌      │  │ pub/sub: ✓           │  │ pub/sub: simulated   │
   │ Lua: ✓ (eval)    │  │ Lua: ✓               │  │ Lua: subset          │
   │ TLS: ✓ (HTTPS)   │  │ TLS: ✓ (rediss://)   │  │ N/A                  │
   └──────────────────┘  └──────────────────────┘  └──────────────────────┘
```

### 10.1.1 `UpstashRedisClient`

```
   ┌─────────────────────────────────────────────────────────────┐
   │  When to use:                                               │
   │  • Serverless functions (no long-lived connections)         │
   │  • Edge runtimes (no TCP sockets — only HTTP fetch)         │
   │  • Anywhere "cold start with fresh state" is normal         │
   │                                                             │
   │  How it works:                                              │
   │  Each Redis op = one HTTPS POST to Upstash's REST endpoint  │
   │  Auth = Bearer token in Authorization header                │
   │                                                             │
   │  Costs:                                                     │
   │  • One TCP+TLS handshake per request (mitigated by HTTP/2   │
   │    keep-alive on supporting clients)                        │
   │  • One billed op per call (Upstash counts these)            │
   │                                                             │
   │  Trade-offs:                                                │
   │  • No native pub/sub → streamer falls back to polling       │
   │  • No persistent connection → no MULTI; each op atomic only │
   │  • Backend automatically deserialised by Upstash unless     │
   │    automaticDeserialization:false (we set false to control  │
   │    encoding ourselves)                                      │
   └─────────────────────────────────────────────────────────────┘
```

### 10.1.2 `NodeRedisClient`

```
   ┌─────────────────────────────────────────────────────────────┐
   │  When to use:                                               │
   │  • Long-running Node hosts (the standalone host, dashboards)│
   │  • Self-hosted / ElastiCache deployments                    │
   │  • Anywhere you can hold a TCP socket open                  │
   │                                                             │
   │  How it works:                                              │
   │  Uses the `redis` npm package (node-redis v4+)              │
   │  Single TCP connection per process                          │
   │  Pipelines commands via the RESP protocol                   │
   │                                                             │
   │  Trade-offs:                                                │
   │  • Pub/sub works natively (no polling for streams)          │
   │  • Higher throughput, lower per-op latency                  │
   │  • Must call `world.close()` to release the connection      │
   │  • Reconnect logic is built into node-redis                 │
   └─────────────────────────────────────────────────────────────┘
```

### 10.1.3 `MemoryRedisClient`

```
   ┌─────────────────────────────────────────────────────────────┐
   │  When to use:                                               │
   │  • Unit tests / integration tests                           │
   │  • Local development with no Redis installed                │
   │  • Smoke-testing the world without external dependencies    │
   │                                                             │
   │  How it works:                                              │
   │  A Map<string,any> with implementations of the Redis        │
   │  operations the world uses                                  │
   │  Pub/sub simulated with EventEmitter                        │
   │                                                             │
   │  Trade-offs:                                                │
   │  • Process-local only — state lost on restart               │
   │  • Single-process — no multi-host                           │
   │  • No persistence — irrelevant for dev/test                 │
   │  • Exported separately at `/client` to avoid bundling into   │
   │    production builds                                        │
   └─────────────────────────────────────────────────────────────┘
```

---

## 10.2 Client selection logic

`createWorld()` resolves the client in this order:

```
   resolveRedisConfig(env)
        │
        ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  if config.client    → use it directly                        │
   │  elif both                                                    │
   │      WORKFLOW_REDIS_REST_URL                                  │
   │      WORKFLOW_REDIS_REST_TOKEN     present                    │
   │      (or UPSTASH_REDIS_REST_URL / _TOKEN as fallback)         │
   │                       → UpstashRedisClient(url, token)        │
   │  elif WORKFLOW_REDIS_URL present                              │
   │      and url.startsWith("redis://" or "rediss://")            │
   │                       → NodeRedisClient(url)                  │
   │  elif WORKFLOW_REDIS_URL === "memory"                         │
   │                       → MemoryRedisClient()                   │
   │  else                  → throw "no Redis configured"          │
   └──────────────────────────────────────────────────────────────┘
```

```
       ┌──────────────────────────────────────────────────┐
       │  Selection tree                                   │
       │                                                   │
       │  explicit config.client?                          │
       │    ├─ yes → use it                                │
       │    └─ no →                                        │
       │       REST_URL + REST_TOKEN?                      │
       │         ├─ yes → UpstashRedisClient               │
       │         └─ no  →                                  │
       │            URL = "memory"?                        │
       │              ├─ yes → MemoryRedisClient            │
       │              └─ no  →                              │
       │                 URL = "redis://"|"rediss://"?     │
       │                   ├─ yes → NodeRedisClient         │
       │                   └─ no  → error                  │
       └──────────────────────────────────────────────────┘
```

---

## 10.3 Environment variable reference

### 10.3.1 Connection

| env var | purpose | default |
| --- | --- | --- |
| `WORKFLOW_REDIS_URL` | RESP URL (`redis://…` or `rediss://…`), or the literal `memory` | — |
| `WORKFLOW_REDIS_REST_URL` | Upstash REST endpoint base URL | — |
| `WORKFLOW_REDIS_REST_TOKEN` | Upstash REST token (`Bearer …`) | — |
| `UPSTASH_REDIS_REST_URL` | alias for `WORKFLOW_REDIS_REST_URL` | — |
| `UPSTASH_REDIS_REST_TOKEN` | alias for `WORKFLOW_REDIS_REST_TOKEN` | — |

### 10.3.2 Namespace & deployment identity

| env var | purpose | default |
| --- | --- | --- |
| `WORKFLOW_REDIS_KEY_PREFIX` | prefix prepended to every key | `owf` |
| `WORKFLOW_DEPLOYMENT_ID` | reported by `getDeploymentId()` | `dpl_redirect_local` |
| `WORKFLOW_BASE_URL` | where the dispatcher POSTs (your host's URL) | `http://localhost:${PORT||3000}` |
| `WORKFLOW_OWNER_ID` | recorded on each hook | `owf-owner` |
| `WORKFLOW_PROJECT_ID` | recorded on each hook | `owf-project` |
| `WORKFLOW_ENVIRONMENT` | recorded on each hook | `development` |

### 10.3.3 Dispatcher tuning

| env var | purpose | default |
| --- | --- | --- |
| `WORKFLOW_REDIS_DISABLE_DISPATCHER` | `1` → don't start the pump (read-only host) | unset |
| `WORKFLOW_REDIS_DISPATCHER_POLL_MS` | poll tick interval | `1000` |
| `WORKFLOW_REDIS_DISPATCHER_BATCH` | candidates per tick | `8` |
| `WORKFLOW_REDIS_LEASE_SECONDS` | msg lease TTL | `30` |
| `WORKFLOW_REDIS_MAX_HOPS` | trampoline safety cap | `32` |
| `WORKFLOW_REDIS_MAX_ATTEMPTS` | drop after this many attempts | `10` |
| `WORKFLOW_REDIS_RETRY_BASE_MS` | retry backoff base | `5000` |

### 10.3.4 Stream tuning

| env var | purpose | default |
| --- | --- | --- |
| `WORKFLOW_REDIS_STREAM_FLUSH_MS` | poll interval for Upstash fallback live tail | `2000` |

### 10.3.5 Lifecycle

| env var | purpose | default |
| --- | --- | --- |
| `WORKFLOW_REDIS_RECOVER_ACTIVE_RUNS` | on `start()`, re-enqueue pending/running runs | `1` |

---

## 10.4 Programmatic config

```ts
import { createRedisWorld } from "@open-workflow/world-redirect";

const world = createRedisWorld({
  // explicit client wins over env vars
  client: undefined,                       // or a custom RedisClient instance

  // alternative: pass connection details inline
  redisUrl: "rediss://my-redis:6379",
  redisRestUrl: "https://eu1-renewing-heron-48789.upstash.io",
  redisRestToken: "AB…",

  // namespace
  keyPrefix: "myapp",
  deploymentId: "dpl_my_prod_v3",
  baseUrl: "https://my-app.example.com",

  // dispatcher
  startDispatcher: true,
  dispatcherPollMs: 1000,
  dispatcherBatch: 8,
  leaseSeconds: 30,
  maxHops: 32,
  maxAttempts: 10,
  retryBaseMs: 5000,

  // streams
  streamFlushIntervalMs: 2000,

  // lifecycle
  recoverActiveRuns: true,
});

await world.start();
```

Anything you don't pass is filled from env, falling back to defaults
above.

---

## 10.5 The `RedisClient` interface

For implementers writing a new client (e.g. to talk to a different
backend that you'd like to keep behind the same world):

```ts
export interface RedisClient {
  // strings
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: SetOptions): Promise<"OK" | null>;
  setnx(key: string, value: string): Promise<number>;

  // expirations / deletions
  del(...keys: string[]): Promise<number>;
  expire(key: string, seconds: number): Promise<number>;

  // hashes
  hgetall(key: string): Promise<Record<string, string>>;
  hset(key: string, fields: Record<string, string>): Promise<number>;
  hdel(key: string, ...fields: string[]): Promise<number>;

  // lists
  rpush(key: string, ...values: string[]): Promise<number>;
  lrange(key: string, start: number, stop: number): Promise<string[]>;
  llen(key: string): Promise<number>;

  // sets
  sadd(key: string, ...members: string[]): Promise<number>;
  smembers(key: string): Promise<string[]>;
  srem(key: string, ...members: string[]): Promise<number>;

  // sorted sets
  zadd(key: string, score: number, member: string): Promise<number>;
  zrem(key: string, ...members: string[]): Promise<number>;
  zrangebyscore(
    key: string,
    min: number | string,
    max: number | string,
    opts?: ZRangeOptions
  ): Promise<string[]>;
  zrange(
    key: string,
    start: number,
    stop: number,
    opts?: ZRangeOptions
  ): Promise<string[]>;

  // pub/sub (optional — fallback to polling if missing)
  publish?(channel: string, message: string): Promise<number>;
  subscribe?(
    channel: string,
    handler: (msg: string) => void
  ): Promise<() => Promise<void>>;

  // lua (optional — used for atomic ops)
  eval?(
    script: string,
    keys: string[],
    args: string[]
  ): Promise<unknown>;

  // lifecycle (optional — only stateful clients need it)
  disconnect?(): Promise<void>;
}
```

Backends without `subscribe` get the polling streamer; backends
without `eval` get a less-atomic fallback in the few places Lua is
used (currently only the multi-step atomic claim path).

---

## 10.6 Example configurations

### 10.6.1 Upstash (production, edge / serverless)

```bash
WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect
WORKFLOW_REDIS_REST_URL=https://renewing-heron-48789.upstash.io
WORKFLOW_REDIS_REST_TOKEN=AB...
WORKFLOW_BASE_URL=https://my-app.example.com
WORKFLOW_REDIS_KEY_PREFIX=owf
WORKFLOW_DEPLOYMENT_ID=dpl_prod_v3
```

### 10.6.2 Self-hosted RESP (long-running Node host)

```bash
WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect
WORKFLOW_REDIS_URL=rediss://redis.internal:6379
WORKFLOW_BASE_URL=https://my-app.example.com
WORKFLOW_REDIS_KEY_PREFIX=owf
```

### 10.6.3 Local dev (in-memory)

```bash
WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect
WORKFLOW_REDIS_URL=memory
WORKFLOW_BASE_URL=http://localhost:3000
```

### 10.6.4 Dashboard reader (no dispatcher)

```bash
WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect
WORKFLOW_REDIS_REST_URL=https://renewing-heron-48789.upstash.io
WORKFLOW_REDIS_REST_TOKEN=AB...
WORKFLOW_REDIS_KEY_PREFIX=owf
WORKFLOW_REDIS_DISABLE_DISPATCHER=1
PORT=4000
```

---

## 10.7 Choosing prefix carefully

`WORKFLOW_REDIS_KEY_PREFIX` is the most important multi-tenancy lever.
Two deployments sharing a Redis must have different prefixes or they
will read each other's runs:

```
   prod uses owf:run:wrun_…           ┐
   staging uses staging:run:wrun_…     ├── safe in same Redis
   review uses review-pr42:run:wrun_… ┘
```

Indexes are prefixed too, so list queries don't bleed across
environments. Switching prefixes is the cleanest way to provide a
"clean slate" for a deployment without dropping data from another.

---

[← 09 Hooks & streams](./09-hooks-and-streams.md) · [Index](./README.md) · [Next: 11 Deployment →](./11-deployment.md)
