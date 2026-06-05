# 02 · Architecture

[← 01 Overview](./01-overview.md) · [Index](./README.md) · [Next: 03 World contract →](./03-world-contract.md)

---

## 2.1 The four layers

```
 ┌───────────────────────────────────────────────────────────────────────┐
 │                          1.  AUTHOR LAYER                             │
 │                                                                       │
 │     "use workflow"  "use step"  step()  sleep()  createHook()         │
 │     getRun()  cancelRun()  FatalError  RetryableError                 │
 │                                                                       │
 │     ── you write TypeScript that uses the `workflow` package ──       │
 └───────────────────────────────────┬───────────────────────────────────┘
                                     │ SWC plugin (@workflow/swc-plugin)
                                     │ build-time transformation
                                     ▼
 ┌───────────────────────────────────────────────────────────────────────┐
 │                       2.  RUNTIME LAYER  (@workflow/core)             │
 │                                                                       │
 │      flow handler     step handler     webhook handler                │
 │   POST /.well-known/  POST /.well-     POST /.well-                   │
 │      v1/flow          known/v1/step      known/v1/webhook/<tok>       │
 │                                                                       │
 │      replays event log → drives workflow body → emits events          │
 │                                                                       │
 │      loads world via WORKFLOW_TARGET_WORLD ──┐                        │
 └───────────────────────────────────────────────┼───────────────────────┘
                                                 ▼
 ┌───────────────────────────────────────────────────────────────────────┐
 │                  3.  WORLD LAYER  (@open-workflow/world-redirect)     │
 │                                                                       │
 │   ┌───────────────┐  ┌──────────────────────┐  ┌──────────────────┐   │
 │   │   Storage     │  │  Queue + Dispatcher  │  │   Streamer       │   │
 │   │   (event-     │  │  (ZSET scheduler +   │  │   (pub/sub live  │   │
 │   │   sourced)    │  │   307 trampoline)    │  │   + LIST replay) │   │
 │   └───────┬───────┘  └──────────┬───────────┘  └────────┬─────────┘   │
 │           └────────────┬────────┴────────────┬──────────┘             │
 │                        │                     │                        │
 │                        ▼                     ▼                        │
 │              ┌─────────────────────────────────────────┐              │
 │              │  RedisClient  (one of three backends)   │              │
 │              │  • UpstashRedisClient   (HTTP/REST)     │              │
 │              │  • NodeRedisClient      (RESP / TCP)    │              │
 │              │  • MemoryRedisClient    (in-process)    │              │
 │              └────────────────────┬────────────────────┘              │
 └───────────────────────────────────┼───────────────────────────────────┘
                                     ▼
 ┌───────────────────────────────────────────────────────────────────────┐
 │                  4.  REDIS  (the durable substrate)                   │
 │                                                                       │
 │   You bring this. Upstash, ElastiCache, self-hosted, or a Map         │
 │   for development. It's the source of truth for everything.           │
 └───────────────────────────────────────────────────────────────────────┘
```

> **Boundary rule**: each layer only talks to the layer directly below
> it. The Author layer never touches Redis directly; the Runtime never
> reaches inside the World implementation; the World never knows the
> Runtime exists (it's loaded by name, called like a library).

---

## 2.2 Components inside the World

`world-redirect` itself is composed of four small, single-responsibility
modules:

```
                              createWorld()
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │  RedisWorld         │
                         │  (the assembled     │
                         │  World object)      │
                         └──────────┬──────────┘
                                    │
        ┌───────────────┬───────────┴───────────┬────────────────┐
        ▼               ▼                       ▼                ▼
  ┌──────────┐   ┌────────────┐         ┌─────────────┐   ┌────────────┐
  │ Storage  │   │ Queue +    │         │ Streamer    │   │ Lifecycle  │
  │          │   │ Dispatcher │         │             │   │ (start /   │
  │ runs     │   │            │         │ streams.    │   │ close)     │
  │ steps    │   │ queue()    │         │   write     │   │            │
  │ events   │   │ createQH() │         │   close     │   │ recover    │
  │ hooks    │   │ pump loop  │         │   get       │   │ active     │
  │ waits    │   │            │         │   getChunks │   │ runs       │
  └────┬─────┘   └─────┬──────┘         └──────┬──────┘   └─────┬──────┘
       │               │                       │                │
       └───────────────┴─────────┬─────────────┴────────────────┘
                                 ▼
                           RedisClient
```

| module | file | responsibility |
| --- | --- | --- |
| **Storage** | `src/storage/*.ts` | The event-sourced layer — `events.create` is the only mutating entry point; all entity transitions happen through it. |
| **Queue + Dispatcher** | `src/queue/*.ts` | `queue()` writes a message + ZADDs the scheduler; the dispatcher polls the ZSET and POSTs to the flow handler. |
| **Streamer** | `src/streamer/*.ts` | `write` appends to a Redis LIST and PUBLISHes to a channel; `get` SUBSCRIBEs or polls (depending on client). |
| **Lifecycle** | `src/index.ts` (`start()`, `close()`) | Starts the dispatcher, re-queues pending/running runs after a restart, gracefully drains in-flight on close. |

---

## 2.3 World lifecycle

```
   process boots
        │
        ▼
   ┌────────────────────────────────┐
   │ require/import                  │
   │ '@open-workflow/world-redirect' │
   └────────────────┬────────────────┘
                    │
                    ▼
   createWorld()                            ─┐
   │                                         │  pure construction
   │  • resolveConfig() from env vars        │  no network I/O
   │  • pick RedisClient impl                │  cheap; safe to call
   │  • new RedisWorld({storage, queue,…})   │  many times — cached
   │                                         │  by config key
   └────────────────┬───────────────────────┘
                    │
                    ▼
   world.start()                            ─┐
   │                                         │  optional but normal
   │  • dispatcher.startPump()  → polls ZSET │  for any host that
   │  • if recoverActiveRuns:                │  *runs* workflows
   │      for status in {pending, running}:  │  (not for dashboards)
   │        re-enqueue each run              │
   │                                         │
   └────────────────┬───────────────────────┘
                    │
                    ▼
   normal operation
   • Runtime calls world.events.create(...) → Storage writes
   • Runtime calls world.queue(...)         → Queue writes ZSET + msg
   • Dispatcher tick → reads ZSET           → POST flow handler
   • Handler returns 307 → fetch follows    → continues chain
                    │
                    ▼
   world.close()                            ─┐
   │  • dispatcher.stopPump()                │  graceful drain
   │  • wait for in-flight ≤ 5s              │  before exit
   │  • close Redis connection (if RESP)     │
   └────────────────────────────────────────┘
```

The `createWorld()` factory is **idempotent and cheap**: it caches one
instance per resolved-config key, so calling it many times in a process
returns the same object. This matters because the WDK runtime loads the
world with `require()`/`import()` per-handler-invocation in some target
configurations.

---

## 2.4 Concurrency model

### 2.4.1 Within one process

```
  world.events.create(runId, {...})
            │
            ▼
   ┌──────────────────────────────┐
   │ RunMutex.run(runId, async() ─│─▶  per-run in-process lock
   │   { ... actual write ... })  │    (event mutations serialize
   │                              │    per runId; different runs
   │                              │    proceed concurrently)
   └──────────────────────────────┘
```

The Storage layer serialises all event writes for the same `runId`
through an in-process `RunMutex`. Two events on different runs proceed
in parallel; two events on the same run are ordered.

> **Why per-run rather than per-process**: workflows can fan out — many
> `step()`s for the same run advance concurrently inside the same
> handler. The mutex serialises only the storage transitions that
> mustn't race (state machine guards, NX claims), letting the
> compute-heavy step bodies overlap.

### 2.4.2 Across processes

`world-redirect` does **not** assume a single host. Multi-host is
supported but with rules:

```
   ┌──────────────────────────┐     ┌──────────────────────────┐
   │  host A  (workflow exec) │     │  host B  (workflow exec) │
   │  - flow handler runs     │     │  - flow handler runs     │
   │  - dispatcher ON         │     │  - dispatcher ON   ⚠     │
   └────────────┬─────────────┘     └────────────┬─────────────┘
                │                                │
                └────────────┬───────────────────┘
                             ▼
                  ┌────────────────────────────────────────┐
                  │  Redis                                  │
                  │  • event writes guarded by NX on ZADD   │
                  │  • job claim via lease key (NX + EX 30) │
                  │  • dispatcher pollers race, lease wins  │
                  └────────────────────────────────────────┘
```

Multiple dispatchers polling the same `jobs` ZSET will both see the same
due jobs; the **lease key** (`owf:msg:<msgId>:lease` set with `NX EX 30`)
acts as the at-most-one claim. The loser of the race quietly skips.

**Recommended**: pin one dispatcher per environment by setting
`WORKFLOW_REDIS_DISABLE_DISPATCHER=1` on the other hosts; this avoids
wasted Redis ops on lease contention.

### 2.4.3 The dashboard

The dashboard (`@workflow/web`) is **always** a reader-only host:

```
   ┌──────────────────────────┐
   │  @workflow/web           │
   │  dispatcher OFF (set     │
   │  WORKFLOW_REDIS_DISABLE_  │
   │  DISPATCHER=1)            │
   │                          │
   │  reads:                  │
   │    runs / steps / events │
   │    streams (live tail)   │
   │                          │
   │  writes (UI mutations):  │
   │    cancelRun, recreate,  │
   │    reenqueueRun,         │
   │    wakeUpRun, resumeHook │
   └──────────────────────────┘
```

The dashboard does write — but only mutations the operator initiated.
The dispatcher must be off so it doesn't race the production
dispatcher.

---

## 2.5 The singleton cache

`createWorld()` caches by a "config key" — a deterministic snapshot of
the env vars that affect world construction:

```
   createWorld() called
        │
        ▼
   build cacheKey =
     [REDIS_URL,
      REDIS_REST_URL,
      KEY_PREFIX,
      BASE_URL,
      DEPLOYMENT_ID,
      …all WORKFLOW_REDIS_*]
     .join('|')
        │
        ▼
   already in Map?
        │
   ┌────┴────┐
   yes        no
   │          │
   │          ▼
   │     construct new RedisWorld
   │     cache.set(cacheKey, world)
   │          │
   ▼          ▼
   return cached world
```

This means:

- Switching env vars *between* `createWorld()` calls in the same
  process gives you separate worlds (e.g. two Redis URLs, two
  dispatchers).
- The same env vars always return the same instance — important for
  dispatchers (you don't want N pumps in one process), and for
  test setups.

---

## 2.6 Resource ownership

```
                 ┌────────────────────────────┐
                 │  Owned by world-redirect:  │
                 │  • RunMutex                │
                 │  • dispatcher pump timer   │
                 │  • in-flight job counter   │
                 │  • RedisClient connection  │
                 │    (only NodeRedisClient — │
                 │    Upstash REST is         │
                 │    stateless)              │
                 └─────────────┬──────────────┘
                               │
                               │ closed by world.close()
                               ▼
                          (clean exit)

                 ┌────────────────────────────┐
                 │  Not owned (you bring):    │
                 │  • Redis itself            │
                 │  • the HTTP host           │
                 │    (Next.js, standalone,   │
                 │     EdgeOne function)      │
                 │  • the SWC build output    │
                 └────────────────────────────┘
```

`world-redirect` does not start an HTTP server. The flow / step /
webhook endpoints are provided by the host (Next.js routes, the
standalone host, etc.); the world just exposes the `Queue`, `Storage`,
and `Streamer` that those endpoints call into.

---

## 2.7 Module map (file → responsibility)

```
packages/world-redirect/src/
├── index.ts                     createWorld(), RedisWorld assembly, singleton cache
├── codec.ts                     CBOR + base64 helpers, ULID id generators
├── config.ts                    resolveRedisConfig() — env-var fallthrough
│
├── client/
│   ├── index.ts                 createRedisClient() — selects backend
│   ├── upstash.ts               UpstashRedisClient (REST)
│   ├── node-redis.ts            NodeRedisClient (RESP / TCP)
│   ├── memory.ts                MemoryRedisClient (in-process Map)
│   └── types.ts                 RedisClient interface
│
├── storage/
│   ├── index.ts                 createRedisStorage() — wires the sub-stores
│   ├── runs.ts                  WorkflowRun blob + indexes
│   ├── steps.ts                 Step blob + indexes
│   ├── events.ts                events.create — the event log writer
│   ├── hooks.ts                 Hook blob + token NX claim
│   ├── waits.ts                 Wait blob (sleep + hook waits)
│   └── mutex.ts                 RunMutex (per-run serialisation)
│
├── queue/
│   ├── index.ts                 createRedisQueue() — queue() + handler
│   ├── dispatcher.ts            pump loop (poll ZSET, POST flow)
│   ├── trampoline.ts            buildRedirect() — 307 response helper
│   └── lease.ts                 lease key NX/EX helpers
│
└── streamer/
    ├── index.ts                 createRedisStreamer() — streams.* methods
    ├── live.ts                  pub/sub subscriber loop
    └── replay.ts                LRANGE-based replay reader
```

Cross-reference: every implementation detail in the rest of this
reference maps back to one of these files.

---

## 2.8 What the runtime expects

When `@workflow/core` calls into the world, the surface it touches is
small and the calls happen on well-defined boundaries:

```
   request arrives at flow handler
         │
         ▼
   1.  world.queue → look up msg body in Redis
   2.  world.events.list(runId) → replay event log
   3.  workflow body runs (your code)
   4.  every step/hook/sleep → world.events.create(...)
   5.  every workflow output write → world.streams.write(...)
   6.  next durable jump?
         │
         ├── immediate → world.queue({delay:0}) + 307 response
         └── delayed   → world.queue({delay:ms}) + 200 response
   7.  request ends; lease released by handler
```

The next page ([03 · World contract](./03-world-contract.md)) walks the
exact method signatures the runtime relies on.

---

[← 01 Overview](./01-overview.md) · [Index](./README.md) · [Next: 03 World contract →](./03-world-contract.md)
