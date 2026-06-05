# 01 · Overview

[← Index](./README.md) · [Next: 02 Architecture →](./02-architecture.md)

---

## 1.1 Why this exists

The **Vercel Workflow Development Kit (WDK)** is an elegant programming
model: write functions tagged `"use workflow"` / `"use step"`, get
durable execution, exactly-once side effects, sleeps that survive
crashes, and a built-in event log. The author API is excellent.

The **runtime** that backs it is not. The production runtime is
`@workflow/world-vercel`, which delegates every durable operation to a
managed Vercel queue + storage:

- It's billed per message.
- It ties your workflow's data and execution lifecycle to Vercel.
- It can't be self-hosted.
- It can't be pointed at infrastructure you already own (a Redis you
  already run, a Postgres, a Kafka).

`@open-workflow/world-redirect` is a drop-in replacement for that one
package. Your workflows keep the same author API, the same SWC plugin,
the same dashboard. What changes is the substrate underneath.

---

## 1.2 The central trick — Redis + 307

The WDK's `World` interface (`@workflow/world`) is intentionally narrow:

```
type World = Queue & Storage & Streamer
```

Replacing it requires replacing three things:

1. **A durable queue** that supports delayed delivery (for `sleep`,
   retry backoff, future-dated steps).
2. **An event-sourced storage layer** with materialised entity views
   (runs, steps, hooks, waits) and an append-only event log.
3. **A streamer** with both live pub/sub (for tail-reading) and a
   replay buffer (for late readers).

Redis covers all three primitives natively:

- ZSET keyed by `runAtMs` → durable scheduler.
- HASH per entity + STRING per event + LIST per run → event-sourced
  storage.
- LIST + PUB/SUB → streams with replay.

What Redis doesn't give you is a **broker** — something that watches the
ZSET and physically delivers ready jobs to your HTTP endpoint. That's
where the **307 trampoline** comes in:

> An in-process dispatcher polls Redis for due jobs and POSTs them to
> your flow handler. The flow handler, when it has another durable jump
> ready *right now*, returns a **`307 Temporary Redirect`** to itself
> with the next message ID in the query string. `fetch` automatically
> follows the 307 with the original POST method and body preserved, so
> the same HTTP request — and the same function invocation — handles
> multiple durable jumps in sequence.

That is the *entire* defining trick of `world-redirect`. Everything else
is plumbing.

---

## 1.3 Vercel WDK vs `world-redirect`

```
                  VERCEL WDK (the managed path)
                  ─────────────────────────────

  ┌─────────────┐                                ┌─────────────────────┐
  │ start(wf)   │ ─────── HTTP ───────────────▶  │ Vercel Workflow API │
  │  (client)   │                                │   (managed, $)      │
  └─────────────┘                                └──────────┬──────────┘
                                                            │
                                                  enqueue   │
                                                            ▼
                                                ┌────────────────────────┐
                                                │  Vercel Queue (paid)   │
                                                │  Vercel Storage (paid) │
                                                └──────────┬─────────────┘
                                                           │
                       (one broker → app HTTP request PER event)
                                                           ▼
                              POST  /.well-known/workflow/v1/flow
                                                           │
                                                           ▼
                                                ┌────────────────────────┐
                                                │ your app function      │
                                                │ runs workflow body     │
                                                │ • need a step?  ─────▶ │── POST /step ──▶ Vercel
                                                │ • need to sleep?─────▶ │── enqueue ────▶ Vercel
                                                │ • write events  ─────▶ │── PUT events ─▶ Vercel
                                                └──────────┬─────────────┘
                                                           │  each arrow = a billed broker hop
                                                           ▼
                                                  Vercel storage (event log)
```

```
                  OPEN-WORKFLOW WORLD-REDIRECT
                  ────────────────────────────

  ┌─────────────┐         ┌─────────────────────────────┐
  │ start(wf)   │ ──────▶ │  world.queue()              │
  │ (cli/HTTP)  │         │  ZADD jobs <runAtMs> <msgA> │
  └─────────────┘         └────────────┬────────────────┘
                                       │
                                       ▼
                          ┌──────────────────────────────┐
                          │     Redis (yours, BYO)       │
                          │  • CBOR entity blobs         │
                          │  • ZSET scheduler by runAtMs │
                          │  • LIST event log per run    │
                          │  • PUB/SUB for live streams  │
                          └────────────┬─────────────────┘
                                       │ in-process dispatcher polls
                                       │ ZRANGEBYSCORE due jobs
                                       ▼
                  POST  /.well-known/workflow/v1/flow?msg=A
                                       │
                                       ▼
                  ┌──────────────────────────────────────┐
                  │ flow handler                         │
                  │ • read msg A from Redis              │
                  │ • execute workflow body              │
                  │ • write events to Redis              │
                  │ • next durable jump = msg B          │
                  │ • return 307 → ?msg=B&hop=1  ────────│──┐
                  └──────────────────────────────────────┘  │
                                                            │ fetch auto-follows
                                                            │ 307 keeps method + body
                                                            ▼
                  POST  /.well-known/workflow/v1/flow?msg=B&hop=1
                                       │
                                       ▼
                          (same handler instance, msg B)
                              writes events, maybe
                              307 → msg C → msg D …
                                       │
                                       ▼
                                  200 OK (chain ends)
```

---

## 1.4 Comparison matrix

| dimension | Vercel WDK | `world-redirect` |
| --- | --- | --- |
| **queue** | managed, billed per message | Redis ZSET keyed by `runAtMs` |
| **storage** | managed Vercel store | Redis CBOR blobs + ZSET / LIST indexes |
| **streams** | managed pub/sub | Redis pub/sub (live) + LIST replay buffer |
| **dispatch** | broker → app HTTP per event | in-process poll + **307 trampoline** |
| **hops per chained workflow** | N broker round-trips | 1 dispatcher round-trip + N internal 307s |
| **idempotency model** | at-least-once via broker | at-least-once via Redis lease + event-log replay |
| **cost** | per-message billing | flat Redis bill |
| **portability** | Vercel only | any Redis (Upstash REST, ElastiCache, self-hosted, in-memory) |
| **author API** | `"use workflow"` / `"use step"` / `step()` / `sleep()` / `createHook()` | **identical** — same `workflow` package, same SWC plugin, same directives |
| **dashboard** | their hosted UI | `@workflow/web` pointed at the same Redis |
| **multi-host** | yes (broker handles it) | yes, with single-dispatcher recommendation per environment |
| **encryption** | provider-managed | your Redis (TLS + at-rest is your call) |

---

## 1.5 Why 307 specifically

The HTTP redirect family has several status codes:

| status | preserves method | preserves body | follows automatically | notes |
| --- | --- | --- | --- | --- |
| 301 Moved Permanently | ❌ legacy clients downgrade to GET | ❌ | yes | cacheable |
| 302 Found | ❌ same | ❌ | yes | the messy default |
| 303 See Other | ❌ explicit "use GET" | ❌ | yes | for POST-redirect-GET patterns |
| **307 Temporary Redirect** | **✅ MUST preserve** | **✅ MUST preserve** | yes | RFC 7231 §6.4.7 |
| 308 Permanent Redirect | ✅ | ✅ | yes | cacheable — bad for us |

The trampoline needs the **same HTTP method + the same body** to land on
the same flow handler, picking up the next durable message. Only 307 and
308 satisfy that, and 308's cacheability would cause CDNs / proxies to
short-circuit subsequent jumps to the previous target — so 307 is the
unique correct choice.

Inside Node, `fetch` (whether `undici`, the global, or a polyfill)
follows 307 automatically without re-rewriting the request — exactly the
behaviour we need. Browsers and most HTTP libraries do the same.

---

## 1.6 What stays the same for the author

Your application code does **not** change between Vercel-WDK and
`world-redirect`. The same SWC plugin compiles the same directives into
the same flow/step bundles. The same `workflow` package exports
`step()`, `sleep()`, `createHook()`, `getRun()`, `FatalError`,
`RetryableError`. The same `@workflow/web` dashboard renders the same
views.

What changes is exactly one environment variable:

```bash
# before
WORKFLOW_TARGET_WORLD=@workflow/world-vercel

# after
WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect
WORKFLOW_REDIS_REST_URL=...
WORKFLOW_REDIS_REST_TOKEN=...
```

That's the entire migration on the author side.

---

## 1.7 When to use this

**Good fit:**

- You want durable workflows but don't want Vercel managed infra.
- You already operate a Redis (Elasticache, Upstash, self-hosted).
- You're deploying to a platform where the Vercel broker isn't
  available (EdgeOne, Cloudflare Pages, OpenNext targets, your own
  Kubernetes).
- You want a flat-cost backend with the same author DX as the WDK.

**Acceptable fit, with caveats:**

- High-throughput workloads — Redis is fast, but the in-process
  dispatcher polling model is single-pump; if you have one host serving
  many concurrent workflows, dispatch throughput is bounded by your
  Redis op latency × poll batch size.
- Multi-region — `world-redirect` itself is region-agnostic, but a
  single dispatcher per Redis is recommended. Run dispatchers in one
  region and let workflow handlers run anywhere.

**Bad fit:**

- "I want managed everything." Use Vercel WDK as-is.
- "I want sub-millisecond dispatch latency." A poll-based dispatcher
  with a 1s default tick isn't going to get there; reduce the poll
  interval, but at scale you'll see Redis CPU rise.
- "I need cross-host transactional consistency across multiple
  dispatchers." The current model assumes one dispatcher per env;
  multi-dispatcher needs explicit partitioning.

---

## 1.8 One more sentence

If the WDK is "a programming model where your durable substrate is
hidden", `world-redirect` is "the WDK's programming model with the
durable substrate explicitly being a Redis you already own, and dispatch
collapsed into the same HTTP request chain whenever it can be".

The rest of this reference explains exactly how that collapse works.

---

[← Index](./README.md) · [Next: 02 Architecture →](./02-architecture.md)
