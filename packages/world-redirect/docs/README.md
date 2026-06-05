# `world-redirect` — Engineering Reference

> Vendor-agnostic, Redis-backed `World` implementation for the Workflow
> Development Kit, using a **307-redirect trampoline** as the dispatch
> mechanism.

This directory is the deep-dive engineering reference for
`@open-workflow/world-redirect`. Each page is focused on one subsystem and
heavy on annotated ASCII diagrams. Read sequentially the first time;
afterwards each page works as a stand-alone reference.

---

## Reading order

| # | Page | What you'll learn |
| --- | --- | --- |
| 01 | [Overview](./01-overview.md) | Why this exists, the central trick (Redis + 307), comparison to the Vercel WDK runtime. |
| 02 | [Architecture](./02-architecture.md) | Layers (Author / Runtime / World / Redis), components, world lifecycle, concurrency model. |
| 03 | [The World contract](./03-world-contract.md) | The `World = Queue & Storage & Streamer` interface that the WDK runtime expects, method by method. |
| 04 | [Redis keyspace](./04-redis-keyspace.md) | Every key shape, what it stores, what indexes it, when it's deleted. |
| 05 | [Event sourcing](./05-event-sourcing.md) | The 16 event types, the run/step/hook/wait state machines, how entity blobs are derived. |
| 06 | [307 dispatch](./06-307-dispatch.md) | The dispatcher poll loop, the trampoline mechanism (bytes on the wire), hop counter, multi-host behaviour. |
| 07 | [End-to-end flow](./07-end-to-end-flow.md) | Full time-axis walkthrough of one workflow with steps, streams, sleep, retry, completion. |
| 08 | [Failure & retry](./08-failure-and-retry.md) | Error taxonomy (`FatalError` vs `RetryableError`), backoff formula, lease expiry, terminal cleanup. |
| 09 | [Hooks & streams](./09-hooks-and-streams.md) | Hook token NX-claim, `hook_conflict` fallback, webhook resume, live streaming via pub/sub + replay buffer. |
| 10 | [Clients & configuration](./10-clients-and-config.md) | `UpstashRedisClient` / `NodeRedisClient` / `MemoryRedisClient`, env-var resolution, programmatic config. |
| 11 | [Deployment](./11-deployment.md) | Standalone host, Next.js, EdgeOne / OpenNext, running the `@workflow/web` dashboard. |

---

## Map of the territory

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              YOU ARE HERE                                │
│                                                                          │
│   01 Overview  ────────▶  Why & comparison to Vercel WDK                │
│   02 Architecture ─────▶  Layers + components + lifecycle               │
│       │                                                                  │
│       ▼                                                                  │
│   03 World contract                                                      │
│       │                                                                  │
│       ├──▶  04 Redis keyspace        (the substrate)                    │
│       ├──▶  05 Event sourcing        (the source-of-truth model)        │
│       └──▶  06 307 dispatch          (the engine)                       │
│              │                                                          │
│              ▼                                                          │
│           07 End-to-end flow         (it all together)                  │
│              │                                                          │
│              ├──▶  08 Failure & retry                                   │
│              ├──▶  09 Hooks & streams                                   │
│              ├──▶  10 Clients & configuration                           │
│              └──▶  11 Deployment                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Audience & assumptions

This reference assumes you:

- Know what the **Vercel Workflow Development Kit (WDK)** is at a high
  level (`"use workflow"` / `"use step"` directives, `step()`, `sleep()`,
  `createHook()`, durable runs).
- Are comfortable with Redis primitives (HASH, LIST, ZSET, PUB/SUB, NX
  semantics).
- Read TypeScript at a reference level.

You **don't** need to have read the WDK source. Each page links back to
the relevant upstream concept when it matters.

---

## How the diagrams work

```
┌── plain box           = a process / function / module
│   inner text          = its responsibility
└── ──────────────

────▶  arrow             = control or data flow
──────▶  arrow with body = HTTP request / Redis command

  ZADD jobs … msgX       = literal Redis command (post-prefix shown without prefix)
  POST /…/flow?msg=X     = literal HTTP wire format

  ╳ ╳ ╳ wait ╳ ╳ ╳        = time gap / sleep / backoff
```

Long horizontal diagrams use a leftmost time / actor column and stagger
events so a reader can follow rows top-down for the chronological story.

---

## Companion documents

- `../README.md` — package-level README (install + quick start).
- `../ARCHITECTURE.md` — single-page condensed version of this reference,
  useful as a print-out / sharing summary.
- Upstream WDK source: <https://github.com/vercel/workflow>.

---

## Conventions

- Code citations use `path/to/file.ts:LINE` format.
- "the dispatcher" = the in-process scheduler poll loop in
  `world-redirect`'s queue module. Not the Vercel queue broker.
- "the chain" = a single HTTP request that, via 307s, advances multiple
  durable jumps inside one function invocation.
- "the trampoline" = the 307-redirect dispatch mechanism — the central
  idea of this implementation.
- "owf" = `WORKFLOW_REDIS_KEY_PREFIX`, the default Redis key prefix. All
  example keys use it; replace with your own prefix in production.

---

Next: [01 Overview →](./01-overview.md)
