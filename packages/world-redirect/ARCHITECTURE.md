# `world-redirect` — architecture & diagrams

A complete visual reference for how `@open-workflow/world-redirect`
implements the Workflow Development Kit's `World` contract on top of
plain Redis using a **307-redirect trampoline** for dispatch.

## Contents

1. [The shape of the problem](#1-the-shape-of-the-problem-vercel-wdk-vs-world-redirect)
2. [Why 307 specifically](#2-why-307-specifically)
3. [Moving parts](#3-moving-parts)
4. [Redis keyspace](#4-redis-keyspace-prefix-owf-by-default)
5. [End-to-end lifecycle](#5-end-to-end-lifecycle-of-one-workflow)
6. [The 307 trampoline — bytes on the wire](#6-the-307-trampoline--bytes-on-the-wire)
7. [Sleep & wake](#7-sleep--wake)
8. [Failure & retry](#8-failure--retry-path)
9. [Hook / webhook resume](#9-hook--webhook-resume)
10. [Streams](#10-stream-lifecycle)
11. [Dashboard read path](#11-dashboard-read-path-workflowweb)
12. [Five rules that explain everything](#12-five-rules-that-explain-everything)

---

## 1. The shape of the problem — Vercel WDK vs `world-redirect`

The author-facing API is identical. What changes is the substrate.

### Vercel WDK (the managed path)

```
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
                                                           │ each arrow = a billed broker hop
                                                           ▼
                                                  Vercel storage (event log)
```

### `world-redirect`

```
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

### Side-by-side

| dimension | Vercel WDK | `world-redirect` |
| --- | --- | --- |
| **queue** | managed, billed per message | Redis ZSET keyed by `runAt` |
| **storage** | managed Vercel store | Redis CBOR blobs + indexes |
| **streams** | managed pub/sub | Redis pub/sub (live) + list fallback (replay) |
| **dispatch** | broker → app HTTP per event | poll + **307 trampoline** — many events per HTTP chain |
| **hops per chained workflow** | N broker round-trips | 1 dispatcher round-trip, N internal 307s |
| **idempotency model** | at-least-once via broker | at-least-once via Redis lease + event-log replay |
| **cost** | per-message billing | flat Redis bill |
| **portability** | Vercel only | any Redis (Upstash REST, ElastiCache, self-hosted, in-memory) |
| **author API** | `"use workflow"` / `"use step"` / `step()` / `sleep()` / `createHook()` | **identical** — same `workflow` package, same SWC plugin |
| **dashboard** | their hosted UI | `@workflow/web` pointed at the same Redis |

---

## 2. Why 307 specifically

`307 Temporary Redirect` is the one redirect status code that **must
preserve the original method and body** (RFC 7231 §6.4.7). `fetch`
follows it by default. That means when the flow handler finishes one
durable jump and a follow-up jump is ready immediately (no sleep, no
delay), it can redirect to itself with the next `?msg=` and the same
TCP connection / function invocation carries through — instead of
bouncing back to the dispatcher for each step, paying queue latency and
(on Vercel) per-message cost. The dispatcher is only on the path for
*delayed* work (sleeps, retry backoff) — anything ready right now stays
inside the chain.

Net effect: a 5-step workflow with no `sleep` becomes **one** dispatcher
tick + four free in-chain 307s, vs **five** broker round-trips on
Vercel.

---

## 3. Moving parts

```
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                          AUTHOR LAYER (your code)                        │
 │                                                                          │
 │   "use workflow"           "use step"            createHook()            │
 │   workflow body            step(name, fn)        sleep(ms) sleepUntil()  │
 │       │                         │                      │                 │
 │       └──────────┬──────────────┴──────────────────────┘                 │
 │                  ▼                                                       │
 │         @workflow SWC plugin compiles to                                 │
 │         flow bundle + step bundle  + manifest.json                       │
 └──────────────────────┬───────────────────────────────────────────────────┘
                        │
 ┌──────────────────────▼───────────────────────────────────────────────────┐
 │                    RUNTIME LAYER (@workflow/core)                        │
 │                                                                          │
 │   POST /.well-known/workflow/v1/flow      ◀── invoke / resume workflow   │
 │   POST /.well-known/workflow/v1/step      ◀── invoke a step              │
 │   POST /.well-known/workflow/v1/webhook   ◀── hook resume from outside   │
 │                                                                          │
 │              loads world via WORKFLOW_TARGET_WORLD ───┐                  │
 └────────────────────────────────────────────────────────┼─────────────────┘
                                                          ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                    @open-workflow/world-redirect                         │
 │                                                                          │
 │  ┌────────────┐   ┌──────────────────────┐    ┌──────────────────────┐   │
 │  │  Storage   │   │  Queue + Scheduler   │    │  Streamer            │   │
 │  │  event-    │   │  (ZSET by runAt) +   │    │  pub/sub live tail   │   │
 │  │  sourced   │   │  in-process 307      │    │  LIST replay buffer  │   │
 │  └─────┬──────┘   │  dispatcher          │    └──────────┬───────────┘   │
 │        │          └──────────┬───────────┘               │               │
 │        └──────────────┬──────┴──────────────┬────────────┘               │
 │                       ▼                     ▼                            │
 │           ┌─────────────────────────────────────────────────┐            │
 │           │           RedisClient (3 backends)              │            │
 │           │   • UpstashRedisClient (REST)                   │            │
 │           │   • NodeRedisClient    (RESP / TCP)             │            │
 │           │   • MemoryRedisClient  (in-process dev)         │            │
 │           └────────────────────┬────────────────────────────┘            │
 └────────────────────────────────┼─────────────────────────────────────────┘
                                  ▼
                              R E D I S
```

---

## 4. Redis keyspace (prefix `owf` by default)

```
ENTITY BLOBS                                            SHAPE   CONTENT
──────────────────────────────────────────────────────  ─────   ─────────────────────────
owf:run:<runId>                                         HASH    CBOR(WorkflowRun) + meta
owf:step:<runId>:<stepId>                               HASH    CBOR(Step) + meta
owf:hook:<hookId>                                       HASH    CBOR(Hook)
owf:wait:<runId>:<correlationId>                        HASH    CBOR(Wait)

INDEXES (for list queries)
──────────────────────────────────────────────────────
owf:runs:all                                            ZSET    runId   score=createdAt
owf:runs:status:<status>                                ZSET    runId   score=createdAt
owf:steps:run:<runId>                                   ZSET    stepId  score=createdAt
owf:hooks:run:<runId>                                   ZSET    hookId  score=createdAt

EVENT LOG (event-sourcing primary store)
──────────────────────────────────────────────────────
owf:event:<runId>:<eventId>                             STRING  CBOR(Event)
owf:events:run:<runId>                                  LIST    eventIds in append order
owf:events:corr:<correlationId>                         LIST    eventIds for steps/hooks lookup

HOOK TOKEN CLAIM (atomic NX)
──────────────────────────────────────────────────────
owf:hook:tok:<sha256(token)>                            STRING  hookId — SETNX wins,
                                                                losers get hook_conflict event

SCHEDULER (the heart of dispatch)
──────────────────────────────────────────────────────
owf:jobs                                                ZSET    messageId  score=runAtMs
owf:msg:<messageId>                                     STRING  CBOR(QueuePayload) — durable job body
owf:msg:<messageId>:lease                               STRING  workerId  EX=30s
                                                                (claim — prevents double-dispatch)

STREAMS
──────────────────────────────────────────────────────
owf:stream:<runId>:<name>:chunks                        LIST    base64(bytes) per chunk (replay)
owf:stream:<runId>:<name>:done                          STRING  "1" when closed
owf:stream:<runId>:<name>:channel                       PUB/SUB live chunk fan-out
owf:stream:<runId>                                      SET     stream names for this run
```

---

## 5. End-to-end lifecycle of one workflow

A workflow that does: **start → step("greet") → stream("Hello") → sleep(30s) → step("farewell") → complete**.

```
TIME   CALLER / DISPATCHER          FLOW HANDLER  /.well-known/workflow/v1/flow            REDIS
─────  ────────────────────────     ─────────────────────────────────────────────────       ────────────────────

 t0    workflow.start("hello")
         │
         ▼
       world.events.create(
         null, {run_created, ...}) ─────────────────────────────────────────────────▶  WRITE event:<runId>:<evtA>
                                                                                       HSET run:<runId> status=pending
                                                                                       RPUSH events:run:<runId> evtA
                                                                                       ZADD runs:status:pending …

         ▼
       world.queue(
         __wkf_workflow_hello,
         {runId})              ─────────────────────────────────────────────────────▶  SET  msg:<msgA> CBOR(payload)
                                                                                       ZADD jobs <t0> msgA

 t0+ε  dispatcher tick:
       ZRANGEBYSCORE jobs 0 now → [msgA]
       SET msg:<msgA>:lease … EX 30  (NX, wins)
         │
         ▼
       POST /flow?msg=msgA ───────▶┌──────────────────────────────────────────────┐
                                    │ flow handler — chain start                   │
                                    │                                              │
                                    │ • GET msg:<msgA>                             │
                                    │ • events.create(run_started)  ───────────────│─▶ WRITE event run_started
                                    │                                              │   HSET run:<runId> status=running
                                    │                                              │
                                    │ • run workflow body, hits step("greet")      │
                                    │ • events.create(step_created)  ──────────────│─▶ event:step_created
                                    │ • events.create(step_started)                │   step:<runId>:greet status=running
                                    │ • execute step → "Hello, World"              │
                                    │ • streams.write(runId,"out","Hello")  ───────│─▶ RPUSH stream chunks
                                    │                                              │   PUBLISH stream channel
                                    │ • events.create(step_completed)              │   step status=completed
                                    │                                              │
                                    │ • next durable jump = msgB (immediate)       │
                                    │   world.queue() → msg:<msgB>, ZADD jobs ─────│─▶ SET msg:<msgB>, ZADD jobs now msgB
                                    │                                              │
                                    │ • respond 307                                │
                                    │   Location: /flow?msg=msgB&hop=1             │
                                    └──────────────────┬───────────────────────────┘
                                                       │
                                       fetch follows 307 (POST + body preserved)
                                                       │
                                                       ▼
                                    POST /flow?msg=msgB&hop=1
                                                       │
                                                       ▼
                                    ┌──────────────────────────────────────────────┐
                                    │ flow handler — chain continues               │
                                    │ (same TCP / same function invocation)        │
                                    │                                              │
                                    │ • GET msg:<msgB>                             │
                                    │ • replay event log to current position       │
                                    │ • workflow body resumes after "greet"        │
                                    │ • hits sleep(30s)                            │
                                    │ • events.create(wait_created,                │
                                    │     resumeAt=t1+30s)  ───────────────────────│─▶ event:wait_created
                                    │                                              │   wait:<runId>:<corr>
                                    │ • enqueue msgC delayed 30s                   │
                                    │   ZADD jobs (t1+30s) msgC ───────────────────│─▶ SET msg:<msgC>, ZADD jobs t+30 msgC
                                    │                                              │
                                    │ • respond 200 OK (no 307 — delayed)          │
                                    └──────────────────┬───────────────────────────┘
                                                       │
                                                       ▼
                                    request chain ENDS — function released
                                    DEL msg:<msgB>:lease
                                    DEL msg:<msgB>

       ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ 30 seconds — $0 of compute ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳

 t1+30s dispatcher tick:
        ZRANGEBYSCORE jobs 0 now → [msgC]
        SET msg:<msgC>:lease … EX 30
          │
          ▼
        POST /flow?msg=msgC ─────▶ ┌──────────────────────────────────────────────┐
                                    │ flow handler — chain start (new invocation) │
                                    │                                              │
                                    │ • GET msg:<msgC>                             │
                                    │ • replay event log → know we slept           │
                                    │ • events.create(wait_completed)              │─▶ event:wait_completed
                                    │ • workflow body resumes after sleep          │   wait status=completed
                                    │                                              │
                                    │ • hits step("farewell")                      │
                                    │ • step_created / step_started / step_completed
                                    │                                              │
                                    │ • workflow returns → events.create(          │
                                    │     run_completed, {output})  ───────────────│─▶ event:run_completed
                                    │                                              │   run status=completed
                                    │                                              │
                                    │ • streams.close()                            │─▶ SET stream:<runId>:<name>:done = "1"
                                    │                                              │   PUBLISH stream channel (EOF)
                                    │                                              │
                                    │ • respond 200 OK                             │
                                    └──────────────────┬───────────────────────────┘
                                                       │
                                                       ▼
                                    DEL msg:<msgC>:lease, DEL msg:<msgC>
                                    terminal cleanup: drop hooks, waits for runId
```

---

## 6. The 307 trampoline — bytes on the wire

```
 Dispatcher → flow (initial)
 ──────────────────────────────────────────────────────────────────
   POST /.well-known/workflow/v1/flow?msg=msgA  HTTP/1.1
   host: my-app.example.com
   content-type: application/json
   x-vqs-queue-name: __wkf_workflow_hello
   x-vqs-message-id: msgA
   x-vqs-message-attempt: 1
   (empty body — the durable job payload lives in Redis at msg:<msgA>)

 Flow handler → fetch (chain continues)
 ──────────────────────────────────────────────────────────────────
   HTTP/1.1 307 Temporary Redirect
   location: /.well-known/workflow/v1/flow?msg=msgB&hop=1
   (no body)

       ┌─────────────────────────────────────────────────────────┐
       │ Why 307 (and not 302/303/308)                           │
       │   - RFC 7231 §6.4.7: method + body MUST be preserved    │
       │   - fetch follows automatically                          │
       │   - 308 would also work but caches more aggressively    │
       │   - 302/303 may downgrade to GET on legacy clients      │
       └─────────────────────────────────────────────────────────┘

 fetch retransmits with the new path
 ──────────────────────────────────────────────────────────────────
   POST /.well-known/workflow/v1/flow?msg=msgB&hop=1  HTTP/1.1
   …same headers / empty body…

   hop counter prevents runaway loops.
   default safety cap: 32 hops, then flow returns 200 → next jump goes
   through the dispatcher even though it's immediate.
```

---

## 7. Sleep & wake

```
  workflow body
       │
       ▼
   sleep(30s)                                     ┌─────────────────┐
       │                                          │  Redis ZSET     │
       │   writes wait_created event   ───────▶   │  jobs:          │
       │   ZADD jobs (now+30s) msgB    ───────▶   │   ...            │
       │                                          │   now+30s → msgB │
       │                                          └────────┬────────┘
       ▼                                                   │
   return 200 — request chain ENDS                         │
   function invocation released ($0 during sleep)          │
                                                            │
       ╳ ╳ ╳ ╳ ╳ ╳ 30 seconds pass ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ │
                                                            │
                          dispatcher poll                   │
                          ZRANGEBYSCORE 0 now      ◀────────┘
                                       │
                                       ▼
              POST /.well-known/workflow/v1/flow?msg=B
                                       │
                                       ▼
                          flow handler resumes
                          (replays event log to sleep point)
                          continues workflow body
                          maybe 307 → msgC → msgD …
```

`sleep` durably persists in Redis — the function isn't running and no
compute is charged for the wait. The same machinery powers
`sleepUntil()`, retry backoff, and step timeouts.

---

## 8. Failure & retry path

```
   step throws RetryableError  /  handler returns 5xx
                  │
                  ▼
   events.create(step_failed)              ───▶  event:step_failed
                                                  step status=failed (transient)
                  │
                  ▼
   compute backoff:
     ≤ 6 attempts → fixed retryBaseMs
     > 6 attempts → base * 2^(attempt-6)  (cap 5 min)
                  │
                  ▼
   re-enqueue same msg, push runAt forward
     SET    msg:<msgB> (already there)
     ZADD   jobs (now + backoff) msgB
                  │
                  ▼
   respond 500 (chain ends)
   DEL msg:<msgB>:lease         (so dispatcher can claim again)

   ╳ ╳ ╳  backoff window  ╳ ╳ ╳

   dispatcher tick → claims msgB again
   x-vqs-message-attempt: N+1
                  │
                  ▼
   flow handler:
     • events.create(step_retrying)        ───▶  event:step_retrying
     • replay event log up to last step_failed
     • re-run the step body                       (event-sourced replay → idempotent)
                  │
                  ▼
   exceeded maxAttempts?
     YES → events.create(run_failed)  ───▶  event:run_failed
                                            run status=failed (terminal)
                                            DEL msg:<msgB>, ZREM jobs msgB
     NO  → either succeeds (continue chain) or fails again (loop above)
```

---

## 9. Hook / webhook resume

```
   In-workflow:
     const hook = await createHook()                    ┌──────────────────────────┐
            │                                            │  events.create(          │
            ▼                                            │    hook_created,         │
   events.create(hook_created, {token,…})  ────────────▶ │    token, isWebhook)     │
            │                                            │  SET tok:<sha256> hookId │
            │                                            │  (NX — atomic claim)     │
            │                                            │  WRITE hook:<hookId>     │
            │                                            └──────────────────────────┘
            │
            │  (token contains <hookId>, returned to caller)
            ▼
   workflow body suspends (no msg enqueued — waiting on external trigger)
   chain ENDS

   ─── external system POSTs the webhook URL ───

   POST /.well-known/workflow/v1/webhook/<token>     (or mirror /api/wf/webhook/<token>)
            │
            ▼
   webhook handler:
     • GET tok:<sha256(token)> → hookId
     • GET hook:<hookId> → {runId, …}
     • events.create(hook_received, {payload}) ─────▶  event:hook_received
     • enqueue msg for the run → ZADD jobs now msg
     • respond 200
            │
            ▼
   dispatcher picks up the msg → flow handler resumes the run from the hook
```

---

## 10. Stream lifecycle

```
   Writer (inside flow handler)
   ───────────────────────────
     world.streams.write(runId, "out", "Hello")
       │
       ▼
     RPUSH stream:<runId>:out:chunks  base64("Hello")
     SADD  stream:<runId>             "out"
     PUB   stream:<runId>:out:channel <chunkIdx,bytes>

     ... many writes ...

     world.streams.close(runId, "out")
       │
       ▼
     SET stream:<runId>:out:done "1"
     PUB stream:<runId>:out:channel <EOF marker>

   Reader (dashboard / RPC client)
   ───────────────────────────────
     world.streams.get(runId, "out")
       │
       ├─▶ LRANGE chunks for any history before subscribe
       └─▶ SUBSCRIBE stream:<runId>:out:channel for live tail
           (Upstash REST: falls back to polling LRANGE instead)
```

---

## 11. Dashboard read path (`@workflow/web`)

```
   browser ──▶ @workflow/web Express server (port 4000)
                       │
                       ▼
              loadWorld(WORKFLOW_TARGET_WORLD) → world-redirect
                       │
              world.runs.list / .get        ──▶ HGETALL run:* / ZRANGE runs:*
              world.steps.list / .get       ──▶ HGETALL step:*  / ZRANGE steps:*
              world.events.list             ──▶ LRANGE events:run:* + MGET event:*
              world.streams.get             ──▶ LRANGE + SUBSCRIBE

   (Set WORKFLOW_REDIS_DISABLE_DISPATCHER=1 — dashboard is a reader,
    you don't want it racing the real dispatcher for jobs.)
```

---

## 12. Five rules that explain everything

1. **The durable substrate is Redis.** Every entity, every event, every
   queued job, every stream chunk is a key. Restarting any function
   invocation loses nothing.
2. **The event log is the source of truth.** Entity blobs (`run:*`,
   `step:*`) are materialised views over `event:*`. Replay = re-derive.
3. **The scheduler is one ZSET (`jobs`) scored by `runAtMs`.** All
   sleep, retry, delay, future-dated work go here.
   `ZRANGEBYSCORE 0 now` is "what's due".
4. **307 is the cheap path.** If the very next durable jump fires *now*,
   return a 307 to yourself — same TCP, same function, no broker
   round-trip. Used for chained steps, immediate fan-out continuations,
   hook-resumed runs.
5. **Anything delayed exits the chain.** `sleep`, retry backoff,
   scheduled timeouts, future-dated `step({delay})` — all park in the
   ZSET and resume in a brand-new chain when the dispatcher polls them
   due.
