# 04 · Redis keyspace

[← 03 World contract](./03-world-contract.md) · [Index](./README.md) · [Next: 05 Event sourcing →](./05-event-sourcing.md)

---

Every durable thing `world-redirect` does lives in Redis. This page
enumerates every key shape: its purpose, its Redis data type, the
operations that read or write it, and the lifecycle that creates and
eventually retires it.

> **Prefix**: all keys are prefixed with `WORKFLOW_REDIS_KEY_PREFIX`
> (default `owf`) plus a `:`. This page uses `owf:` literally for
> clarity; in your deployment substitute your prefix.

---

## 4.1 Tree of keyspace

```
owf:                                                                 (prefix root)
│
├── ENTITIES (materialised views of the event log)
│   ├── run:<runId>                          HASH
│   ├── step:<runId>:<stepId>                HASH
│   ├── hook:<hookId>                        HASH
│   └── wait:<runId>:<correlationId>         HASH
│
├── INDEXES (for paginated list queries)
│   ├── runs:all                             ZSET   score=createdAt
│   ├── runs:status:<status>                 ZSET   score=createdAt
│   ├── steps:run:<runId>                    ZSET   score=createdAt
│   └── hooks:run:<runId>                    ZSET   score=createdAt
│
├── EVENT LOG (source of truth)
│   ├── event:<runId>:<eventId>              STRING (CBOR)
│   ├── events:run:<runId>                   LIST   eventIds, append order
│   └── events:corr:<correlationId>          LIST   eventIds for steps/hooks
│
├── HOOK TOKEN CLAIM (atomic NX semantics)
│   └── hook:tok:<sha256(token)>             STRING hookId
│
├── SCHEDULER (the queue + dispatch state)
│   ├── jobs                                 ZSET   score=runAtMs
│   ├── msg:<messageId>                      STRING CBOR(QueuePayload)
│   └── msg:<messageId>:lease                STRING workerId, TTL ~30s
│
└── STREAMS
    ├── stream:<runId>                       SET    stream names per run
    ├── stream:<runId>:<name>:chunks         LIST   base64(bytes)
    ├── stream:<runId>:<name>:done           STRING "1" when closed
    └── stream:<runId>:<name>:channel        PUBSUB live chunk fanout
```

---

## 4.2 Entity hashes

These are the **materialised views** of the event log. The event log is
the source of truth; entity hashes exist so that point reads (and the
dashboard's list pages) don't have to re-replay events on every load.

### 4.2.1 `owf:run:<runId>` (HASH)

| field | type | example | description |
| --- | --- | --- | --- |
| `runId` | string | `wrun_01K…` | ULID-prefixed run id |
| `workflowName` | string | `workflow//./app/wf/hello//greet` | the SWC-emitted workflow ident |
| `status` | enum | `pending` / `running` / `completed` / `failed` / `cancelled` | current state |
| `deploymentId` | string | `dpl_local` | from `getDeploymentId()` at creation |
| `specVersion` | int | `2` | event-sourcing protocol version |
| `errorCode` | string? | `STEP_FATAL` | on failure |
| `createdAtIso` | iso | `2026-06-02T19:50:00Z` | first event |
| `startedAtIso` | iso? | | first `run_started` |
| `completedAtIso` | iso? | | terminal transition |
| `expiredAtIso` | iso? | | TTL expiry (optional) |
| `attributesJson` | json | `{}` | user-attached metadata |
| `inputB64` | base64 | … | CBOR(input) |
| `outputB64` | base64 | … | CBOR(output) — only after `run_completed` |
| `errorB64` | base64 | … | CBOR(error) — only after `run_failed` |
| `executionContextJson` | json | `{deploymentId, …}` | resolved at start time |

**Written by**: `events.create({eventType: run_*})`.
**Read by**: `runs.get`, `runs.list`, the dashboard, every flow-handler
invocation (to enforce terminal-state guards).

**Lifecycle**:

```
   run_created    ──▶  HSET run:<runId>  status=pending,  inputB64=…
   run_started    ──▶  HSET run:<runId>  status=running, startedAt=…
   run_completed  ──▶  HSET run:<runId>  status=completed, outputB64=…
   run_failed     ──▶  HSET run:<runId>  status=failed,    errorB64=…
   run_cancelled  ──▶  HSET run:<runId>  status=cancelled
```

### 4.2.2 `owf:step:<runId>:<stepId>` (HASH)

```
   runId  stepId  stepName  status  attempt
   createdAtIso  startedAtIso  completedAtIso  retryAfterIso
   inputB64  outputB64  errorB64  specVersion
```

**Lifecycle**:

```
   step_created   ──▶  status=pending,  attempt=0
   step_started   ──▶  status=running,  attempt+=1, startedAt=now
   step_completed ──▶  status=completed, outputB64=...
   step_failed    ──▶  status=failed,    errorB64=...
   step_retrying  ──▶  status=pending,   retryAfter=...
```

### 4.2.3 `owf:hook:<hookId>` (HASH)

```
   hookId  runId  token  ownerId  projectId  environment
   isWebhook  isSystem  specVersion  createdAtIso  metadataB64
```

**Lifecycle**:

```
   hook_created  ──▶  HSET hook:<hookId>  …          (after winning the token NX claim)
   hook_disposed ──▶  DEL  hook:<hookId>
                       DEL  hook:tok:<sha256(token)>  (release the claim)
                       ZREM hooks:run:<runId>  <hookId>
```

### 4.2.4 `owf:wait:<runId>:<correlationId>` (HASH)

```
   waitId  runId  correlationId  status  resumeAtIso  completedAtIso
   createdAtIso  specVersion
```

Two flavours of wait:

- **sleep wait** — `correlationId = the sleep ulid`, `resumeAt = now + delay`
- **hook wait** — `correlationId = the hookId`, `resumeAt = undefined`

**Lifecycle**:

```
   wait_created   ──▶  HSET wait:<runId>:<corr>  status=waiting, resumeAt=…
   wait_completed ──▶  HSET wait:<runId>:<corr>  status=completed, completedAt=…
```

---

## 4.3 Indexes

```
   owf:runs:all              ZSET    score=createdAt    member=runId
   owf:runs:status:<status>  ZSET    score=createdAt    member=runId
   owf:steps:run:<runId>     ZSET    score=createdAt    member=stepId
   owf:hooks:run:<runId>     ZSET    score=createdAt    member=hookId
```

**Why ZSET, not LIST**: status indexes need cheap re-ordering when a run
transitions states (e.g. `pending` → `running`), and the dashboard pages
need range queries (`runs.list({status: 'running', pagination: {…}})`).
ZSET gives both: `ZADD` moves a member's score atomically and `ZRANGE`
supports limit/offset cleanly.

**Status transitions update the indexes:**

```
   run goes pending → running
        │
        ▼
   ZREM runs:status:pending <runId>
   ZADD runs:status:running <createdAt> <runId>
   ZADD runs:all            <createdAt> <runId>   (no-op if already present)
```

---

## 4.4 Event log

This is the durable source of truth. Materialised entities are derived
views; everything else can be rebuilt from the event log.

### 4.4.1 `owf:event:<runId>:<eventId>` (STRING — CBOR blob)

```
   GET → CBOR(Event {
     eventId: "evnt_01K…",
     runId:   "wrun_01K…",
     eventType: "step_completed",
     correlationId?: "step1",
     specVersion: 2,
     createdAt: Date,
     eventData?: { ... }
   })
```

Events are **append-only**. Once written, never modified.

### 4.4.2 `owf:events:run:<runId>` (LIST)

The append-only log of event ids for a single run. Used for sequential
replay.

```
   RPUSH events:run:<runId> <eventId>
```

Read with `LRANGE 0 -1` (or paginated ranges).

### 4.4.3 `owf:events:corr:<correlationId>` (LIST)

A parallel index of events for a single correlationId — e.g. all events
about a specific step or hook. Used by
`events.listByCorrelationId(…)` (dashboard "step detail" view, hook
debugging).

```
   RPUSH events:corr:<correlationId> <eventId>
```

---

## 4.5 Hook token NX claim

```
   owf:hook:tok:<sha256(token)>   STRING   hookId
```

Webhook tokens must be globally unique within a deployment (otherwise
two runs could claim the same external URL). The claim mechanism is a
single Redis `SET … NX` against this key:

```
   events.create(runId, { eventType: 'hook_created', eventData: {token} })
        │
        ▼
   SET owf:hook:tok:<sha256(token)>  <hookId>  NX
        │
   ┌────┴────┐
   ok          nil  (someone else already claimed it)
   │           │
   ▼           ▼
   write hook   write hook_conflict event
   normally     return without creating the hook
```

The NX semantics give us atomic exactly-once claim with no locks. The
loser emits a `hook_conflict` event that the runtime turns into a clean
"this URL is already in use" surface.

---

## 4.6 Scheduler — the heart of dispatch

### 4.6.1 `owf:jobs` (ZSET, single key for the whole namespace)

```
   ZADD jobs <runAtMs>  <messageId>
   ...
   member=msg_01K…   score=1717459200000
   member=msg_01L…   score=1717459260000
   member=msg_01M…   score=1717459300000
```

The dispatcher's only query:

```
   ZRANGEBYSCORE jobs 0 <now> LIMIT 0 <batchSize>
   → [msg ids that are due]
```

Members live in the ZSET until the message is consumed (deleted) or the
job exceeds `maxAttempts` and is dropped.

### 4.6.2 `owf:msg:<messageId>` (STRING — CBOR)

The durable message payload that the scheduler can't fit in the ZSET
member field. Contains:

```
   CBOR({
     queueName: "__wkf_workflow_hello",
     route:     "flow",           // or "step"
     runId:     "wrun_01K…",
     attempt:   1,
     body:      { ... the runtime's QueuePayload ... }
   })
```

**Written by**: `queue()`.
**Read by**: the `createQueueHandler` wrapper when a request arrives.
**Deleted by**: the wrapper on successful or terminal failure.

### 4.6.3 `owf:msg:<messageId>:lease` (STRING with TTL)

The dispatch claim — prevents two dispatchers from racing the same
message.

```
   SET msg:<id>:lease <workerId> NX EX 30
        │
   ┌────┴────┐
   ok          nil
   │           │
   ▼           ▼
   we got      another dispatcher
   this msg    has it; skip
   (POST it)
```

TTL is the safety net: if a dispatcher crashes mid-dispatch, the lease
expires and another dispatcher can pick the message up.

---

## 4.7 Streams

### 4.7.1 `owf:stream:<runId>` (SET)

The names of all streams known for a run.

```
   SADD stream:<runId>  "out"
   SADD stream:<runId>  "logs"
   SADD stream:<runId>  "events"
```

**Read by**: `streams.list(runId)` (returns `SMEMBERS`).

### 4.7.2 `owf:stream:<runId>:<name>:chunks` (LIST — replay buffer)

Each `streams.write` appends one base64-encoded chunk.

```
   RPUSH stream:<runId>:<name>:chunks  base64("Hello, World")
```

A late subscriber reads from index 0 (or any earlier offset) using
`LRANGE` to catch up.

### 4.7.3 `owf:stream:<runId>:<name>:done` (STRING)

A single-byte flag: `"1"` when the stream has been closed by the writer.

```
   SET stream:<runId>:<name>:done "1"
```

The subscriber loop checks this on poll/subscribe to know when to close
the `ReadableStream`.

### 4.7.4 `owf:stream:<runId>:<name>:channel` (PUB/SUB)

The live channel. Publishers PUBLISH on every chunk; subscribers
SUBSCRIBE for live tail.

```
   PUBLISH stream:<runId>:<name>:channel  <chunkIdx, base64>
   PUBLISH stream:<runId>:<name>:channel  <EOF marker>      (on close)
```

PUB/SUB is fire-and-forget — that's why the replay LIST exists for late
joiners. With the Upstash REST client, PUB/SUB is unavailable; the
streamer falls back to polling LRANGE on a configurable interval.

---

## 4.8 Lifecycle summary

```
   ┌──────────────────────────┐ ┌──────────────────────────┐ ┌──────────────────────────┐
   │ KEY                      │ │ CREATED WHEN             │ │ DELETED WHEN             │
   ├──────────────────────────┤ ├──────────────────────────┤ ├──────────────────────────┤
   │ run:<runId>              │ │ run_created event        │ │ never (retention policy   │
   │                          │ │                          │ │  is the operator's)       │
   │ step:<runId>:<stepId>    │ │ step_created             │ │ never                    │
   │ hook:<hookId>            │ │ hook_created (winner)    │ │ hook_disposed / terminal │
   │ wait:<runId>:<corr>      │ │ wait_created             │ │ terminal cleanup         │
   │ event:* / events:*       │ │ events.create            │ │ never (the log)          │
   │ hook:tok:<sha256>        │ │ hook_created (winner)    │ │ hook_disposed / terminal │
   │ jobs (ZSET member)       │ │ queue()                  │ │ msg delivered or dropped │
   │ msg:<id>                 │ │ queue()                  │ │ msg delivered            │
   │ msg:<id>:lease           │ │ dispatcher claim         │ │ TTL (30s) or release     │
   │ stream:<runId>           │ │ first streams.write      │ │ terminal cleanup         │
   │ stream:*:chunks          │ │ first chunk              │ │ terminal cleanup         │
   │ stream:*:done            │ │ streams.close            │ │ terminal cleanup         │
   └──────────────────────────┘ └──────────────────────────┘ └──────────────────────────┘
```

**Terminal cleanup** runs after `run_completed` / `run_failed` /
`run_cancelled`: it removes hooks, hook-token claims, waits, and stream
state for the run, but **not** the event log or entity blobs — those
stay for observability.

---

## 4.9 Worked example dump

A small run that did one step and finished. After completion, a `SCAN
MATCH owf:*` for the run prefix would show roughly:

```
owf:event:wrun_01K…:evnt_01K0…    CBOR(run_created)
owf:event:wrun_01K…:evnt_01K1…    CBOR(run_started)
owf:event:wrun_01K…:evnt_01K2…    CBOR(step_created)
owf:event:wrun_01K…:evnt_01K3…    CBOR(step_started)
owf:event:wrun_01K…:evnt_01K4…    CBOR(step_completed)
owf:event:wrun_01K…:evnt_01K5…    CBOR(run_completed)

owf:events:run:wrun_01K…           ["evnt_01K0…","evnt_01K1…","evnt_01K2…",…]
owf:events:corr:step1               ["evnt_01K2…","evnt_01K3…","evnt_01K4…"]

owf:run:wrun_01K…                   {status: completed, output: ...}
owf:step:wrun_01K…:step1            {status: completed, output: ...}

owf:runs:all                        ZSET  {wrun_01K…: 1717459200000}
owf:runs:status:completed           ZSET  {wrun_01K…: 1717459200000}
owf:steps:run:wrun_01K…             ZSET  {step1: 1717459203000}

(no msg:* keys — both delivered and cleaned up)
(no stream:* keys — no streams used in this run)
```

---

## 4.10 Capacity & sizing notes

- **`jobs` ZSET cardinality** = currently scheduled jobs (delayed
  sleeps + retry-waiting). For a few thousand long sleeps, it's
  negligible (~80 bytes per member).
- **`events.create` writes per run** = `2 (start/end) + 3 per step + 1
  per hook + 1 per wait` events. A 5-step workflow ≈ 17 events.
- **Stream chunks** = arbitrary; if you stream large blobs, monitor
  `stream:*:chunks` LIST sizes and consider archiving long runs.

The next page ([05 · Event sourcing](./05-event-sourcing.md)) zooms
into the event log and the state-machines it materialises.

---

[← 03 World contract](./03-world-contract.md) · [Index](./README.md) · [Next: 05 Event sourcing →](./05-event-sourcing.md)
