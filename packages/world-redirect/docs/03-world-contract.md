# 03 · The World contract

[← 02 Architecture](./02-architecture.md) · [Index](./README.md) · [Next: 04 Redis keyspace →](./04-redis-keyspace.md)

---

The WDK runtime talks to whatever backend you point it at through a
narrow TypeScript interface called `World`, defined in
`@workflow/world`. This page enumerates every method of that interface
and explains what `world-redirect` does when each one is called.

---

## 3.1 The type, at a glance

```
                                ┌──────────────────────────┐
                                │         World            │
                                │  = Queue                 │
                                │  & Storage               │
                                │  & Streamer              │
                                │  + specVersion           │
                                │  + getEncryptionKeyForRun│
                                │  + start? close?         │
                                └────┬─────┬─────┬─────────┘
                                     │     │     │
                  ┌──────────────────┘     │     └──────────────────┐
                  ▼                        ▼                        ▼
        ┌─────────────────┐     ┌────────────────────┐    ┌─────────────────┐
        │     Queue       │     │      Storage       │    │    Streamer     │
        │                 │     │                    │    │                 │
        │ queue()         │     │ runs.get/list      │    │ streams.write   │
        │ createQueue     │     │ steps.get/list     │    │ streams.writeM  │
        │   Handler()     │     │ events.create/get/ │    │ streams.close   │
        │ getDeployment   │     │   list/listByCorr  │    │ streams.get     │
        │   Id()          │     │ hooks.get/getByTok │    │ streams.list    │
        │                 │     │   list             │    │ streams.getChks │
        │                 │     │                    │    │ streams.getInfo │
        └─────────────────┘     └────────────────────┘    └─────────────────┘
```

---

## 3.2 Queue methods

### 3.2.1 `queue(queueName, message, options?)`

Persists a durable job and schedules it for delivery.

```
   caller                                       Redis
   ──────                                       ─────
   world.queue(
     "__wkf_workflow_demo",                     ┐
     {runId: "wrun_...", ...},                  │  computes msgId = ULID
     {delaySeconds: 0})                         │  computes runAtMs = now + delay
       │                                        │
       ▼                                        │
   resolve queueName route                      │
     starts with "__wkf_step_"  → step          │
     else                       → flow          │
       │                                        │
       ▼                                        ▼
                            SET  msg:<msgId>  CBOR({queueName, route, runId,
                                                    attempt:1, body: message})
                            ZADD jobs  <runAtMs>  <msgId>
       │                                        │
       ▼                                        ▼
   return {messageId: msgId}                    (no other I/O)
```

**Inputs:**
- `queueName`: opaque string used to route to the right handler. By
  convention `__wkf_workflow_<name>` for workflow start/resume,
  `__wkf_step_<stepId>` for individual steps.
- `message`: the durable payload. Stored as CBOR-encoded base64.
- `options.delaySeconds`: schedule into the future. `0` (or omitted)
  means "due immediately".

**Effects:** two Redis writes; no HTTP. The dispatcher picks it up on
the next poll tick.

**Idempotency:** the messageId is a fresh ULID per call, so two
`queue()`s with the same body produce two distinct messages. If you
want deduplication, do it at the call site.

---

### 3.2.2 `createQueueHandler(prefix, handler)`

Returns a `Request → Response` function that the host wires into its
HTTP layer. This is what `/.well-known/workflow/v1/flow` and `/step`
become.

```
   ┌──────────────────────────────────────────────────────────────────┐
   │  POST /.well-known/workflow/v1/flow?msg=msgA  arrives             │
   └────────────────────────────────┬─────────────────────────────────┘
                                    │
                                    ▼
                     ┌──────────────────────────────────┐
                     │  createQueueHandler-returned     │
                     │  wrapper                         │
                     └─────┬──────────────────┬─────────┘
                           │                  │
            url.searchParams.get('msg')        │
                           │                  │
                           ▼                  │
                  GET msg:<msgId> from Redis   │
                           │                  │
                           ▼                  │
                  decode CBOR(QueuePayload)    │
                           │                  │
                           ▼                  │
                  validate queueName prefix    │
                  matches handler's prefix     │
                           │                  │
                           ▼                  ▼
                  call user handler(payload, ctx)
                           │
              ┌────────────┼─────────────────────┐
              │            │                     │
        success         throws              returns {timeoutSeconds}
              │            │                     │
              ▼            ▼                     ▼
        DEL msg:*    re-enqueue with        re-enqueue with
        respond      backoff;               new runAt = now+T;
        2xx or       respond 5xx            respond 200
        307                                  
```

**Note**: the response from this handler is **not** what determines
whether a chain continues. The handler may return:

- `200 OK` — chain ends; dispatcher picks up next due job on a later tick.
- `307 Temporary Redirect` — chain continues immediately into the next
  message (the trampoline).
- `5xx` — failure path; the wrapper re-enqueues with backoff and
  returns the 5xx to the dispatcher (which logs and moves on).

---

### 3.2.3 `getDeploymentId()`

Returns a string identifying the current build/deployment. Used by the
runtime to detect deploy changes between event creation and replay.

```
   const id = await world.getDeploymentId()
   // → process.env.WORKFLOW_DEPLOYMENT_ID
   //   || (resolved fallback "dpl_<somehash>_local")
```

Purely a function of env; no Redis I/O.

---

## 3.3 Storage methods

### 3.3.1 `events.create(runId, event, params?)`

The single mutating entry point for the entire Storage layer. Every
state transition — run created, step started, hook claimed, wait
completed — flows through here.

```
   events.create(runId, {
     eventType: "step_completed",
     correlationId: "step1",
     eventData: { result: { … } }
   })
        │
        ▼
   RunMutex.run(runId, async () => {
     ──────────────────────────────
     1.  fetch current run (unless skipRunRead)
     2.  enforce terminal-state guards
     3.  fetch correlated entity (if step/hook event)
     4.  perform the event-specific transition:
         ┌────────────────────────────────────────┐
         │  switch (eventType) {                  │
         │    case "run_created":   …             │
         │    case "run_started":   …             │
         │    case "run_completed": …             │
         │    case "step_created":  …             │
         │    case "step_started":  …             │
         │    case "step_completed": …            │
         │    case "step_failed":    …            │
         │    case "step_retrying":  …            │
         │    case "hook_created":   …            │
         │    case "hook_received":  …            │
         │    case "hook_disposed":  …            │
         │    case "wait_created":   …            │
         │    case "wait_completed": …            │
         │    case "run_failed":     …            │
         │    case "run_cancelled":  …            │
         │  }                                     │
         └────────────────────────────────────────┘
     5.  append event blob + index entry
     6.  return EventResult { event, run?, step?, hook?, wait? }
   })
```

**Return**: an `EventResult` describing what changed — the new event
itself plus any entity whose materialised view was updated as a side
effect.

**Throws**:
- `WorkflowRunNotFoundError` if the run can't be found when needed.
- `EntityConflictError` if the transition is invalid (e.g. trying to
  re-start a completed run).
- `RunExpiredError` for the specific case of trying to start an
  already-terminal run.
- `HookNotFoundError` if a hook event refers to a missing hook.

---

### 3.3.2 `events.get(runId, eventId, params?)`

Read a single event by id.

```
   world.events.get(runId, "evnt_01K…", { resolveData: 'all' })
        │
        ▼
   GET event:<runId>:<eventId>
        │
        ▼
   decodeBlob → Event
        │
        ▼
   stripEventDataRefs(event, resolveData)
        │
        ▼
   return Event
```

`resolveData` controls whether the heavy `eventData` payload is
returned (`'all'`) or stripped to references (`'none'`). The dashboard
typically uses `'all'`; replay paths often use `'none'` for cheaper
loads.

---

### 3.3.3 `events.list({ runId, pagination, resolveData })`

Page through the event log for a run.

```
   world.events.list({ runId: "wrun_…", pagination: { limit: 50 } })
        │
        ▼
   LRANGE events:run:<runId> 0 -1           ─┐  (or paginated range)
   MGET   event:<runId>:<eventId>...         │  one Redis round-trip per chunk
        │                                    │  via batched MGET
        ▼                                    │
   decode each → Event[]                     │
        │                                    │
        ▼                                    │
   sort asc, apply pagination cursor          │
        │                                    │
        ▼
   return { data: Event[], cursor, hasMore }
```

The event list is the durable log used both by the dashboard *and* by
the runtime's event replay (workflow body resumption).

---

### 3.3.4 `events.listByCorrelationId({ correlationId, pagination, … })`

Returns all events sharing a `correlationId` — useful for "show me
everything that happened to step X" or "all events for hook Y".

```
   world.events.listByCorrelationId({ correlationId: "step1" })
        │
        ▼
   LRANGE events:corr:<correlationId> 0 -1
   MGET event:* …
```

---

### 3.3.5 `runs.get(runId, params?)` / `runs.list(params?)`

Read materialised run views.

```
   runs.get("wrun_…", { resolveData: 'all' })
        │
        ▼
   HGETALL run:<runId>
        │
        ▼
   parse → WorkflowRun
        │
        ▼
   strip input/output if resolveData === 'none'

   ──────────────────────────────────────────────

   runs.list({ status: 'running', pagination: { limit: 100 } })
        │
        ▼
   ZRANGE runs:status:running <offset> <offset+limit>
   MGET   run:<runId>...
        │
        ▼
   return { data: WorkflowRun[], cursor, hasMore }
```

When no `status` filter is given, the index used is `runs:all`.

---

### 3.3.6 `steps.get(runId, stepId, params?)` / `steps.list({ runId, … })`

```
   steps.get("wrun_…", "step1")    → HGETALL step:<runId>:<stepId>
   steps.list({ runId: "wrun_…" }) → ZRANGE  steps:run:<runId>
                                     MGET    step:<runId>:<stepId>...
```

`resolveData` strips `input`/`output` blobs the same way it does for
runs.

---

### 3.3.7 `hooks.get(hookId)`, `hooks.getByToken(token)`, `hooks.list({ runId?, … })`

```
   hooks.get("hook_…")               → HGETALL hook:<hookId>

   hooks.getByToken("...")            → GET     hook:tok:<sha256(token)>
                                          (→ hookId)
                                       HGETALL hook:<hookId>

   hooks.list({ runId: "wrun_…" })   → ZRANGE  hooks:run:<runId>
                                       MGET    hook:<hookId>...
```

`getByToken` is the path the **webhook endpoint** uses to resolve an
incoming request to a hook.

---

## 3.4 Streamer methods

```
                     producer side                consumer side
                     ─────────────                ──────────────
                                                                
                     streams.write                streams.get
                     streams.writeMulti           streams.list
                     streams.close                streams.getChunks
                                                  streams.getInfo
```

### 3.4.1 `streams.write(runId, name, chunk)`

```
   streams.write("wrun_…", "out", "Hello")
        │
        ▼
   RPUSH stream:<runId>:<name>:chunks  base64("Hello")
   SADD  stream:<runId>                 <name>
   PUBLISH stream:<runId>:<name>:channel  <chunkIdx, base64>
```

The PUBLISH side feeds live subscribers; the LIST side gives any
late-arriving subscriber a replay buffer.

### 3.4.2 `streams.writeMulti(runId, name, chunks)`

Same as `write`, but batch — one `RPUSH` for the whole batch, one
`PUBLISH` per chunk (so subscribers still get fine-grained updates).

### 3.4.3 `streams.close(runId, name)`

```
   SET     stream:<runId>:<name>:done "1"
   PUBLISH stream:<runId>:<name>:channel  <EOF marker>
```

### 3.4.4 `streams.get(runId, name, startIndex?)`

Returns a `ReadableStream<Uint8Array>` of the chunks.

```
                         caller calls .get()
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │ initial backfill                          │
              │   LRANGE stream:*:chunks  startIndex -1   │
              │   enqueue each chunk into the stream      │
              └─────────────────┬────────────────────────┘
                                │
                                ▼
              ┌──────────────────────────────────────────┐
              │ live subscribe                            │
              │   SUBSCRIBE stream:*:channel              │
              │   on message:                             │
              │     if chunkIdx >= cursor: enqueue        │
              │   on EOF: controller.close()              │
              │                                           │
              │   (Upstash REST client: falls back        │
              │    to polling LRANGE every flushIntervalMs)│
              └──────────────────────────────────────────┘
```

`startIndex` can be:
- non-negative — start from that absolute chunk index.
- negative — "last N chunks" relative to current tail.

### 3.4.5 `streams.list(runId)` / `streams.getChunks(...)` / `streams.getInfo(...)`

Read-side helpers used mostly by the dashboard:

| method | returns |
| --- | --- |
| `streams.list(runId)` | `string[]` — stream names for the run |
| `streams.getChunks(runId, name, options?)` | a paginated, non-live snapshot |
| `streams.getInfo(runId, name)` | `{ tailIndex, done }` for the UI |

---

## 3.5 Lifecycle methods

### 3.5.1 `world.start()`

Optional but normal.

```
   world.start()
        │
        ▼
   if (config.startDispatcher) dispatcher.startPump()
        │
        ▼
   if (config.recoverActiveRuns) {
     for status in ['pending', 'running']:
       for run in await storage.runs.list({status}):
         await world.queue("__wkf_workflow_" + run.workflowName,
                           {runId: run.runId})
   }
```

The recovery step re-enqueues active runs after a restart so that even
a process that crashed mid-execution picks up where it left off.

### 3.5.2 `world.close()`

```
   world.close()
        │
        ▼
   dispatcher.stopPump()    // sets running=false, clears timer
        │
        ▼
   wait for in-flight ≤ 5s
        │
        ▼
   if (RedisClient.disconnect) await client.disconnect()
```

---

## 3.6 `specVersion`

```
   world.specVersion  // 2  (SPEC_VERSION_SUPPORTS_EVENT_SOURCING)
```

The constant signals to the runtime which version of the World protocol
this world implements. Bumped only when the protocol itself changes.

---

## 3.7 The interface in TypeScript

For the literal type definitions (and the source of truth that this
page summarises), see:

- `@workflow/world/src/interfaces.ts` — top-level `World` type.
- `@workflow/world/src/queue.ts` — `Queue` + payload schemas.
- `@workflow/world/src/events.ts` — `Event`, `EventResult`, event types.
- `@workflow/world/src/runs.ts` / `steps.ts` / `hooks.ts` / `waits.ts`
  — entity Zod schemas.

`world-redirect`'s implementation is in
`packages/world-redirect/src/`; the next page ([04 · Redis
keyspace](./04-redis-keyspace.md)) breaks the on-the-wire shapes down
key by key.

---

[← 02 Architecture](./02-architecture.md) · [Index](./README.md) · [Next: 04 Redis keyspace →](./04-redis-keyspace.md)
