# 06 · The 307 dispatch engine

[← 05 Event sourcing](./05-event-sourcing.md) · [Index](./README.md) · [Next: 07 End-to-end flow →](./07-end-to-end-flow.md)

---

This is the page about the central trick. Two cooperating mechanisms
make `world-redirect` work without a managed broker:

1. An **in-process poll loop** (the *dispatcher*) reads due jobs from
   Redis and POSTs them to the flow handler.
2. The flow handler chains **immediate** next-jumps via `307 Temporary
   Redirect` so the next jump runs inside the same HTTP request — no
   round-trip back to the dispatcher.

Together they replace the Vercel-managed queue broker.

---

## 6.1 The dispatcher pump

```
   world.start() called
        │
        ▼
   dispatcher.startPump()
        │
        ▼
   ┌─────────────────────────────────────────────────────────────┐
   │  pump loop (setTimeout-based)                               │
   │                                                             │
   │  while (running) {                                          │
   │      tick();                                                │
   │      await sleep(pollMs)   // default 1000 ms               │
   │  }                                                          │
   │                                                             │
   │  tick():                                                    │
   │    1. ZRANGEBYSCORE jobs 0 now LIMIT 0 batchSize            │
   │       → due msg ids                                         │
   │    2. for each msgId:                                       │
   │         SET msg:<id>:lease workerId NX EX 30                │
   │         if lease acquired:                                  │
   │            fire and forget: dispatchOne(msgId)              │
   │         else: skip                                          │
   │    3. yield until next tick                                 │
   └─────────────────────────────────────────────────────────────┘
```

The pump is **non-blocking**: `dispatchOne` is a fire-and-forget
async function that POSTs to the flow handler. The pump moves on to
the next msg immediately, bounded by `batchSize`
(`WORKFLOW_REDIS_DISPATCHER_BATCH`, default 8).

### 6.1.1 Pump states

```
        ┌────────────────────────────────────────────────────────┐
        │                                                        │
        │   running=false ─── world.start() ──▶ running=true     │
        │       ▲                                    │            │
        │       │                                    ▼            │
        │       │                              ┌──────────┐       │
        │       │                              │  ticking │       │
        │       │                              └────┬─────┘       │
        │       │                                    │            │
        │       │     world.close()                  │ sleep      │
        │       │                                    ▼            │
        │       └─────────────────────────────  ┌──────────┐      │
        │                                       │  idle    │      │
        │                                       └──────────┘      │
        └────────────────────────────────────────────────────────┘
```

`close()` sets `running = false` and clears the timer, then waits up to
5 seconds for in-flight `dispatchOne`s to finish before resolving.

---

## 6.2 The trampoline mechanism

When the dispatcher POSTs to the flow handler, the handler does one of
three things:

```
                     ┌────────────────────────────┐
                     │ flow handler completes      │
                     │ workflow step / state       │
                     │ transition for msg X        │
                     └─────────────┬───────────────┘
                                    │
              ┌─────────────────────┼──────────────────────────┐
              │                     │                          │
              ▼                     ▼                          ▼
   (a) immediate next jump   (b) delayed next jump      (c) run is terminal
        ready (now)              (sleep, retry, hook)      or workflow body
        │                        │                         done
        │                        │                          │
   ZADD jobs now msgY        ZADD jobs (now+T) msgY        nothing more to
   write msgY blob           write msgY blob                queue
   return 307 with            return 200                    return 200
   Location: ?msg=msgY
        │                        │                          │
        ▼                        ▼                          ▼
   fetch follows 307         chain ends; dispatcher       chain ends
   inside the same           picks msgY up after T
   function invocation       seconds
   to msgY → handler
   re-enters
```

### 6.2.1 Why path (a) is special

Paths (b) and (c) both end the request chain. The dispatcher takes over
next time. Path (a) — immediate continuation — is the one where the
trampoline pays off. A multi-step workflow with no `sleep` between
steps becomes:

```
   dispatcher tick
        │
        ▼
   POST /flow?msg=A
        │ (handler runs msg A, writes events, queues msg B with delay=0)
        ▼
   307 → /flow?msg=B&hop=1
        │ (fetch follows, same TCP / function)
        ▼
   POST /flow?msg=B&hop=1
        │ (handler runs msg B, writes events, queues msg C with delay=0)
        ▼
   307 → /flow?msg=C&hop=2
        │
        ▼
   ... continues until the workflow body suspends or finishes
        │
        ▼
   200 OK → request ends
```

One `fetch` from the dispatcher → many durable jumps. No per-jump
broker hop, no per-jump dispatcher poll latency.

---

## 6.3 Bytes on the wire

```
 Dispatcher → flow (initial POST)
 ──────────────────────────────────────────────────────────────────
   POST /.well-known/workflow/v1/flow?msg=msgA  HTTP/1.1
   host: my-app.example.com
   content-type: application/json
   x-vqs-queue-name: __wkf_workflow_demo
   x-vqs-message-id: msgA
   x-vqs-message-attempt: 1
   content-length: 0
   (empty body — payload is in Redis at msg:<msgA>)


 Flow handler → fetch (chain continues)
 ──────────────────────────────────────────────────────────────────
   HTTP/1.1 307 Temporary Redirect
   location: /.well-known/workflow/v1/flow?msg=msgB&hop=1
   content-length: 0
   (no body)


 fetch retransmits to the redirected URL
 ──────────────────────────────────────────────────────────────────
   POST /.well-known/workflow/v1/flow?msg=msgB&hop=1  HTTP/1.1
   host: my-app.example.com
   (same headers as the original POST — method + body preserved by 307)


 ... up to ~32 hops (default safety cap)

 Last hop: handler returns 200 instead of another 307
 ──────────────────────────────────────────────────────────────────
   HTTP/1.1 200 OK
   content-type: application/json
   content-length: 12
   {"ok":true}
```

### 6.3.1 The headers

```
   x-vqs-queue-name        the queueName originally used in world.queue()
   x-vqs-message-id        the msgId being dispatched
   x-vqs-message-attempt   delivery attempt count (incremented on retry)
```

These are inspected by `@workflow/core`'s flow handler to route the
message and report telemetry. The `x-vqs-*` prefix is preserved from
the Vercel WDK protocol for compatibility.

### 6.3.2 The `hop` query parameter

```
   ?msg=B&hop=1
```

A defensive integer that increments on every trampoline jump within
one chain. If `hop > maxHops` (default 32), the handler responds 200
instead of 307 even when the next jump is ready immediately. The
dispatcher then takes over.

**Why the cap?** Two reasons:

- Run-away loops (a programming bug in the workflow body that
  repeatedly enqueues the next step) would otherwise consume a single
  function invocation indefinitely. Most function platforms have an
  invocation time limit.
- Long chains pin a single TCP connection / function instance for the
  whole chain. Capping releases the instance periodically so other work
  can use it.

---

## 6.4 The full dispatcher tick

```
   tick()
     │
     ▼
   ZRANGEBYSCORE jobs 0 <now> LIMIT 0 <batchSize>
     │
     ▼  candidate msgIds
   for each msgId:
     ┌──────────────────────────────────────┐
     │  SET msg:<id>:lease w NX EX 30        │
     │     OK   →  proceed                   │
     │     nil  →  another dispatcher got    │
     │             it; skip                  │
     └──────────────────────────────────────┘
              │
              ▼
       GET msg:<id>
              │
              ▼
       parse CBOR(QueuePayload)
              │
              ▼
       target = base + (route==='step'
                       ? '/.well-known/workflow/v1/step'
                       : '/.well-known/workflow/v1/flow')
                + '?msg=' + id
              │
              ▼
       fetch(target, {
         method: 'POST',
         redirect: 'follow',    ← key: 307s are followed automatically
         headers: { x-vqs-queue-name,
                    x-vqs-message-id,
                    x-vqs-message-attempt },
         body: '',
       })
              │
              ▼
       (chain runs entirely server-side; we just await final response)
              │
              ▼
       res.status:
         2xx        → message delivered (handler deleted msg key)
                      DEL msg:<id>:lease
         307...     → fetch already followed; we're seeing the final 2xx/5xx
         5xx        → handler re-enqueued with backoff; lease persisted briefly
                      DEL msg:<id>:lease
         (network)  → re-enqueue ourselves with backoff
                      DEL msg:<id>:lease
```

`redirect: 'follow'` (Node `undici` default) is what makes the
trampoline work transparently from the dispatcher's perspective. From
its point of view, it issued one POST and got one response.

---

## 6.5 When trampoline doesn't apply

```
   ┌──────────────────────────────────────────────────────┐
   │  Cases where flow handler returns 200, not 307       │
   ├──────────────────────────────────────────────────────┤
   │                                                      │
   │  • Workflow body suspended on sleep(ms > 0)          │
   │  • Workflow body suspended on createHook()           │
   │  • Step retry needs backoff (>0 delay)               │
   │  • Workflow body finished (terminal)                 │
   │  • Workflow body errored fatally (terminal failed)   │
   │  • Hop counter exceeded maxHops                      │
   │                                                      │
   └──────────────────────────────────────────────────────┘
```

In all these cases the chain ends. The dispatcher's poll loop is what
resumes the work later.

---

## 6.6 Multi-host & multi-dispatcher

`world-redirect`'s leasing is correct for multiple dispatchers, but
**recommended** is one dispatcher per environment:

```
        ┌──────────────────────────────────────────────────────────┐
        │  Recommended topology                                    │
        │                                                          │
        │   ┌──────────────────────────┐                           │
        │   │  Host A                  │                           │
        │   │  - flow handler runs     │                           │
        │   │  - dispatcher ON          │                           │
        │   └────────────┬─────────────┘                           │
        │                │                                          │
        │   ┌──────────────────────────┐  ┌──────────────────────┐ │
        │   │  Host B                  │  │  Host C              │ │
        │   │  - flow handler runs     │  │  - flow handler runs │ │
        │   │  - dispatcher OFF         │  │  - dispatcher OFF    │ │
        │   │  (set                     │  │  (same)              │ │
        │   │   WORKFLOW_REDIS_         │  │                      │ │
        │   │   DISABLE_DISPATCHER=1)   │  │                      │ │
        │   └──────────────────────────┘  └──────────────────────┘ │
        └──────────────────────────────────────────────────────────┘
```

Why: every additional dispatcher means more Redis reads / sec
(`ZRANGEBYSCORE` every tick) and more lease contention. The work is
already distributed via the fact that all hosts can serve flow handler
requests — only the *poller* role needs to be singular.

### 6.6.1 Multi-dispatcher correctness

If you do run multiple dispatchers (e.g. blue-green deploy windows):

```
   tick at dispatcher A
        ↓
   ZRANGEBYSCORE jobs 0 now → [m1, m2]
   SET m1:lease A NX EX 30 → OK
   SET m2:lease A NX EX 30 → OK
        ↓
   POST m1, POST m2 (fire and forget)

   tick at dispatcher B (concurrent)
        ↓
   ZRANGEBYSCORE jobs 0 now → [m1, m2]
   SET m1:lease B NX EX 30 → nil (A holds)
   SET m2:lease B NX EX 30 → nil (A holds)
        ↓
   skip both
```

The lease key serialises "who handles this message" at the cost of one
extra Redis op per candidate.

### 6.6.2 Lease expiry edge case

If host A crashes between acquiring the lease and the handler deleting
the message:

```
   A acquires m1:lease (TTL 30s)
   A crashes
        │
        ▼ 30 seconds later
   m1:lease auto-expires
        │
        ▼
   B's next tick:
     SET m1:lease B NX EX 30 → OK
     POST flow?msg=m1
        │
        ▼
   handler replays event log, sees the partial state if any,
   continues from there
```

The event-sourced replay (see [05 · Event sourcing](./05-event-sourcing.md))
makes this safe even if A had already written some events before
crashing — B's invocation skips those side effects and resumes at the
first unwritten event.

---

## 6.7 Failure paths in the chain

```
                  POST /flow?msg=A
                          │
              ┌───────────┼───────────────────────────────────┐
              │           │                                   │
              ▼           ▼                                   ▼
        2xx (chain   307 (chain                            5xx (handler
        ended)       continues)                            error)
              │           │                                   │
              ▼           ▼                                   ▼
        DEL msg:A    fetch follows                       handler already:
        DEL lease    → POST /flow?msg=B                   - bumped attempt
                                                          - re-enqueued msg
                                                            with backoff
                                                          - logged
                                                          dispatcher just
                                                          releases the
                                                          lease and moves on
              │           │                                   │
              │           ▼                                   ▼
              │      eventually 2xx                  next tick (after backoff):
              │      or 5xx for the                  dispatcher picks up
              │      chain endpoint                  again with attempt+=1
              └───────────────────────────────────────────────┘
```

Network-level errors (fetch reject) are treated the same as 5xx by the
dispatcher: bump attempt + re-enqueue + log.

---

## 6.8 Throughput characteristics

```
  Operation                                   Cost  (Redis ops per chain)
  ──────────────────────────────────────────  ──────────────────────────
  Dispatcher tick (no work)                    1 ZRANGEBYSCORE
  Tick that finds N due msgs                   1 ZRANGE + N SET (lease)
  Each dispatchOne                             1 GET msg + 1 fetch
  Each chain that does K hops                  K-1 trampoline POSTs (no Redis)
  Each handler invocation                      ~5-10 ops (events.list +
                                                 events.create + entity HSET)
```

**Limiting factor** is usually the Redis op rate divided by the
handler's per-invocation ops. With Upstash REST (HTTP), each op is a
network round-trip — latency dominates. With node-redis (RESP over
TCP), pipelined ops can run thousands per second on a single
connection.

---

## 6.9 Tuning knobs

```
   env var                                       default   purpose
   ────────────────────────────────────────────  ───────   ──────────────────
   WORKFLOW_REDIS_DISPATCHER_POLL_MS              1000     pump tick interval
   WORKFLOW_REDIS_DISPATCHER_BATCH                8        candidates per tick
   WORKFLOW_REDIS_LEASE_SECONDS                  30        msg lease TTL
   WORKFLOW_REDIS_MAX_HOPS                       32        trampoline cap
   WORKFLOW_REDIS_MAX_ATTEMPTS                   10        before drop
   WORKFLOW_REDIS_RETRY_BASE_MS                  5000      backoff base
   WORKFLOW_REDIS_DISABLE_DISPATCHER             unset     1 = read-only host
```

See [10 · Clients & configuration](./10-clients-and-config.md) for the
full env-var reference.

---

[← 05 Event sourcing](./05-event-sourcing.md) · [Index](./README.md) · [Next: 07 End-to-end flow →](./07-end-to-end-flow.md)
