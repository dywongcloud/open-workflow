# 07 · End-to-end flow

[← 06 307 dispatch](./06-307-dispatch.md) · [Index](./README.md) · [Next: 08 Failure & retry →](./08-failure-and-retry.md)

---

This page walks one complete workflow lifecycle from `start()` to
terminal cleanup. We follow a small example workflow:

```ts
// app/wf/greeting.ts
"use workflow";

import { step, sleep, getStream } from "workflow";

export default async function greeting(name: string) {
  await step("greet", async () => {
    const out = getStream("out");
    await out.write(`Hello, ${name}!\n`);
    return { greeted: true };
  });

  await sleep(30_000);

  return await step("farewell", async () => {
    return `Goodbye, ${name}!`;
  });
}
```

Triggered by:

```ts
await workflow.start("greeting", "World");
```

---

## 7.1 The full picture (one diagram)

```
TIME   CALLER / DISPATCHER          FLOW HANDLER  /.well-known/workflow/v1/flow            REDIS
─────  ────────────────────────     ─────────────────────────────────────────────────       ────────────────────

 t0    workflow.start("greeting",
                       "World")
         │
         ▼
       world.events.create(
         null, {run_created,
                input:"World",
                workflowName:
                  "greeting"}) ──────────────────────────────────────────────────▶   SET   event:<R>:<eA> CBOR
                                                                                     RPUSH events:run:<R> eA
                                                                                     HSET  run:<R>  status=pending
                                                                                                    input=...
                                                                                     ZADD  runs:status:pending
                                                                                     ZADD  runs:all

       world.queue(
         "__wkf_workflow_greeting",
         {runId:R})           ─────────────────────────────────────────────────▶   SET   msg:<mA> CBOR
                                                                                     ZADD  jobs <t0> mA

 t0+ε  dispatcher tick:
       ZRANGEBYSCORE jobs 0 now
         → [mA]
       SET msg:<mA>:lease ok
         │
         ▼
       POST /flow?msg=mA ─────▶ ┌─────────────────────────────────────────────────┐
                                 │ flow handler — chain start                       │
                                 │                                                  │
                                 │ GET msg:<mA>                                     │
                                 │ events.list({runId:R}) → [run_created]           │
                                 │                                                  │
                                 │ events.create(run_started)  ────────────────────│─▶ SET event:<R>:<eB> CBOR
                                 │                                                  │  RPUSH events:run:<R> eB
                                 │                                                  │  HSET run:<R> status=running
                                 │                                                  │  ZREM runs:status:pending
                                 │                                                  │  ZADD runs:status:running
                                 │                                                  │
                                 │ workflow body runs                               │
                                 │ → step("greet", …)                               │
                                 │   events.create(step_created, corr:s1) ─────────│─▶ event:<R>:<eC> CBOR
                                 │                                                  │  HSET step:<R>:s1 status=pending
                                 │                                                  │  ZADD steps:run:<R> s1
                                 │                                                  │
                                 │   events.create(step_started, corr:s1)          │  event:<R>:<eD> CBOR
                                 │                                                  │  HSET step:<R>:s1 status=running
                                 │                                                  │                attempt=1
                                 │   step body executes →                           │
                                 │     getStream("out").write(...)                  │
                                 │       streams.write(R,"out","Hello,…")  ────────│─▶ RPUSH stream:<R>:out:chunks
                                 │                                                  │  SADD  stream:<R> out
                                 │                                                  │  PUB   stream:<R>:out:channel
                                 │     returns {greeted:true}                       │
                                 │                                                  │
                                 │   events.create(step_completed, corr:s1)  ──────│─▶ event:<R>:<eE>
                                 │                                                  │  HSET step:<R>:s1 status=completed
                                 │                                                  │                output=...
                                 │                                                  │
                                 │ workflow body continues                          │
                                 │ → sleep(30_000) — DELAYED                        │
                                 │   events.create(wait_created,                    │
                                 │     corr:w1, resumeAt=t0+30s)  ─────────────────│─▶ event:<R>:<eF>
                                 │                                                  │  HSET wait:<R>:w1 status=waiting
                                 │   queue("__wkf_workflow_greeting",               │
                                 │         {runId:R, resume:"w1"},                  │
                                 │         {delaySeconds:30})  ─────────────────────│─▶ SET msg:<mB> CBOR
                                 │                                                  │  ZADD jobs <t0+30s> mB
                                 │                                                  │
                                 │ → respond 200 OK (DELAYED, NO 307)               │
                                 └──────────────────────┬───────────────────────────┘
                                                         │
                                                         ▼
                                 DEL msg:<mA>
                                 DEL msg:<mA>:lease

       ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ 30 seconds — $0 of compute ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳ ╳

 t0+30s dispatcher tick:
        ZRANGEBYSCORE jobs 0 now
          → [mB]
        SET msg:<mB>:lease ok
          │
          ▼
        POST /flow?msg=mB ────▶ ┌─────────────────────────────────────────────────┐
                                 │ flow handler — chain start (new invocation)      │
                                 │                                                  │
                                 │ GET msg:<mB>                                     │
                                 │ events.list({runId:R})                           │
                                 │   → [created, started, sc, ss, st_comp,          │
                                 │      wait_created]                               │
                                 │                                                  │
                                 │ workflow body replays:                           │
                                 │   step("greet")    → log says completed → skip   │
                                 │   sleep(30_000)    → wait_created found,         │
                                 │                       wait_completed missing     │
                                 │                       → emit wait_completed and  │
                                 │                         continue                 │
                                 │                                                  │
                                 │ events.create(wait_completed,corr:w1) ──────────│─▶ event:<R>:<eG>
                                 │                                                  │  HSET wait:<R>:w1 status=completed
                                 │                                                  │
                                 │ → step("farewell", …) — next jump immediate      │
                                 │   queue("__wkf_step_s2", …, {delaySeconds:0}) ──│─▶ SET msg:<mC> CBOR
                                 │                                                  │  ZADD jobs <now> mC
                                 │                                                  │
                                 │ → respond 307                                    │
                                 │   Location: /flow?msg=mC&hop=1                   │
                                 └──────────────────────┬───────────────────────────┘
                                                         │
                                       fetch auto-follows 307
                                                         │
                                                         ▼
                                  POST /flow?msg=mC&hop=1
                                                         │
                                                         ▼
                                 ┌─────────────────────────────────────────────────┐
                                 │ flow handler — chain continues                    │
                                 │                                                  │
                                 │ GET msg:<mC>                                     │
                                 │ events.list({runId:R})                           │
                                 │                                                  │
                                 │ → step("farewell", …) — execute fresh             │
                                 │   events.create(step_created, corr:s2)  ────────│─▶ event:<R>:<eH>
                                 │   events.create(step_started, corr:s2)  ────────│─▶ event:<R>:<eI>
                                 │   step body executes → "Goodbye, World!"         │
                                 │   events.create(step_completed, corr:s2) ──────│─▶ event:<R>:<eJ>
                                 │                                                  │  HSET step:<R>:s2 status=completed
                                 │                                                  │                output=...
                                 │                                                  │
                                 │ workflow body returns "Goodbye, World!"          │
                                 │                                                  │
                                 │ events.create(run_completed,                     │
                                 │   output:"Goodbye, World!")  ───────────────────│─▶ event:<R>:<eK>
                                 │                                                  │  HSET run:<R> status=completed
                                 │                                                  │                output=...
                                 │                                                  │  ZREM runs:status:running
                                 │                                                  │  ZADD runs:status:completed
                                 │                                                  │
                                 │ streams.close(R,"out")  ─────────────────────────│─▶ SET stream:<R>:out:done "1"
                                 │                                                  │  PUB stream:<R>:out:channel EOF
                                 │                                                  │
                                 │ terminal cleanup:                                │
                                 │   delete hooks for run                           │
                                 │   delete waits for run                           │
                                 │   delete streams for run                         │
                                 │ (event log is kept)                              │
                                 │                                                  │
                                 │ → respond 200 OK                                 │
                                 └──────────────────────┬───────────────────────────┘
                                                         │
                                                         ▼
                                 DEL msg:<mC>
                                 DEL msg:<mC>:lease
```

---

## 7.2 Reading the diagram

The diagram has three columns:

```
   CALLER / DISPATCHER          FLOW HANDLER                                  REDIS
   ───────────────────          ────────────                                  ─────
```

Rows are chronological. Arrows that touch the Redis column are literal
Redis commands. The boxed regions are one flow handler invocation; a
box ends when the chain ends (the handler returns 200 or hits a 5xx).

The `╳ ╳ ╳ sleep ╳ ╳ ╳` band represents wall-clock time where **no
compute** is running — the function isn't busy, the dispatcher polls
periodically but the work isn't due, no charges accrue.

---

## 7.3 Reduced view — just the Redis writes

To sanity-check what actually persists, here's the same lifecycle as a
sequence of Redis-only writes (entity HSETs collapsed for brevity):

```
 t0      SET event:<R>:eA  (run_created)
         RPUSH events:run:<R> eA
         HSET run:<R> status=pending
         ZADD runs:status:pending

         SET msg:<mA>
         ZADD jobs <t0> mA

 t0+ε    SET msg:<mA>:lease NX EX 30

         SET event:<R>:eB (run_started)
         HSET run:<R> status=running

         SET event:<R>:eC (step_created s1)
         HSET step:<R>:s1 status=pending
         ZADD steps:run:<R> s1

         SET event:<R>:eD (step_started s1)
         HSET step:<R>:s1 status=running

         RPUSH stream:<R>:out:chunks "Hello,…"
         SADD  stream:<R> out
         PUB   stream:<R>:out:channel ...

         SET event:<R>:eE (step_completed s1)
         HSET step:<R>:s1 status=completed

         SET event:<R>:eF (wait_created w1)
         HSET wait:<R>:w1 status=waiting

         SET msg:<mB>
         ZADD jobs <t0+30s> mB

         DEL msg:<mA>
         DEL msg:<mA>:lease

 t0+30s  SET msg:<mB>:lease NX EX 30

         SET event:<R>:eG (wait_completed w1)
         HSET wait:<R>:w1 status=completed

         SET msg:<mC>
         ZADD jobs <now> mC

         (307 follow; same function invocation)

         SET msg:<mC>:lease NX EX 30   (in the new sub-request the lease
                                          is acquired implicitly)

         SET event:<R>:eH (step_created s2)
         SET event:<R>:eI (step_started s2)
         SET event:<R>:eJ (step_completed s2)
         HSET step:<R>:s2 status=completed

         SET event:<R>:eK (run_completed)
         HSET run:<R> status=completed
         ZREM runs:status:running
         ZADD runs:status:completed

         SET stream:<R>:out:done "1"

         (terminal cleanup: hooks, waits, streams for run)

         DEL msg:<mC>
         DEL msg:<mC>:lease
```

Total: 11 events, ~30 Redis ops, 2 dispatcher polls, 2 flow handler
invocations (each chaining through 1-2 hops).

---

## 7.4 The same workflow on the Vercel runtime — for contrast

For the same workflow on `@workflow/world-vercel`, the picture is:

```
   workflow.start
        │
        ▼ HTTP
   Vercel API queue.enqueue
        │
        ▼ broker dispatch (HTTP)
   /flow?msg=mA
        │
        ▼ handler runs step("greet"); needs new flow message:
   Vercel API queue.enqueue
        │
        ▼ broker dispatch (HTTP)
   /flow?msg=mB  (still part of run)
        │
        ▼ handler hits sleep(30s); enqueue delayed:
   Vercel API queue.enqueue delay=30s
        │
        ╳ ╳ ╳ wait ╳ ╳ ╳
        ▼
   broker dispatch
   /flow?msg=mC
        │
        ▼ handler runs step("farewell"); needs new flow message:
   Vercel API queue.enqueue
        │
        ▼ broker dispatch
   /flow?msg=mD
        │
        ▼ handler writes run_completed
```

**Round-trips to the broker**: ~5 (one per durable jump).
**Round-trips on `world-redirect`**: 2 (one per chain).

The difference comes from collapsing the immediately-due jumps into the
same chain via the 307.

---

## 7.5 Parallel fan-out

If the workflow body does `Promise.all([step('a'), step('b'),
step('c')])`, the runtime queues three messages with no delay. Each gets
its own dispatcher tick and its own chain — they run in parallel.

```
   workflow body:                                Redis ZSET jobs:
     Promise.all([                                 mA <now>
       step('a'),                                  mB <now>
       step('b'),                                  mC <now>
       step('c')                                   …
     ])

   dispatcher tick:
     ZRANGEBYSCORE → [mA, mB, mC]
     leases → all three OK
     POST /step?msg=mA ──┐
     POST /step?msg=mB ──┼── three concurrent fetches
     POST /step?msg=mC ──┘

   each step handler:
     reads its msg payload
     runs step body
     queues its result via step_completed event
     responds 2xx

   each Promise resolves; workflow body continues with the joined results
```

There is no special path for parallel — the queue + dispatcher + event
log handles it naturally because each step is its own durable message.

---

## 7.6 The same flow as a state diagram

```
   ┌────────────────────────────────────────────────────────────────────┐
   │  RUN STATE                                                         │
   │                                                                    │
   │   pending ── run_started ──▶ running ── run_completed ──▶ completed│
   │                                                                    │
   │   STEPS (s1, s2)                                                   │
   │                                                                    │
   │   s1: pending → running → completed                                │
   │   s2: pending → running → completed                                │
   │                                                                    │
   │   WAITS (w1)                                                       │
   │                                                                    │
   │   w1: waiting → completed                                          │
   │                                                                    │
   │   STREAMS                                                          │
   │                                                                    │
   │   "out": open → open (1 chunk) → closed                            │
   └────────────────────────────────────────────────────────────────────┘
```

Each line above corresponds to one or more event types in the event
log; the entity HASH carries the current state for quick reads.

---

[← 06 307 dispatch](./06-307-dispatch.md) · [Index](./README.md) · [Next: 08 Failure & retry →](./08-failure-and-retry.md)
