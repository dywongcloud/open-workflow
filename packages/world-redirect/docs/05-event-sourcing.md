# 05 · Event sourcing

[← 04 Redis keyspace](./04-redis-keyspace.md) · [Index](./README.md) · [Next: 06 307 dispatch →](./06-307-dispatch.md)

---

`world-redirect` is **event-sourced**: every state transition is an
immutable event, and the entity blobs (`run:*`, `step:*`, `hook:*`,
`wait:*`) are *materialised views* over that log. Reads can hit the
views directly for speed; rebuilds and replays use the log.

This page enumerates the event types, draws each entity's state
machine, and explains how the views stay consistent.

---

## 5.1 The event taxonomy

```
       ┌─────────────────────────────────────────────────────────────┐
       │                       16 event types                        │
       │                                                             │
       │  RUN          STEP            HOOK              WAIT        │
       │  ────         ────            ────              ────        │
       │  run_created  step_created    hook_created      wait_created│
       │  run_started  step_started    hook_received     wait_completed
       │  run_completed step_completed hook_disposed                 │
       │  run_failed   step_failed     hook_conflict                 │
       │  run_cancelled step_retrying                                │
       │                                                             │
       └─────────────────────────────────────────────────────────────┘
```

Each event carries:

```
   {
     eventId:        ULID-prefixed string ("evnt_…")
     runId:          which run it belongs to
     eventType:      one of the 16 above
     correlationId?: which sub-entity it concerns (stepId, hookId, …)
     specVersion:    protocol version (2 currently)
     createdAt:      Date
     eventData?:     event-specific payload (CBOR-encoded)
   }
```

---

## 5.2 Run state machine

```
                  ┌─────────────────────────────────────────┐
                  │                                         │
        run_created                                         │
            │                                               │
            ▼                                               │
         ┌──────────┐ ─── run_started ────▶ ┌─────────┐    │
         │ pending  │                       │ running │    │
         └────┬─────┘ ◀─── (re-enqueue) ─── └─────┬───┘    │
              │                                    │        │
              │ run_cancelled                      │        │
              │                                    │        │
              ▼                                    ▼        │
                                          ┌──────────────┐  │
                                          │ run_completed│  │
              │                           │ run_failed   │  │
              │                           │ run_cancelled│  │
              │                           └──────┬───────┘  │
              │                                  │           │
              └────────► terminal (no further transitions)   │
                                                              │
                  ┌────────────────────────────────────────────┘
                  ▼
              ┌─────────────┐
              │ terminal:   │ ─── run_cancelled is the only event allowed
              │ completed / │     after a terminal state, and only if the
              │ failed /    │     current state is already cancelled
              │ cancelled   │     (idempotent re-cancellation)
              └─────────────┘
```

Transition guards (enforced in `events.create`):

- From any terminal state:
  - `run_started`, `run_completed`, `run_failed` → throw
    `EntityConflictError` (or `RunExpiredError` for `run_started`).
  - `run_cancelled` when already cancelled → returns the existing run
    (idempotent), still records the event for the audit log.
- `step_created`, `hook_created`, `wait_created` on a terminal run →
  throw `EntityConflictError`.

```
        ┌────────────────────────────────────────────────────────────┐
        │  events.create(runId, {eventType: run_started})            │
        ├────────────────────────────────────────────────────────────┤
        │  1. fetch run                                              │
        │  2. if status == 'running'  → return existing (idempotent) │
        │  3. if status in terminal   → throw RunExpiredError        │
        │  4. update HSET run:<runId> status=running, startedAt=now  │
        │  5. ZADD runs:status:running, ZREM runs:status:pending     │
        │  6. RPUSH events:run:<runId>, SET event:<runId>:<id>       │
        │  7. return EventResult({event, run})                       │
        └────────────────────────────────────────────────────────────┘
```

---

## 5.3 Step state machine

```
        step_created
            │
            ▼
         ┌──────────┐ ─── step_started ───▶ ┌─────────┐
         │ pending  │                       │ running │
         └────┬─────┘                       └────┬────┘
              │                                  │
              │                                  │
              │                                  │
              │                                  ▼
              │                                                ┌──────────────┐
              │                                                │ completed    │
              │                                                │ failed       │
              │                                                └──────┬───────┘
              │                                                       │
              │                                                       │ no further
              │                                                       │ transitions
              │                                                       ▼
              │                                                   (terminal)
              │
              │
              │   ┌─── step_retrying ──┐
              │   │                    │
              │   ▼                    │
         ┌──────────┐                  │
         │ pending  │ ─── step_started ┘  (attempt += 1)
         └──────────┘
                                     
   step_completed   ──▶ status=completed, output=…
   step_failed      ──▶ status=failed,    error=…   (if maxAttempts reached)
   step_retrying    ──▶ status=pending,   retryAfter=…
```

The runtime tracks `attempt`; each `step_started` increments it. A
`step_retrying` after a `step_failed` resets the entity back to
`pending`, ready for the dispatcher to re-deliver.

```
        ┌────────────────────────────────────────────────────────────┐
        │  Step retry → restart cycle                                │
        │                                                            │
        │   step throws RetryableError                               │
        │       │                                                    │
        │       ▼                                                    │
        │   events.create(step_failed)                               │
        │       │                                                    │
        │       ▼                                                    │
        │   events.create(step_retrying, {retryAfter: now+backoff}) │
        │       │                                                    │
        │       ▼                                                    │
        │   queue('__wkf_step_<stepId>', {…}, {delaySeconds: …})    │
        │       │                                                    │
        │       ╳ ╳ backoff window ╳ ╳                              │
        │       ▼                                                    │
        │   step handler invoked again, attempt += 1                 │
        │   events.create(step_started)                              │
        │       │                                                    │
        │       ▼                                                    │
        │   step runs again                                          │
        └────────────────────────────────────────────────────────────┘
```

---

## 5.4 Hook state machine

```
        hook_created                              hook_conflict
            │                                          │
            │ wins NX claim                            │ loses NX claim
            ▼                                          ▼
       ┌──────────┐                              (no hook persisted,
       │  active  │ ◀────── hook_received        log records the
       └────┬─────┘         (token used,         conflict)
            │                run resumes via
            │                wait_completed)
            │
            │ hook_disposed
            ▼
        (deleted — hook:<hookId> + hook:tok:<sha256(token)> +
                   hooks:run:<runId> ZSET member all removed)
```

`hook_conflict` is a special event: it's recorded for visibility but
**does not** create a hook entity. The runtime sees the event, surfaces
"token already in use" to the caller, and the run carries on without
this hook.

Hook lifetime is tied to the run that created it: terminal cleanup
disposes any still-active hooks for the run.

---

## 5.5 Wait state machine

```
        wait_created
            │
            ▼
       ┌──────────┐
       │ waiting  │
       └────┬─────┘
            │
            │ wait_completed
            ▼
       ┌──────────┐
       │ completed│  (terminal — wait id can never be reused for the same runId)
       └──────────┘
```

`wait_created` fires on both sleeps and hook waits. Re-creation with
the same `correlationId` throws `EntityConflictError` — the runtime
guards against double-suspending on the same identifier.

---

## 5.6 Materialisation: from event log to entity blob

The **event** is the truth; the **entity blob** is a view. Every
event-handler in `events.create` does two writes:

```
         events.create(runId, evt)
                   │
       ┌───────────┴─────────────┐
       │                         │
       ▼                         ▼
   1. event write           2. entity write (HSET)
                                + index update (ZADD / ZREM)

   SET event:<runId>:<evtId> CBOR(evt)
   RPUSH events:run:<runId> <evtId>
   RPUSH events:corr:<corr>  <evtId>    (if correlationId present)
```

So a read can take either path:

- **Fast** path: `runs.get(runId)` → `HGETALL run:<runId>` → return the
  materialised view directly.
- **Authoritative** path: `events.list({runId})` → re-derive any view
  from the log. Used by the runtime's event-replay during workflow
  resume.

If the two ever disagree, the event log wins. The materialised view can
be rebuilt by replaying.

---

## 5.7 Replay semantics

When the flow handler resumes a workflow (e.g. on a 307 chain
continuation, or after a sleep wakes up), it:

```
   1. world.events.list({runId})        ─▶ load full event log
   2. workflow body runs, but every     
      step() / sleep() / createHook()   
      asks the runtime "have I done     
      this before?"                     
   3. for each "have I done X?":        ─▶ if the event log already
      runtime checks the event log         contains the corresponding
                                            event, replay it without
                                            re-executing the side effect
   4. when it reaches an event that     
      is NOT in the log → that's the    
      current frontier → execute it      
      fresh, append the event            
```

This makes every workflow body **deterministic given the same event
log** — the foundation of the WDK's exactly-once-effects guarantee.

```
        ┌─────────────────────────────────────────────────────────┐
        │   Replay diagram                                        │
        │                                                         │
        │   workflow body                                         │
        │     run_started ───────▶ log says: already there        │
        │     step("a")    ───────▶ log says: completed, return out│
        │     stream.write ───────▶ log says: chunk index N already│
        │     step("b")    ───────▶ log says: completed, return out│
        │     sleep(30s)   ───────▶ log says: waiting              │
        │                  ───────  if wait_completed is in log:   │
        │                                continue                  │
        │                          else:                            │
        │                                suspend (return 200)       │
        │     step("c")    ───────▶ log: NOT THERE → execute        │
        │                          create step_created/started/…   │
        │     return out   ───────▶ run_completed                  │
        └─────────────────────────────────────────────────────────┘
```

---

## 5.8 Why this matters for the trampoline

The 307 trampoline only works if each hop is independently safe to
re-deliver. Event sourcing guarantees that:

1. The flow handler may be invoked multiple times for the same message.
   The first time appends events; subsequent times see those events in
   the log and **skip** the side effects.
2. The lease key prevents most double-dispatch; the event log catches
   the rest.
3. A crashed handler mid-chain leaves the event log in a valid state
   (the partial events are durable). A retry replays cleanly to the
   exact point of failure and continues.

```
        ┌──────────────────────────────────────────────────────────┐
        │  Crashes are safe                                        │
        │                                                          │
        │  ┌─────────────┐   ┌──────────────┐   ┌─────────────┐   │
        │  │ start chain │   │ chain dies   │   │ next         │   │
        │  │ msg A       │   │ mid-msg B    │   │ dispatcher   │   │
        │  │ writes      │   │ partial      │   │ tick:        │   │
        │  │ run_started │   │ events       │   │ ZSET still   │   │
        │  │ writes      │   │ written      │   │ has msg B    │   │
        │  │ step_created│   │ before crash │   │ - claim,     │   │
        │  │ posts 307   │   │              │   │ - POST,      │   │
        │  └──────┬──────┘   └──────┬───────┘   │ - handler    │   │
        │         │                  │           │   replays    │   │
        │         └──────────────────┘           │   from log   │   │
        │                                        │ - picks up   │   │
        │                                        │   where it   │   │
        │                                        │   left off   │   │
        │                                        └─────────────┘   │
        └──────────────────────────────────────────────────────────┘
```

---

## 5.9 The per-run mutex

Event writes for the same run are serialised through `RunMutex`:

```
    events.create(runId, …)
        │
        ▼
    RunMutex.run(runId, async () => {
       …actual create logic…
    })

    ┌────────────────────────────────────────────────────────────────┐
    │  Inside one process:                                            │
    │    runId="A" mutex holds → other A-event creates queue          │
    │    runId="B" mutex independent → B-event creates proceed         │
    │                                                                 │
    │  Across processes:                                              │
    │    no shared lock; relies on event-write atomicity per command  │
    │    plus the runtime's terminal-state guards to reject races     │
    └────────────────────────────────────────────────────────────────┘
```

The mutex is sufficient for the **single-dispatcher per environment**
deployment model recommended in
[02 · Architecture](./02-architecture.md#243-the-dashboard). Multi-host
event writes are still atomic per Redis command, but ordering across
hosts depends on event-arrival order.

---

## 5.10 Why we never modify or delete events

The event log is **append-only**. Even on terminal cleanup, we delete
hooks/waits/scheduler keys but never events. This buys:

1. **Audit trail.** The dashboard can show exactly what happened, in
   order, on any run.
2. **Reproducibility.** A run can be replayed with the same inputs
   forever.
3. **Schema evolution.** New event types added in future versions can
   live alongside old ones; views are rebuilt from the same source.

The cost is storage growth (one CBOR blob per event, ~100-500 bytes
typical). For high-volume deployments operators sometimes archive old
runs by `COPY` + `DEL` of the run-scoped event keys — but this is an
explicit operator action, never something the world does on its own.

---

[← 04 Redis keyspace](./04-redis-keyspace.md) · [Index](./README.md) · [Next: 06 307 dispatch →](./06-307-dispatch.md)
