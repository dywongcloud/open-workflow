# 08 · Failure & retry

[← 07 End-to-end flow](./07-end-to-end-flow.md) · [Index](./README.md) · [Next: 09 Hooks & streams →](./09-hooks-and-streams.md)

---

This page covers the failure model: error categories, the retry
backoff formula, the lease-expiry safety net, terminal cleanup, and the
boundary conditions where things stop being safe.

---

## 8.1 Error taxonomy

The WDK runtime distinguishes two kinds of error a step body can
throw:

```
        ┌────────────────────────────────────────────────────────┐
        │                                                        │
        │   throw new RetryableError("temporary issue")          │
        │       │                                                │
        │       ▼                                                │
        │   Runtime records step_failed,                          │
        │   schedules step_retrying for delay = backoff(attempt) │
        │   step will run again                                  │
        │                                                        │
        │   throw new FatalError("permanent issue")              │
        │       │                                                │
        │       ▼                                                │
        │   Runtime records step_failed (terminal)               │
        │   Records run_failed                                   │
        │   No retry; workflow terminated                        │
        │                                                        │
        │   throw new Error("ordinary")                          │
        │       │                                                │
        │       ▼                                                │
        │   Treated as RetryableError by default — retry until   │
        │   maxAttempts, then promoted to terminal run_failed    │
        │                                                        │
        └────────────────────────────────────────────────────────┘
```

The world doesn't need to inspect the error class — the runtime decides
which event to emit. `world-redirect`'s job is only to honour the
delivery semantics that emit prescribes.

---

## 8.2 The retry backoff formula

```
   backoffMs(attempt, base = retryBaseMs) =
       attempt ≤ 6:  base
       attempt > 6:  min(base * 2^(attempt - 6), 5 * 60_000)
```

So with defaults (`base = 5_000` ms, `maxAttempts = 10`):

| attempt | delay |
| --- | --- |
| 1 | 5 s |
| 2 | 5 s |
| 3 | 5 s |
| 4 | 5 s |
| 5 | 5 s |
| 6 | 5 s |
| 7 | 10 s |
| 8 | 20 s |
| 9 | 40 s |
| 10 | 80 s (then drop) |

Curve: flat early (to absorb transient hiccups quickly), then
exponential to avoid hammering downstream during a sustained outage,
then capped at 5 minutes.

```
   delay
   (sec)
    300│                            ┌──────────────  cap = 5 min
       │                            │
    200│                            ┘
       │
    100│                       ┌───┘
       │                  ┌────┘
     50│             ┌────┘
       │        ┌────┘
     20│   ┌────┘
       │┌──┘
      0└───────────────────────────────────▶ attempt
        1  2  3  4  5  6  7  8  9  10
```

---

## 8.3 The retry path step by step

```
   step body throws RetryableError
        │
        ▼
   runtime calls
   events.create(step_failed, corr:<stepId>, eventData:{error})
        │
        ▼
   if attempt < maxAttempts:
     events.create(step_retrying,
                   corr:<stepId>,
                   eventData:{error, retryAfter: now+backoff})
       │
       ▼
     queue("__wkf_step_<stepId>",
            {runId, stepId, attempt: N+1},
            {delaySeconds: backoffMs/1000})
       │
       ▼
     respond 200 (chain ends, dispatcher will pick up later)

   if attempt >= maxAttempts:
     events.create(run_failed,
                   eventData:{error,
                              errorCode:"STEP_RETRY_EXHAUSTED"})
       │
       ▼
     terminal cleanup
       │
       ▼
     respond 200
```

---

## 8.4 Lease expiry — the safety net for crashes

If the flow handler crashes between acquiring the lease and finishing
the message:

```
   t0   dispatcher acquires lease msg:<mA>:lease (TTL 30s)
        POST flow?msg=mA → handler is invoked

   t1   handler writes events.create(step_started)
        partial state in Redis (intentional — event log is durable)

   t1+δ handler process dies (OOM / crash / hard timeout)
        the in-flight HTTP request returns 5xx or never returns

   t1+30s lease auto-expires

   next dispatcher tick:
        ZRANGEBYSCORE jobs 0 now → [mA] (still there, never consumed)
        SET msg:<mA>:lease NX EX 30 → OK
        POST flow?msg=mA → handler invoked anew
            replay event log → step_started already there → skip side effect
            execute step body (it never wrote step_completed before)
            success → step_completed → continue chain
```

The combination of **durable event log** + **deterministic replay** +
**short lease TTL** is what makes the system self-healing without any
operator intervention.

---

## 8.5 Network errors in the dispatcher

```
   dispatcher.fetch(target) → throws (DNS, connect, EPIPE, abort)
        │
        ▼
   catch:
     console.error
     // best-effort: bump attempt + re-enqueue with backoff
     get current msg
     update attempt = current + 1
     if attempt > maxAttempts: drop (DEL msg, ZREM jobs)
     else: ZADD jobs (now + backoff) <mA>
     DEL msg:<mA>:lease
```

Network errors are treated similar to 5xx but are observed by the
dispatcher rather than the handler. The end state is the same: message
is rescheduled for retry, lease released.

---

## 8.6 5xx vs 2xx — what they mean for the chain

```
   handler returns 5xx
       │
       ▼
   dispatcher sees 5xx → already-rescheduled-by-handler scenario
       │
       ▼
   DEL msg:<id>:lease
   (no further action; the handler already wrote step_failed and
    re-enqueued or dropped)

   handler returns 2xx (200 or 307-followed-to-2xx)
       │
       ▼
   the handler ALREADY deleted the message inside the chain
   dispatcher just releases the lease

   handler returns 4xx
       │
       ▼
   (rare) — treated as a permanent handler error; lease released,
   message remains. Next tick will retry, hitting the same 4xx.
   This usually means a misconfigured handler / mismatched
   queue prefix. Investigate.
```

The 2xx / 5xx distinction is preserved end-to-end so the dispatcher can
log without doing extra Redis work; the actual durable state changes
happen inside the handler regardless of the response code.

---

## 8.7 Run-level failure & terminal cleanup

```
   run_failed event handled
        │
        ▼
   HSET run:<R> status=failed errorB64=...
   ZREM runs:status:running
   ZADD runs:status:failed

   then:

   terminalCleanup(runId):
     ── hooks ──
     SMEMBERS hooks:run:<R>
     for each hookId:
        get hook:<hookId>.token
        DEL hook:tok:<sha256(token)>
        DEL hook:<hookId>
        ZREM hooks:run:<R> <hookId>

     ── waits ──
     SCAN MATCH wait:<R>:*
     for each: DEL

     ── streams ──
     SMEMBERS stream:<R>
     for each name:
        SET stream:<R>:<name>:done "1"
        PUB stream:<R>:<name>:channel <EOF>
        DEL stream:<R>:<name>:chunks
        DEL stream:<R>:<name>:done
     DEL stream:<R>

     ── notably NOT deleted ──
     event:<R>:* and events:run:<R>
     run:<R>
     step:<R>:*
```

So after a terminal transition you keep:

- the run blob (for the dashboard's run list)
- the step blobs (for the run's step history)
- the event log (immutable record)

You lose:

- live hooks (their tokens are released for re-use)
- live waits (they can never resume anyway)
- stream chunks (the LIST replay buffer goes; the dashboard can show
  the closed state but not re-tail)

---

## 8.8 Dead-letter behaviour (or lack thereof)

`world-redirect` does **not** ship a built-in dead-letter store. When a
message exceeds `maxAttempts`:

```
   attempt = 10, threshold exceeded
        │
        ▼
   events.create(run_failed,
                 errorCode: "STEP_RETRY_EXHAUSTED")
        │
        ▼
   DEL msg:<id>
   ZREM jobs <id>
```

The event log is the audit trail; the message is gone. If you want a
DLQ you have two reasonable options:

1. Bump `maxAttempts` very high and never let things drop.
2. Subscribe to the run-completion stream / event-log appends in a
   side process and forward failed runs into your own DLQ system.

---

## 8.9 Failure path — the full picture

```
                          step body throws
                                │
              ┌─────────────────┼──────────────────┐
              │                 │                  │
              ▼                 ▼                  ▼
        FatalError        RetryableError      ordinary Error
              │                 │                  │
              ▼                 ▼                  ▼
        events.create(    events.create(    events.create(
          step_failed)      step_failed)      step_failed)
              │                 │                  │
              │                 ▼                  ▼
              │            attempt < max?     attempt < max?
              │            ──────┬─────       ──────┬─────
              │            yes        no      yes        no
              │             │          │       │          │
              │             ▼          ▼       ▼          ▼
              │       events.create  events.   …same as     …same as
              │       (step_retrying)create   RetryableError RetryableError
              │             │       (run_failed)
              │             ▼          │
              │       queue with       │
              │       backoff          │
              │             │          │
              ▼             ▼          ▼
        events.create   chain ends   events.create
        (run_failed)    dispatcher   (run_failed)
              │         picks up      │
              ▼         after delay   ▼
        terminal       new attempt    terminal
        cleanup        runs through   cleanup
                       same code
                       paths
```

---

## 8.10 What's NOT retried

```
   ┌──────────────────────────────────────────────────────────┐
   │   These never retry:                                     │
   │                                                          │
   │  • Workflow body throws outside any step                 │
   │     → run_failed, errorCode "WORKFLOW_FATAL"             │
   │                                                          │
   │  • events.create's terminal-state guards                 │
   │     → EntityConflictError surfaced as 5xx,               │
   │       but won't put the message back into jobs           │
   │                                                          │
   │  • The hook NX-claim losing race                         │
   │     → hook_conflict event, no retry; the user code        │
   │       decides what to do                                 │
   │                                                          │
   │  • Hop counter exceeded                                  │
   │     → chain stops, dispatcher resumes; not a failure     │
   └──────────────────────────────────────────────────────────┘
```

These cases mostly indicate user-level bugs (a workflow with a runtime
error outside a step, hook tokens that collide because of a bad token
strategy, etc.). The right fix is upstream, not at the dispatch layer.

---

## 8.11 Observability hooks

Every retry path emits events the dashboard can show:

```
   step_failed     (every failure)
   step_retrying   (when a retry is scheduled, with retryAfter)
   run_failed      (when a run permanently fails)
```

`events.listByCorrelationId({correlationId: stepId})` gives you the
complete history of attempts for one step, ordered chronologically.

---

[← 07 End-to-end flow](./07-end-to-end-flow.md) · [Index](./README.md) · [Next: 09 Hooks & streams →](./09-hooks-and-streams.md)
