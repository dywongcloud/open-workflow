# 09 · Hooks & streams

[← 08 Failure & retry](./08-failure-and-retry.md) · [Index](./README.md) · [Next: 10 Clients & configuration →](./10-clients-and-config.md)

---

Hooks and streams are the two surfaces that connect a running workflow
to the outside world. Hooks let *external systems push into* a
workflow (webhooks, manual approvals); streams let a workflow *emit
data to consumers* in real time (logs, partial results, agent
outputs).

This page covers their lifecycle, on-Redis representation, and the
delivery semantics that make them durable.

---

## 9.1 Hooks

### 9.1.1 What a hook is

In the WDK programming model:

```ts
const hook = await createHook({
  isWebhook: true,
  metadata: { purpose: "approval" }
});
// hook.token is a URL-safe string the workflow now waits on

await hook.received;     // suspends until external party POSTs the webhook
```

A hook is a **named suspension point** with a globally-unique token.
The token can be put in a URL and sent to an external party (a webhook,
an email link), and posting to that URL resumes the workflow.

### 9.1.2 The token claim

Tokens must be unique within a deployment — two runs can't claim the
same one or you'd have ambiguous resumption. `world-redirect` enforces
this with a single Redis SETNX:

```
   events.create(hook_created, eventData:{token, ...})
        │
        ▼
   SET owf:hook:tok:<sha256(token)>  <hookId>  NX
        │
   ┌────┴────────────┐
   nil               OK
   │                 │
   ▼                 ▼
   loser              winner
   write              write hook:<hookId>
   hook_conflict      ZADD hooks:run:<runId>
   event              continue normally
   no hook persisted
```

The hash of the token (rather than the token itself) is the lookup
key. This means tokens stay unobservable to anyone reading Redis
directly, while NX semantics remain atomic.

### 9.1.3 Hook lifecycle diagram

```
   in-workflow:
     const h = await createHook({isWebhook:true})

      ┌──────────────────────────────────────────────────┐
      │  events.create(hook_created)                     │
      │    SET hook:tok:<sha256> NX                      │
      │    if wins:                                      │
      │      HSET hook:<hookId>                          │
      │      ZADD hooks:run:<runId>                      │
      │    if loses:                                     │
      │      event records hook_conflict (no hook)       │
      └──────────────────┬───────────────────────────────┘
                         │
                         ▼
                   workflow suspended on:
                       events.create(wait_created)
                       (no message enqueued — the wait has no resumeAt)
                         │
                         ▼
                   chain ends, function released

      external system POSTs the webhook URL:
                         │
                         ▼
      ┌──────────────────────────────────────────────────┐
      │  webhook handler                                  │
      │  /.well-known/workflow/v1/webhook/<token>         │
      │                                                   │
      │  GET hook:tok:<sha256(token)> → hookId            │
      │  GET hook:<hookId> → {runId, ...}                 │
      │  events.create(hook_received,                     │
      │                eventData:{payload})               │
      │  events.create(wait_completed)                    │
      │  queue("__wkf_workflow_<wfName>",                 │
      │        {runId, resume: hookId})  ZADD jobs now    │
      │  respond 200                                      │
      └──────────────────┬───────────────────────────────┘
                         │
                         ▼
                   dispatcher picks up next tick
                   workflow body replays past the wait
                   and continues from the hook
```

### 9.1.4 Hook disposal

Hooks are disposed when:

```
   1. The workflow calls hook.dispose() explicitly
        → events.create(hook_disposed)

   2. The run reaches terminal state (completed/failed/cancelled)
        → terminal cleanup walks hooks:run:<runId> and deletes each

   3. The hook is consumed (some hooks are one-shot, controlled by
      the runtime — e.g. webhook receives a single POST and disposes)
```

```
   events.create(hook_disposed, correlationId:hookId)
        │
        ▼
   GET hook:<hookId> → {token}
   DEL hook:tok:<sha256(token)>     ← release the token claim
   DEL hook:<hookId>
   ZREM hooks:run:<runId> <hookId>
   SET event ...                      ← log the disposal
```

After disposal, a webhook POST to the same token will 404 (no claim
key, no hook).

### 9.1.5 The `hook_conflict` recovery path

```
   workflow A: createHook(token: "shared")
        → wins claim, hook persisted

   workflow B: createHook(token: "shared")
        → loses claim
        → hook_conflict event recorded for B
        → B's createHook() returns null (or throws, depending on runtime)
        → user code handles the conflict — e.g. retries with a different token

   workflow A finishes / hook_disposed
        → claim released

   workflow C: createHook(token: "shared")
        → wins, hook persisted
```

The conflict event lives on in B's event log even though no hook
entity exists for it. This is important for debugging — "why did this
workflow not get a hook?" is answerable from the log.

### 9.1.6 Webhook routes

Two paths typically expose the same webhook handler:

```
   /.well-known/workflow/v1/webhook/<token>     canonical (generated by
                                                  the WDK SWC plugin)

   /api/wf/webhook/<token>                      mirror (used in
                                                  OpenNext / EdgeOne
                                                  deploys where the
                                                  .well-known directory
                                                  is stripped)
```

Both routes call the same `webhookHandler` implementation. See
[11 · Deployment](./11-deployment.md) for when each is required.

---

## 9.2 Streams

### 9.2.1 What streams are

Inside a workflow:

```ts
const out = getStream("out");
await out.write("first chunk\n");
await out.write("second chunk\n");
// chunks become readable to outside consumers immediately
// (via dashboard, RPC, or direct world.streams.get)
```

Streams are durable, replayable, and live-tailable. They're the WDK's
answer to "how do I show partial output to a user while the workflow
is still running?".

### 9.2.2 On-Redis representation (recap)

```
   stream:<runId>                       SET     all stream names for run
   stream:<runId>:<name>:chunks         LIST    base64(bytes) per chunk
   stream:<runId>:<name>:done           STRING  "1" when closed
   stream:<runId>:<name>:channel        PUB/SUB live chunk fanout
```

### 9.2.3 Writer side

```
   streams.write(runId, name, chunk)
        │
        ▼
   1. RPUSH stream:<R>:<n>:chunks  base64(chunk)
   2. SADD  stream:<R>             name           (idempotent registration)
   3. PUBLISH stream:<R>:<n>:channel <chunkIdx, base64>
```

Writes are durably persisted before the publish, so a late subscriber
can backfill via LRANGE and pick up live updates via SUBSCRIBE — see
the reader.

### 9.2.4 Reader side — live tail

```
   streams.get(runId, name, startIndex=0)
        │
        ▼
   returns ReadableStream<Uint8Array>:

   ┌──────────────────────────────────────────────────────────┐
   │  initial backfill (replay buffer)                         │
   │    LRANGE stream:<R>:<n>:chunks  startIndex  -1           │
   │    for each chunk: controller.enqueue(decode(chunk))      │
   │  cursor = startIndex + backfilled count                   │
   │                                                           │
   │  if stream:<R>:<n>:done == "1":                            │
   │    controller.close(); return                             │
   │                                                           │
   │  live subscribe                                           │
   │    SUBSCRIBE stream:<R>:<n>:channel                        │
   │    on each message:                                       │
   │      idx = parse(msg).chunkIdx                            │
   │      if idx == EOF: controller.close()                    │
   │      else if idx >= cursor:                                │
   │        controller.enqueue(parse(msg).data)                │
   │        cursor = idx + 1                                   │
   │                                                           │
   │  on cancel:                                               │
   │    UNSUBSCRIBE; release                                   │
   └──────────────────────────────────────────────────────────┘
```

If `startIndex` is negative, it's interpreted relative to the current
tail (`-1` = last chunk; `-10` = last 10 chunks).

### 9.2.5 Reader side — Upstash REST fallback

Upstash REST does not support persistent PUB/SUB connections (HTTP is
request/response). The streamer falls back to polling:

```
   ┌──────────────────────────────────────────────────────────┐
   │  Polling reader                                           │
   │                                                           │
   │  every flushIntervalMs (default 2 s):                     │
   │    chunks = LRANGE stream:<R>:<n>:chunks  cursor  -1      │
   │    if chunks present:                                     │
   │      enqueue each                                         │
   │      cursor += chunks.length                              │
   │    if stream:<R>:<n>:done == "1":                          │
   │      controller.close(); break                            │
   └──────────────────────────────────────────────────────────┘
```

Latency is the poll interval; throughput is bounded by Upstash REST
op limits. Fine for most observability streams; not ideal for
high-throughput log fanout.

### 9.2.6 Stream lifecycle

```
   open ────── streams.write ───────▶ open with N chunks
                                           │
                                           │ streams.close (or terminal)
                                           ▼
                                       closed
                                           │
                                           │ terminal cleanup
                                           ▼
                                       (chunks deleted; replay
                                        no longer possible —
                                        UI shows "done" only)
```

### 9.2.7 The pub/sub format

```
   PUBLISH payload (per chunk):
     {
       "i": <chunkIdx>,         // integer
       "b": "<base64 of bytes>" // optional; absent on EOF
     }

   PUBLISH payload on close:
     {
       "i": <finalIdx + 1>,
       "eof": true
     }
```

Subscribers parse the JSON and act on `i` / `eof`. Format chosen to be
trivially serialisable on every Redis client without protobuf
ceremony.

---

## 9.3 Combined hook + stream — a worked example

A workflow that uses both — say, a long-running "agent" workflow that
streams partial output and pauses for human approval:

```ts
"use workflow";
import { step, getStream, createHook } from "workflow";

export default async function review() {
  const out = getStream("out");

  await step("draft", async () => {
    for (let i = 0; i < 10; i++) {
      await out.write(`draft chunk ${i}\n`);
    }
  });

  const hook = await createHook({ isWebhook: true });
  await out.write(`Awaiting approval at ${hook.url}\n`);

  const { approved } = await hook.received;  // suspends
  if (!approved) return { status: "rejected" };

  return await step("publish", async () => {
    // …actually publish…
  });
}
```

Timeline:

```
   t0   start
        run_created, queue mA
   t0+ε dispatcher → flow handler
        run_started
        step(draft):
          step_created
          step_started
          ×10 streams.write → 10 RPUSHes + 10 PUBLISHes
          step_completed
        createHook:
          hook_created event, NX claim wins
          hook:<hookId> persisted
        streams.write "Awaiting approval at <url>\n"
        wait_created (waiting on hook)
        respond 200 (chain ends)

   ╳ ╳ wait for external approval ╳ ╳

   tN  external party POSTs /webhook/<token>
       webhook handler:
         hook_received event (with payload {approved:true})
         wait_completed
         queue mB
         200

   tN+ε dispatcher → flow handler
        replays event log
        hook.received resolves with {approved:true}
        step(publish):
          step_created
          step_started
          ...
          step_completed
        run_completed
        streams.close("out")
        200 (chain ends)
```

The dashboard reader of stream "out" sees the 10 draft chunks land
live, then "Awaiting approval at …", then a pause, then any further
chunks written by `publish`, then EOF.

---

## 9.4 Failure modes specific to hooks & streams

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Mode                            How world-redirect handles it  │
   ├─────────────────────────────────────────────────────────────────┤
   │  Hook NX claim race lost         hook_conflict event; no hook    │
   │  Webhook POST after disposal     404 from handler                │
   │  Webhook POST while run is       handler still finds hook by    │
   │  terminal-but-not-cleaned        token, but events.create        │
   │                                  rejects with EntityConflictError│
   │                                  on "create on terminal run"     │
   │                                                                  │
   │  Stream subscriber connects      backfill + live subscribe       │
   │  late                            (replay buffer + pub/sub)       │
   │  Pub/sub down (Upstash)          fall back to polling             │
   │  Writer's RPUSH succeeds but     subscribers still get the chunk │
   │  PUBLISH fails                   on next poll/backfill            │
   │  Late subscriber after close     reads replay buffer then sees   │
   │                                  done=1 → close                  │
   └─────────────────────────────────────────────────────────────────┘
```

The split between durable RPUSH and ephemeral PUBLISH is intentional:
PUBLISH delivery is best-effort, but the buffer is always there to
backfill. Subscribers always get all the chunks, just possibly with
some latency if the live channel hiccups.

---

[← 08 Failure & retry](./08-failure-and-retry.md) · [Index](./README.md) · [Next: 10 Clients & configuration →](./10-clients-and-config.md)
