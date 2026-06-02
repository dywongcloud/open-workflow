# open-workflow

**Vendor-agnostic durable workflows for JavaScript/TypeScript.**

`open-workflow` is a drop-in, self-hostable backend for the [Workflow
Development Kit](https://github.com/vercel/workflow) (WDK). You write the exact
same workflow code — `"use workflow"` / `"use step"`, `sleep`, `step`,
`createHook`, `getRun`, `FatalError`/`RetryableError`, streams — but instead of
a proprietary, billed queue, execution runs on **any Redis** (self-hosted,
ElastiCache, Upstash, …) using **307 redirects + query params** as the
dispatch mechanism.

> The developer experience is identical. The only thing that changes is the
> backend: set `WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect` and point it
> at a Redis. No code changes to your workflows.

## Why

The WDK is excellent, but its production backend (`@workflow/world-vercel`) ties
you to Vercel's managed queue and storage — which is expensive and
vendor-locking. The WDK's architecture, however, is cleanly layered: the
developer-facing API and the SWC compiler talk only to an abstract **`World`**
interface (`World = Queue & Streamer & Storage`). All the lock-in lives in the
`World` implementation.

`open-workflow` reuses the upstream, Apache-2.0, vendor-neutral packages
unchanged (`@workflow/core`, the SWC compiler, the framework adapters, the
dashboard) and supplies the one piece that was locked in: a Redis-backed
`World`.

## How it works — Redis + 307

| WDK concept | open-workflow |
| --- | --- |
| Durable event log + run/step/hook/wait entities | Redis (CBOR blobs + sorted-set / list indexes, atomic transitions) |
| Stream chunks | Redis lists + pub/sub (live tailing), polling fallback |
| Queue / scheduler | Redis sorted set keyed by `runAt` (handles `sleep`, retries, delays) |
| Queue dispatch (the broker) | **307 redirect trampoline** + query params |

When a workflow needs to continue (after a step, suspension, or wake-up), the
flow handler returns a **`307 Temporary Redirect`** to itself with the next
durable job referenced in the query string
(`/.well-known/workflow/v1/flow?msg=…`). The dispatcher follows it (fetch
preserves method + body across 307s), so a whole run advances in a single
redirect-followed request chain — short invocations, no broker on the hot path.
Delayed work (sleeps, retry backoff) is parked in a Redis sorted set and picked
up by a lightweight poll. Every job is durable and processing is idempotent
(event-log replay), so the model is safely at-least-once.

```
start(wf) ──> Redis schedule ──> dispatcher ──POST?msg=A──> flow handler
                                     ▲                          │ writes events, re-enqueues B (no delay)
                                     │                          ▼
                                     └────────307 ?msg=B&hop=1──┘   (same request chain)
```

## Packages

| Package | Description |
| --- | --- |
| [`@open-workflow/world-redirect`](packages/world-redirect) | The Redis `World`: storage (event-sourcing), the 307 dispatcher + scheduler, the streamer. Supports node-redis (RESP), Upstash REST, and a zero-setup in-memory client. (Renamed from `@open-workflow/world-redis` in 0.2.0 — the dispatch model is the defining feature, not the storage.) |
| [`@open-workflow/world-edgeone`](packages/world-edgeone) | EdgeOne Pages / OpenNext-flavoured Workflow World. The default subpath wraps `world-redirect` and bundles a `withEdgeOneWorkflow()` Next.js config helper plus copy-pasteable templates for the `@workflow/world-vercel` shim and the `/api/wf/*` mirror routes needed to deploy through OpenNext-based platforms. The `/kv` subpath ships a self-contained KV-backed World (storage + queue + streams all in EdgeOne Pages KV) — zero Redis dependency. |
| [`@open-workflow/world-zeplo`](packages/world-zeplo) | Workflow World that uses [Zeplo](https://zeplo.io) as the queue and reuses `world-redirect`'s Redis storage. Replaces the 307 trampoline + in-process dispatcher with Zeplo's hosted HTTP queue — ideal for serverless / function-per-invocation platforms where you can't keep a long-running pump alive. |
| [`@open-workflow/world-sheets`](packages/world-sheets) | A Workflow World where every entity is a row in a Google Sheet — runs, events, steps, hooks, waits, queued jobs, and stream chunks all stored in dedicated tabs. Built for low-volume ops / approval workflows where non-engineers want to read, filter, and comment on workflow state directly. Sheets-API rate-limited (~60 ops/min). |
| [`@open-workflow/host`](packages/host) | A self-hostable Node HTTP host that serves the workflow flow/webhook endpoints and runs the 307 dispatch pump. |
| [`open-workflow`](packages/open-workflow) | 1:1 facade re-exporting the `workflow` developer API, plus `open-workflow/redis` for backend construction. |

## Quick start

### Standalone (no framework)

```bash
cd examples/standalone
pnpm build                      # compiles workflows -> .well-known/workflow/v1
WORKFLOW_REDIS_URL=redis://localhost:6379 pnpm host   # serves + dispatches
# in another shell, trigger by name (canonical):
WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect \
WORKFLOW_REDIS_URL=redis://localhost:6379 \
  pnpm exec workflow start hello '"World"'
```

Or run the scripted end-to-end demo (zero setup — uses the in-memory world):

```bash
cd examples/standalone && pnpm build && WORKFLOW_REDIS_URL=memory pnpm demo
```

### Next.js

```ts
// next.config.ts
import { withWorkflow } from 'workflow/next';
process.env.WORKFLOW_TARGET_WORLD ||= '@open-workflow/world-redirect';
export default withWorkflow({ serverExternalPackages: ['@open-workflow/world-redirect'] });
```

```bash
cd examples/nextjs
cp .env.local.example .env.local   # set WORKFLOW_REDIS_URL
pnpm dev
curl -XPOST localhost:3000/api/run -d '{"name":"Ada"}'
curl 'localhost:3000/api/status?runId=<runId>'
```

Your workflow files import from `workflow` exactly as in the WDK — there is no
open-workflow-specific authoring API.

## Dashboard

The WDK's observability dashboard (`@workflow/web`) works unchanged — point it
at the same Redis:

```bash
WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect \
WORKFLOW_REDIS_URL=redis://localhost:6379 \
WORKFLOW_REDIS_DISABLE_DISPATCHER=1 \
PORT=4000 \
  node node_modules/@workflow/web/server.js
```

It lists runs, steps, events, hooks and streams by reading the Redis `World`
directly. Run it from a directory where `@open-workflow/world-redirect` resolves.

## Configuration

All config is read from the environment (so `createWorld()` needs no arguments),
mirroring the other worlds:

| Env var | Purpose |
| --- | --- |
| `WORKFLOW_TARGET_WORLD` | Set to `@open-workflow/world-redirect` |
| `WORKFLOW_REDIS_URL` | RESP URL (`redis://…`), or `memory` for dev |
| `WORKFLOW_REDIS_REST_URL` / `WORKFLOW_REDIS_REST_TOKEN` | Upstash REST credentials |
| `WORKFLOW_REDIS_KEY_PREFIX` | Key namespace (default `owf`) |
| `WORKFLOW_BASE_URL` | Where the dispatcher posts (your server's URL) |
| `WORKFLOW_DEPLOYMENT_ID` | Reported by `getDeploymentId()` |

See [`packages/world-redirect`](packages/world-redirect) for the full list (poll
interval, max attempts, retry backoff, dispatcher toggle).

## Testing

```bash
pnpm test                                   # world-redirect unit tests (memory + real Redis)
cd examples/standalone && pnpm test:e2e     # full-stack e2e (host + 307 + webhook resume)
```

The unit suite runs every storage/queue/streamer test against **both** the
in-memory client and a real Redis. The e2e suite builds the example, boots the
host, and exercises steps, durable retries, parallel fan-out, durable sleeps,
and webhook resume through the real flow handler and 307 dispatcher.

## License

Apache-2.0, like the upstream Workflow SDK it builds on.
# open-workflow
