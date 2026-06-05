# 11 · Deployment

[← 10 Clients & configuration](./10-clients-and-config.md) · [Index](./README.md)

---

`world-redirect` is just a Node module — it doesn't host the HTTP
endpoints itself. This page enumerates the four shapes a deployment
typically takes, plus the dashboard.

```
   ┌─────────────────────────────────────────────────────────────┐
   │                                                             │
   │  Deployment topologies                                      │
   │                                                             │
   │  1. Standalone Node host (the bundled @open-workflow/host)  │
   │  2. Next.js (any platform that runs Next on Node)           │
   │  3. EdgeOne Pages / OpenNext (Cloudflare Pages, etc)        │
   │  4. Lambda / serverless functions                           │
   │                                                             │
   │  + The dashboard (@workflow/web) — runs anywhere Node runs  │
   │                                                             │
   └─────────────────────────────────────────────────────────────┘
```

---

## 11.1 Standalone host

The simplest topology: one long-running Node process serves the flow,
step, and webhook endpoints, **and** runs the dispatcher.

```
   ┌────────────────────────────────────────────────────────────┐
   │     Node process (e.g. PM2 / systemd / Docker container)   │
   │                                                            │
   │   @open-workflow/host  (Express-like router)               │
   │     ├── POST /.well-known/workflow/v1/flow                 │
   │     ├── POST /.well-known/workflow/v1/step                 │
   │     └── POST /.well-known/workflow/v1/webhook/:token       │
   │                                                            │
   │   world-redirect                                           │
   │     ├── Storage  ────▶ ┐                                   │
   │     ├── Queue + dispatcher                                 │
   │     └── Streamer ────▶ │                                   │
   └───────────────────────┼─────────────────────────────────────┘
                           ▼
                   ┌──────────────────┐
                   │      Redis       │
                   └──────────────────┘
```

Setup:

```bash
cd examples/standalone
pnpm build       # SWC compiles workflows → .well-known/workflow/v1/
WORKFLOW_REDIS_URL=redis://localhost:6379 pnpm host
```

Self-dispatching loop:

```
   handler                                  dispatcher (same process)
        │                                            │
        │ writes msg via world.queue                 │
        │ ────────────────────────▶  Redis           │
        │                                            │
        │                                            │ next tick:
        │                                            │ ZRANGEBYSCORE
        │                                            │ → claims msg
        │                                            │
        │                            ◀ fetch ──── POST /flow
        │                                            │
        ▼                                            ▼
   handles, returns 307                       fetch follows 307,
   or 200                                     same as before
```

Best for: self-hosted, dev environments, demos, single-tenant SaaS.

---

## 11.2 Next.js

Use `withWorkflow` from `@workflow/next` to register the routes during
Next's build:

```ts
// next.config.ts
import { withWorkflow } from "workflow/next";
process.env.WORKFLOW_TARGET_WORLD ||= "@open-workflow/world-redirect";

export default withWorkflow({
  serverExternalPackages: ["@open-workflow/world-redirect"],
});
```

The SWC plugin generates:

```
   app/.well-known/workflow/v1/flow/route.ts
   app/.well-known/workflow/v1/step/route.ts
   app/.well-known/workflow/v1/webhook/[token]/route.ts
```

Each route imports the world via `WORKFLOW_TARGET_WORLD` and wires the
queue handler.

```
   Browser / external system
        │
        ▼
   Next.js / Node serverless function
        │
        ▼
   /.well-known/workflow/v1/flow?msg=…
        │
        ▼
   world.createQueueHandler  → ... → 307 / 200

   Dispatcher:
       on application startup (singleton cache),
       world.start() is called once → pump runs in this Node process
       (or, if serverless, dispatched out-of-process; see EdgeOne notes)
```

For Vercel-without-WDK, Cloud Run, or any platform that hosts Next on
durable Node containers, the dispatcher in-process is fine.

For platforms that recycle the process between requests (Vercel
functions, Lambda), in-process dispatcher dies between invocations —
move it to a separate worker (see 11.5).

---

## 11.3 EdgeOne Pages / OpenNext

EdgeOne Pages (and other OpenNext-based platforms — Cloudflare Pages,
Netlify Edge) introduce four issues that `@workflow/next` doesn't
anticipate:

```
   ┌────────────────────────────────────────────────────────────────┐
   │  Four issues                                                   │
   ├────────────────────────────────────────────────────────────────┤
   │                                                                │
   │  1. OpenNext sets VERCEL=1 / VERCEL_DEPLOYMENT_ID for compat   │
   │     → withWorkflow takes the Vercel build path                 │
   │     → import gets hashed: @workflow/world-vercel-<hash>        │
   │     → fails at runtime                                         │
   │                                                                │
   │  2. WORKFLOW_NEXT_LAZY_DISCOVERY defaults on                   │
   │     → routes resolved at runtime via dynamic import            │
   │     → that mechanism does not survive OpenNext transform       │
   │     → 404 for /.well-known/workflow/v1/flow                    │
   │                                                                │
   │  3. OpenNext strips dot-prefix directories from artifact       │
   │     → /.well-known/* routes don't exist at runtime even when   │
   │       generated eagerly                                        │
   │                                                                │
   │  4. Next's rewrites() default to afterFiles                    │
   │     → the eager .well-known route is still in the manifest,    │
   │       so Next tries to require its (stripped) handler before   │
   │       falling back to a rewrite                                │
   └────────────────────────────────────────────────────────────────┘
```

`@open-workflow/world-edgeone` bundles the workarounds. Wrap your
config with `withEdgeOneWorkflow` instead of `withWorkflow`:

```ts
// next.config.ts
import { withEdgeOneWorkflow } from "@open-workflow/world-edgeone/next";

export default withEdgeOneWorkflow({
  reactStrictMode: false,
});
```

What that does:

```
   1. delete VERCEL_*  env vars (defeats Vercel-mode detection)
   2. set WORKFLOW_NEXT_LAZY_DISCOVERY=0  (eager route generation)
   3. add @open-workflow/world-{edgeone,redirect} to
      serverExternalPackages (Next bundling)
   4. install beforeFiles rewrites:
        /.well-known/workflow/v1/flow      → /api/wf/flow
        /.well-known/workflow/v1/webhook/* → /api/wf/webhook/*
   5. wrap with the upstream withWorkflow
```

You also need (one-time setup):

- `templates/vendor/world-vercel-shim/` — a thin
  `@workflow/world-vercel` shim that redirects into `world-redirect`,
  vendored and pinned via `package.json#resolutions`.
- `templates/app/api/wf/{flow,webhook/[token]}/route.ts` — mirror
  routes; tiny re-exports that survive the dot-prefix strip.

See the world-edgeone package README for the full setup.

The dispatcher topology:

```
   EdgeOne Pages function (request-scoped)
   ──────────────────────────────────────
         dispatchers don't survive between
         requests → run the dispatcher
         from a separate worker host (a
         Cloud Run service, a Node container,
         or anywhere you can keep a process
         alive)

   The worker:
     WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect
     WORKFLOW_REDIS_REST_URL=…
     WORKFLOW_REDIS_REST_TOKEN=…
     WORKFLOW_BASE_URL=https://my-edgeone-domain.com  (where /flow lives)
     WORKFLOW_DEPLOYMENT_ID=dpl_edgeone_prod

   The Pages function:
     same target world + REST credentials,
     but WORKFLOW_REDIS_DISABLE_DISPATCHER=1
     (no in-process pump — the worker handles dispatch)
```

---

## 11.4 Lambda / serverless functions

Same topology as EdgeOne — request-scoped functions can't keep a pump
alive. Run an external dispatcher (a small always-on container) and
disable in-process dispatching on the function:

```
   ┌────────────────────────────┐         ┌─────────────────────────────┐
   │   Lambda / function host   │         │  Always-on worker            │
   │   - serves /flow / /step /  │         │  (Fargate, Cloud Run, EC2,   │
   │     /webhook                │         │   any container)             │
   │   - dispatcher OFF          │         │  - dispatcher ON             │
   │   - reads/writes Redis      │         │  - no HTTP routes            │
   │     for events              │         │  - WORKFLOW_BASE_URL points  │
   │                             │         │    at the function URL       │
   └────────────┬────────────────┘         └──────────────┬──────────────┘
                │                                          │
                └─────────────────┬────────────────────────┘
                                  ▼
                          ┌──────────────────┐
                          │      Redis       │
                          └──────────────────┘
```

The worker only needs `world-redirect` installed; it serves no HTTP
endpoints. A tiny start file:

```ts
import { createWorld } from "@open-workflow/world-redirect";

const world = createWorld();
await world.start();
process.on("SIGTERM", () => world.close());
```

---

## 11.5 The dashboard (`@workflow/web`)

Identical setup regardless of runtime topology — the dashboard just
needs to be able to talk to the same Redis with the same key prefix.

```
   browser
      │
      ▼
   localhost:4000
      │
      ▼
   @workflow/web Express server
      │   (no auth — bind to localhost only, or put a reverse proxy
      │    with auth in front before exposing)
      ▼
   loadWorld(WORKFLOW_TARGET_WORLD)
   → world-redirect with WORKFLOW_REDIS_DISABLE_DISPATCHER=1
      │
      ▼
   Redis
```

Command:

```bash
npm i @workflow/web@beta @open-workflow/world-redirect

WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect \
WORKFLOW_REDIS_REST_URL=https://… \
WORKFLOW_REDIS_REST_TOKEN=… \
WORKFLOW_REDIS_KEY_PREFIX=owf \
WORKFLOW_REDIS_DISABLE_DISPATCHER=1 \
PORT=4000 \
node node_modules/@workflow/web/server.js
```

### 11.5.1 What the dashboard does

Reads:
- `runs.list / .get` — list runs, paginate by status, view one run.
- `steps.list / .get` — see each step's status / input / output.
- `events.list / .listByCorrelationId / .get` — the audit log.
- `streams.get` — live tail any stream of any run.

Writes (operator-initiated):
- `cancelRun(runId)` — write `run_cancelled` event.
- `recreateRun(runId)` — copy the run's input, start a new run.
- `reenqueueRun(runId)` — re-queue a stuck pending/running message.
- `wakeUpRun(runId, waitId)` — write `wait_completed`, re-enqueue.
- `resumeHook(token, payload)` — same effect as a webhook POST.

### 11.5.2 No auth — protect it

`@workflow/web/server.js` has no auth middleware. Don't expose the
port directly to the internet. Options:

```
   ┌────────────────────────────────────────────────────────────┐
   │  Option A: localhost only                                  │
   │    PORT=4000 bound to 127.0.0.1                            │
   │    SSH tunnel from operator machines                       │
   │                                                            │
   │  Option B: reverse proxy with auth                         │
   │    nginx / caddy / Cloudflare Access in front              │
   │    require an SSO login before forwarding                  │
   │                                                            │
   │  Option C: Tailscale / WireGuard                           │
   │    bind to a private VPN interface                         │
   └────────────────────────────────────────────────────────────┘
```

---

## 11.6 Topology summary

```
                     dispatcher          flow handler       dashboard
                     location            location           location
                     ───────────         ────────────       ──────────
   Standalone        in-process           in-process         separate process
                                                              same machine OK

   Next on Node      in-process           in-process         separate process
   (Cloud Run,                                                somewhere
    long-lived)

   Next on Vercel    separate worker      Vercel function    separate process
   / Lambda          (Cloud Run /         (per request)
                     container)

   EdgeOne Pages     separate worker      EdgeOne function   separate process
   / OpenNext        (Cloud Run /         (per request)
                     container)
```

The constant: every host shares the same Redis with the same prefix;
exactly one host runs the dispatcher pump.

---

## 11.7 Sanity-check endpoint

A simple `/api/debug` route is a convention in
`world-redirect`-backed deployments — it reports the world's view of
itself:

```ts
// app/api/debug/route.ts (Next.js example)
import { createWorld } from "@open-workflow/world-redirect";

export async function GET() {
  const world = createWorld();
  const recent = await world.runs.list({
    pagination: { limit: 5, sortOrder: "desc" }
  });
  return Response.json({
    target: process.env.WORKFLOW_TARGET_WORLD,
    label: "upstash-rest",  // or "node-redis", "memory"
    baseUrl: process.env.WORKFLOW_BASE_URL,
    prefix: process.env.WORKFLOW_REDIS_KEY_PREFIX || "owf",
    recentRuns: recent.data,
    error: null,
  });
}
```

Hitting `https://<your-host>/api/debug` should return a JSON blob like:

```json
{
  "target": "@open-workflow/world-redirect",
  "label": "upstash-rest",
  "baseUrl": "https://my-app.example.com",
  "prefix": "owf",
  "recentRuns": [ { "runId": "wrun_…", "status": "completed", … } ],
  "error": null
}
```

A 500 from this endpoint usually means env vars aren't set or the
Redis connection isn't working — investigate before chasing deeper
issues.

---

## 11.8 What's next

You now have the complete picture of `world-redirect`:

- [01 · Overview](./01-overview.md) — why this exists
- [02 · Architecture](./02-architecture.md) — the four layers
- [03 · World contract](./03-world-contract.md) — the runtime-facing API
- [04 · Redis keyspace](./04-redis-keyspace.md) — every key
- [05 · Event sourcing](./05-event-sourcing.md) — the state model
- [06 · 307 dispatch](./06-307-dispatch.md) — the central trick
- [07 · End-to-end flow](./07-end-to-end-flow.md) — it all together
- [08 · Failure & retry](./08-failure-and-retry.md) — when things break
- [09 · Hooks & streams](./09-hooks-and-streams.md) — talking to the world
- [10 · Clients & configuration](./10-clients-and-config.md) — env knobs
- 11 · Deployment — *this page*

For the upstream WDK, see <https://github.com/vercel/workflow>. For the
companion KV-backed world (no Redis required on EdgeOne), see
`@open-workflow/world-edgeone/kv`.

---

[← 10 Clients & configuration](./10-clients-and-config.md) · [Index](./README.md)
