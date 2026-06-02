# @open-workflow/world-edgeone

EdgeOne Pages / OpenNext-flavoured Workflow World. Ships **two backends in
one package** — pick whichever fits your operational model:

| Subpath | Backend | When to pick |
| --- | --- | --- |
| `@open-workflow/world-edgeone` (default) | Redis + 307 trampoline (delegates to [`world-redirect`](../world-redirect)) | High-throughput, multi-host, pub/sub-backed streams, you already have Redis. |
| `@open-workflow/world-edgeone/kv` | **EdgeOne Pages KV** (storage + queue + streams all in KV) | Zero-Redis EdgeOne deploys. Storage is the same KV namespace your function already binds to — no extra service to provision, no creds to rotate. |

The default subpath is the original 0.1.0 behaviour — Redis storage +
307-redirect dispatch — wrapped with every platform-specific workaround we
discovered shipping a real bot to EdgeOne (env-purging, eager discovery,
`beforeFiles` rewrites, the `@workflow/world-vercel` shim, mirror routes
under `/api/wf/*`).

The `/kv` subpath is new in 0.2.0. It removes the Redis dependency entirely
by using EdgeOne's native KV store as the durable substrate for runs,
events, steps, hooks, waits, the scheduler, and stream chunks. See [Using
the KV backend](#using-the-kv-backend) below.

Although named "edgeone", this should also work on other OpenNext-based
deployment targets (Cloudflare Pages, etc.) that share the same dot-prefix
stripping and Vercel-env-injection behaviour. Only EdgeOne is currently
exercised end-to-end.

## Why this package exists

Deploying a workflow Next.js app to EdgeOne Pages hits four issues that
`@workflow/next` doesn't anticipate, each manifesting as an opaque runtime
500 in production. They are, in the order you'd encounter them:

1. **Vercel-mode build path.** OpenNext-based platforms set `VERCEL=1` /
   `VERCEL_DEPLOYMENT_ID` for compatibility, which makes `withWorkflow`
   produce a Turbopack-hashed external import for `@workflow/world-vercel-
   <hash>` that only resolves at runtime on real Vercel.
2. **Lazy route discovery.** `WORKFLOW_NEXT_LAZY_DISCOVERY` defaults to
   on; the deferred resolution mechanism doesn't survive OpenNext's
   transformation, so the flow/webhook route files are absent from the
   deployed function bundle.
3. **`.well-known/` directory strip.** Even with eager mode forcing the
   routes into the source tree, OpenNext (and several other deploy
   pipelines) strip dot-prefix directories from the deploy artifact —
   so `.next/server/app/.well-known/workflow/v1/flow/route.js` ends up
   missing at runtime.
4. **Rewrite phase mismatch.** Next's default `rewrites()` shape puts
   rules in `afterFiles`, which only fires when no route matched — but
   the eager `.well-known` route is still registered in the manifest,
   so Next tries to require its (stripped) handler file before the
   rewrite ever gets a chance.

`withEdgeOneWorkflow` solves 1, 2, and 4 automatically. Issue 3 needs two
small additions in the consumer repo (mirror routes and a shim package),
both of which are shipped as templates inside this package.

## Install

```bash
npm install @open-workflow/world-edgeone @open-workflow/world-redirect workflow
# or yarn / pnpm
```

You also need a real Upstash REST credential pair (or any other Redis the
`world-redirect` clients support). Set them in EdgeOne project settings:

```
WORKFLOW_TARGET_WORLD = @open-workflow/world-edgeone
WORKFLOW_REDIS_REST_URL = <your upstash url>
WORKFLOW_REDIS_REST_TOKEN = <your upstash token>
WORKFLOW_BASE_URL = https://<your-edgeone-domain>
```

## Setup (four small steps)

### 1. Use `withEdgeOneWorkflow` in `next.config.ts`

```ts
import type { NextConfig } from "next";
import { withEdgeOneWorkflow } from "@open-workflow/world-edgeone/next";

const nextConfig: NextConfig = {
  reactStrictMode: false,
};

export default withEdgeOneWorkflow(nextConfig);
```

That handles VERCEL env purging, eager discovery, Upstash credential
mapping, `serverExternalPackages`, and the `beforeFiles` rewrites that
translate `/.well-known/workflow/v1/*` → `/api/wf/*`.

### 2. Switch your build script to webpack

Turbopack generates content-hashed external module IDs that the shim
can't override. Update `package.json`:

```json
"scripts": {
  "build": "next build --webpack"
}
```

### 3. Vendor the `@workflow/world-vercel` shim and pin it via resolutions

Copy `node_modules/@open-workflow/world-edgeone/templates/vendor/world-vercel-shim`
to your repo (e.g. `vendor/world-vercel-shim/`), `npm pack` it to produce
`workflow-world-vercel-5.0.0-beta.7-shim.1.tgz`, and reference the
tarball:

```json
"resolutions": {
  "@workflow/world-vercel": "file:./vendor/workflow-world-vercel-5.0.0-beta.7-shim.1.tgz"
}
```

(npm/pnpm equivalents work too — use `overrides` instead of `resolutions`.)

This makes the missing-package import resolve to a thin redirect into
`@open-workflow/world-redirect`, satisfying the bundle without requiring
the real Vercel package.

### 4. Create mirror routes under `app/api/wf/`

Copy the route templates from
`node_modules/@open-workflow/world-edgeone/templates/app/api/wf/` into
your own `app/api/wf/`:

```
app/api/wf/flow/route.ts
app/api/wf/webhook/[token]/route.ts
```

These are two-line re-exports from the eager-generated `.well-known/...`
routes. Webpack inlines the workflow runtime into the new chunks at build
time, and `/api/*` is a path OpenNext keeps in the deploy artifact. The
`beforeFiles` rewrite from step 1 sends `.well-known` URLs here.

## Configuration

`withEdgeOneWorkflow` accepts an options object:

```ts
withEdgeOneWorkflow(nextConfig, {
  targetWorld: "@open-workflow/world-edgeone", // default
  flowRoutePath: "/api/wf/flow",               // default
  webhookRoutePath: "/api/wf/webhook",         // default
  skipEnvSetup: false,                         // set true to manage env yourself
  skipServerExternals: false,                  // set true to skip serverExternalPackages
});
```

If you pick non-default paths, also rename the route files under
`app/<your-path>` accordingly.

## Verifying the deploy

After the first successful deploy, confirm the world is wired correctly:

```bash
curl https://<your-edgeone-domain>/api/debug
```

You should get JSON back including:

- `target`: `@open-workflow/world-edgeone`
- `label`: `upstash-rest`
- `recentRuns`: any workflow runs that have been created
- `error`: `null`

A `500` or non-JSON response here means one of the four setup steps
above is missing.

## Using the KV backend

Set `WORKFLOW_TARGET_WORLD` to the `/kv` subpath and bind a KV namespace to
your EdgeOne Pages function:

```
WORKFLOW_TARGET_WORLD = @open-workflow/world-edgeone/kv
WORKFLOW_BASE_URL     = https://<your-edgeone-domain>
```

Bind the namespace under the default name `EDGEONE_KV` in your EdgeOne Pages
function settings. If your project already uses that name, set
`WORKFLOW_EDGEONE_KV_BINDING` to the actual binding name and the world will
look it up there instead.

`withEdgeOneWorkflow` still applies — only the world implementation changes,
not the deploy plumbing:

```ts
// next.config.ts
import { withEdgeOneWorkflow } from "@open-workflow/world-edgeone/next";

export default withEdgeOneWorkflow(
  { reactStrictMode: false },
  { targetWorld: "@open-workflow/world-edgeone/kv" }
);
```

### How the KV layout works

Every entity, event, queued job and stream chunk is a single KV key under a
configurable prefix (default `owf`):

```
owf/run/<runId>                              CBOR(WorkflowRun)
owf/idx-run-status/<status>/<runId>          presence (lex-sortable)
owf/step/<runId>/<stepId>                    CBOR(Step)
owf/idx-step/<runId>/<stepId>                presence
owf/evt/<runId>/<eventId>                    CBOR(Event)
owf/hook/<hookId>                            CBOR(Hook)
owf/tok/<sha256(token)>                      hookId           (NX-claim key)
owf/wait/<runId>/<correlationId>             CBOR(Wait)
owf/job/<paddedRunAtMs>/<msgId>              CBOR(QueueJob)
owf/lease/<msgId>                            "1" (TTL 60s — claim lease)
owf/chunk/<runId>/<name>/<paddedIdx>         CBOR(StreamChunk)
```

The padded `runAtMs` in scheduler keys means `list({prefix: 'owf/job/'})`
returns due jobs in chronological order — the dispatcher iterates until it
hits a key whose timestamp portion is greater than `now` and stops.

The hook token claim uses `put({ifNotExists: true})` on `owf/tok/<hash>`;
losers fall through to a `hook_conflict` event, matching the semantics of
the Redis-backed world. EdgeOne KV doesn't have atomic put-if-absent, so the
adapter implements it as a check-then-put — the race window is bounded and
the only operation that relies on it accepts the conflict-event fallback.

### Programmatic use

```ts
import { createKVWorld } from "@open-workflow/world-edgeone/kv";

const world = createKVWorld({
  baseUrl: "https://my-app.example.com",
  // kv: myBoundNamespace,    // override auto-discovery
  // bindingName: "MY_KV",    // or just rename the lookup
  // keyPrefix: "prod",       // namespace inside KV
  dispatcherPollMs: 1500,
  maxAttempts: 10,
});

await world.start();   // launches in-process dispatcher
```

For local development without a real KV, set
`WORKFLOW_EDGEONE_KV_MEMORY=1` and the world falls back to an in-process
`Map` (same shape, same code paths — useful for tests and demos).

### Configuration env vars

| Env var | Purpose |
| --- | --- |
| `WORKFLOW_EDGEONE_KV_BINDING` | Name of the KV namespace binding (default `EDGEONE_KV`). |
| `WORKFLOW_EDGEONE_KV_PREFIX` | Key namespace inside KV (default `owf`). |
| `WORKFLOW_EDGEONE_KV_DISPATCHER_POLL_MS` | Scheduler poll interval. Default `1500`. |
| `WORKFLOW_EDGEONE_KV_DISPATCH_BATCH` | Max in-flight dispatches per tick. Default `8`. |
| `WORKFLOW_EDGEONE_KV_LEASE_SECONDS` | Claim lease TTL. KV minimum is `60`. |
| `WORKFLOW_EDGEONE_KV_MAX_ATTEMPTS` | Max delivery attempts before drop. Default `10`. |
| `WORKFLOW_EDGEONE_KV_RETRY_BASE_MS` | Base backoff on dispatch failure. Default `5000`. |
| `WORKFLOW_EDGEONE_KV_STREAM_FLUSH_MS` | Reported stream flush hint + polling interval. |
| `WORKFLOW_EDGEONE_KV_DISABLE_DISPATCHER` | Set on read-only hosts (dashboards). |
| `WORKFLOW_EDGEONE_KV_MEMORY` | `1` → fall back to in-process Map (dev only). |

### KV trade-offs

- **Lower throughput than Redis.** Every entity is a separate KV op; events
  list and step list both do a list-by-prefix plus N gets. Fine for
  thousands of runs/day, not for tens-of-thousands per minute.
- **Eventual consistency.** EdgeOne / Workers KV writes propagate to read
  nodes within a window (~60s worst case). The in-process `RunMutex`
  serialises per-run writes within a single host; multi-host setups should
  pin a single dispatcher to avoid two hosts racing the same lease window.
- **No pub/sub for live streams.** `streams.get()` polls. Default poll is
  1000ms; tune with `WORKFLOW_EDGEONE_KV_STREAM_FLUSH_MS`.
- **Claim window ≥ 60s.** EdgeOne / Workers KV enforce a 60-second minimum
  on TTL keys, so dispatch leases can't be shorter. A crashed dispatcher
  delays redelivery by up to 60s. Workflow handlers are idempotent
  (event-sourced replay), so re-dispatch after lease expiry is safe.
- **`listByCorrelationId` is a full namespace scan.** Used by debug paths,
  not the hot dispatch loop. Acceptable for low/medium volume.

If those constraints don't fit your workload, stay on the default Redis
subpath — `world-redirect` still gives you the same author-facing API.

## License

Apache-2.0.
