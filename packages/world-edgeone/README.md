# @open-workflow/world-edgeone

EdgeOne Pages / OpenNext-flavoured wrapper around
[`@open-workflow/world-redirect`](../world-redirect). The World implementation
itself (Redis storage + 307-redirect dispatch) is identical to
`world-redirect` — what this package adds is every platform-specific
workaround we discovered shipping a real bot to EdgeOne, bundled into one
`withEdgeOneWorkflow()` helper plus copy-pasteable templates for the parts
that have to live in the consumer repo.

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

## License

Apache-2.0.
