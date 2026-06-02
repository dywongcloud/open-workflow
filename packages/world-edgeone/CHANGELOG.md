# @open-workflow/world-edgeone

## 0.1.0 — 2026-06-02

Initial release. Bundles the EdgeOne Pages / OpenNext-specific workarounds
discovered while shipping a real bot to production:

- `withEdgeOneWorkflow(nextConfig, options?)` — Next.js config helper that
  purges Vercel-detection env vars, pins `WORKFLOW_TARGET_WORLD` +
  `WORKFLOW_NEXT_LAZY_DISCOVERY=0`, maps `UPSTASH_REDIS_REST_*` onto
  `WORKFLOW_REDIS_REST_*`, adds `@open-workflow/world-{edgeone,redirect}` to
  `serverExternalPackages`, installs `beforeFiles` rewrites mapping
  `/.well-known/workflow/v1/*` → `/api/wf/*`, then wraps with the upstream
  `withWorkflow`.

- `templates/vendor/world-vercel-shim/` — a thin `@workflow/world-vercel`
  shim that redirects to `@open-workflow/world-redirect`, for consumers to
  vendor and reference via `package.json#resolutions` (or `overrides`).

- `templates/app/api/wf/{flow,webhook/[token]}/route.ts` — mirror route
  files that re-export the eager-generated `.well-known/workflow/v1/*`
  handlers from a non-dot-prefix path that survives OpenNext's deploy
  artifact strip.

The World implementation itself (Redis storage + 307-redirect dispatch) is
identical to `@open-workflow/world-redirect@0.2.0` and is re-exported from
this package so consumers can name one world at runtime
(`WORKFLOW_TARGET_WORLD=@open-workflow/world-edgeone`) and at build
(`withEdgeOneWorkflow`).
