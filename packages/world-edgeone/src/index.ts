/**
 * @open-workflow/world-edgeone — EdgeOne Pages / OpenNext flavour of
 * @open-workflow/world-redirect.
 *
 * The World implementation itself (Redis storage + 307-redirect dispatch) is
 * identical to world-redirect — re-exported here so consumers can refer to a
 * single "edgeone" world both at runtime
 * (`WORKFLOW_TARGET_WORLD=@open-workflow/world-edgeone`) and in their
 * `next.config.ts` (`withEdgeOneWorkflow`). What's bundled on top is the
 * platform-specific Next.js setup that we discovered the hard way while
 * shipping a real app to EdgeOne Pages:
 *
 *   - delete VERCEL_* env vars before withWorkflow runs (the OpenNext layer
 *     re-injects them, which sends @workflow/next down the Vercel build
 *     path and produces a hashed `@workflow/world-vercel-<hash>` external
 *     module that fails to resolve at runtime)
 *   - force eager route discovery (WORKFLOW_NEXT_LAZY_DISCOVERY=0) so
 *     `app/.well-known/workflow/v1/{flow,webhook}/route.js` are written as
 *     real files instead of resolved lazily
 *   - rewrite `/.well-known/workflow/v1/*` -> `/api/wf/*` in `beforeFiles`
 *     so dispatcher POSTs land at non-dot-prefix routes that survive the
 *     OpenNext deploy artifact strip
 *
 * See `templates/` for the matching mirror-route files and the
 * `@workflow/world-vercel` shim referenced by `package.json#resolutions`.
 *
 * Although named "edgeone", this package should also work on other
 * OpenNext-based deployment targets (Cloudflare Pages, etc.) that share the
 * same dot-prefix-stripping and Vercel-env-injection behaviour. Only EdgeOne
 * is currently exercised end-to-end.
 */

export {
  createRedisWorld,
  createWorld,
  NodeRedisClient,
  UpstashRedisClient,
  type RedisClient,
  type RedisWorld,
  type RedisWorldConfig,
} from '@open-workflow/world-redirect';

export {
  withEdgeOneWorkflow,
  type WithEdgeOneWorkflowOptions,
} from './with-edgeone.js';

/**
 * Self-contained KV-backed World. Use the `/kv` subpath to load it via
 * `WORKFLOW_TARGET_WORLD=@open-workflow/world-edgeone/kv`. The programmatic
 * factory is also re-exported here as `createKVWorld` for callers that
 * construct their world explicitly (tests, hosts, dashboards).
 */
export {
  createKVWorld,
  type KVWorld,
  type KVWorldConfig,
  type KV,
  type KVNamespaceLike,
  InMemoryKV,
  adaptNamespace,
  resolveEdgeOneKV,
} from './kv/index.js';
