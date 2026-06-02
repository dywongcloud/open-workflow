import type { NextConfig } from 'next';
import { withWorkflow } from 'workflow/next';

/**
 * Options for {@link withEdgeOneWorkflow}.
 */
export interface WithEdgeOneWorkflowOptions {
  /**
   * Value to assign to `WORKFLOW_TARGET_WORLD` at build time. Defaults to
   * `@open-workflow/world-edgeone` (which itself re-exports the
   * world-redirect implementation, so this resolves to the same Redis-backed
   * World at runtime).
   */
  targetWorld?: string;

  /**
   * Path the dispatcher should hit. Must match the route file you create at
   * `app/<flowRoutePath>/route.ts`. Default: `/api/wf/flow`.
   */
  flowRoutePath?: string;

  /**
   * Base path for the webhook routes. The `:token` segment is appended
   * automatically by the rewrite. Default: `/api/wf/webhook`.
   */
  webhookRoutePath?: string;

  /**
   * If you need to manage the env yourself (e.g. tests), set this to `true`
   * and the helper skips its env-var preparation. Defaults to `false`.
   */
  skipEnvSetup?: boolean;

  /**
   * If you've already arranged a different `@workflow/world-vercel`
   * resolution and want this helper not to add `@open-workflow/world-*`
   * packages to `serverExternalPackages`, set this to `true`. Defaults to
   * `false`.
   */
  skipServerExternals?: boolean;
}

/**
 * Wrap a Next.js config for deployment to EdgeOne Pages (or any OpenNext-
 * based platform). Bundles every platform-specific workaround we found
 * shipping a real bot to EdgeOne:
 *
 * 1. Clears `VERCEL_*` env vars so `withWorkflow` does NOT go down its
 *    Vercel build path (which produces a Turbopack-hashed external import
 *    for `@workflow/world-vercel-<hash>` that only resolves at runtime on
 *    real Vercel).
 * 2. Pins `WORKFLOW_TARGET_WORLD` and forces
 *    `WORKFLOW_NEXT_LAZY_DISCOVERY=0` so the flow/webhook routes are
 *    materialised under `app/.well-known/workflow/v1/{flow,webhook}/route.js` instead of
 *    resolved lazily (the deferred mechanism doesn't survive OpenNext's
 *    transformation).
 * 3. Maps `UPSTASH_REDIS_REST_URL` / `_TOKEN` onto `WORKFLOW_REDIS_REST_*`
 *    so apps that already use Upstash for other things don't need extra
 *    credentials.
 * 4. Adds `@open-workflow/world-{edgeone,redirect}` to
 *    `serverExternalPackages` so the Redis clients aren't bundled.
 * 5. Adds `beforeFiles` rewrites mapping the canonical
 *    `/.well-known/workflow/v1/*` URLs onto non-dot-prefix mirror routes
 *    (`/api/wf/*` by default). EdgeOne / OpenNext strip `.well-known/`
 *    directories from the deploy artifact, so the dispatcher's POSTs and
 *    any external webhook calls would otherwise 500 with a missing
 *    `route.js`. `beforeFiles` ensures the rewrite fires BEFORE Next tries
 *    to match the (now-missing) original route.
 * 6. Wraps the result with the upstream `withWorkflow`.
 *
 * The helper does NOT (and can't) automate two manual steps you still need
 * to perform on the consumer side:
 *
 *   a. Switch your build script to `next build --webpack`. Turbopack
 *      generates content-hashed external module IDs that the shim can't
 *      override; webpack uses stable names.
 *
 *   b. Add a `resolutions` (yarn) or `overrides` (npm/pnpm) entry pointing
 *      `@workflow/world-vercel` at the shim shipped under
 *      `node_modules/@open-workflow/world-edgeone/templates/vendor/world-vercel-shim`.
 *      See the package README and `templates/` for the copy-pasteable setup.
 *
 *   c. Copy the mirror route templates from
 *      `node_modules/@open-workflow/world-edgeone/templates/app/api/wf/*`
 *      into your own `app/api/wf/*` so the rewrites have something to land
 *      on. Webpack inlines the eager-generated handlers into those chunks
 *      at build time, and `/api/*` is a path OpenNext keeps in the deploy
 *      artifact.
 *
 * @example
 * ```ts
 * // next.config.ts
 * import { withEdgeOneWorkflow } from '@open-workflow/world-edgeone/next';
 *
 * export default withEdgeOneWorkflow({
 *   reactStrictMode: true,
 * });
 * ```
 */
export function withEdgeOneWorkflow(
  nextConfig: NextConfig = {},
  options: WithEdgeOneWorkflowOptions = {}
): ReturnType<typeof withWorkflow> {
  const flowPath = (options.flowRoutePath ?? '/api/wf/flow').replace(/\/$/, '');
  const webhookBase = (options.webhookRoutePath ?? '/api/wf/webhook').replace(
    /\/$/,
    ''
  );
  const targetWorld = options.targetWorld ?? '@open-workflow/world-edgeone';

  if (!options.skipEnvSetup) {
    // (1) Vercel-mode build path is triggered by these — OpenNext re-injects
    // some of them; clear here so withWorkflow's detection runs against a
    // clean slate.
    for (const k of [
      'VERCEL_DEPLOYMENT_ID',
      'VERCEL',
      'VERCEL_ENV',
      'VERCEL_URL',
      'VERCEL_PROJECT_ID',
    ] as const) {
      delete process.env[k];
    }

    // (2) Force the world + eager route discovery.
    process.env.WORKFLOW_TARGET_WORLD = targetWorld;
    process.env.WORKFLOW_NEXT_LAZY_DISCOVERY = '0';

    // (3) Convenience: reuse Upstash REST creds if present.
    if (
      !process.env.WORKFLOW_REDIS_REST_URL &&
      process.env.UPSTASH_REDIS_REST_URL
    ) {
      process.env.WORKFLOW_REDIS_REST_URL = process.env.UPSTASH_REDIS_REST_URL;
    }
    if (
      !process.env.WORKFLOW_REDIS_REST_TOKEN &&
      process.env.UPSTASH_REDIS_REST_TOKEN
    ) {
      process.env.WORKFLOW_REDIS_REST_TOKEN =
        process.env.UPSTASH_REDIS_REST_TOKEN;
    }

    // BASE_URL fallback (build-time only; at runtime EdgeOne env vars win).
    process.env.WORKFLOW_BASE_URL ||=
      process.env.APP_BASE_URL ||
      `http://localhost:${process.env.PORT ?? 3000}`;
  }

  // (4) Merge serverExternalPackages without dropping user's entries.
  const userExternals = Array.isArray(nextConfig.serverExternalPackages)
    ? nextConfig.serverExternalPackages
    : [];
  const externals = options.skipServerExternals
    ? userExternals
    : Array.from(
        new Set([
          ...userExternals,
          '@open-workflow/world-redirect',
          '@open-workflow/world-edgeone',
        ])
      );

  // (5) Merge rewrites. Preserve user's rewrites — push them after ours so
  // ours always run first.
  const userRewrites = nextConfig.rewrites;
  const merged: NextConfig = {
    ...nextConfig,
    serverExternalPackages: externals,
    async rewrites() {
      const ours = [
        {
          source: '/.well-known/workflow/v1/flow',
          destination: flowPath,
        },
        {
          source: '/.well-known/workflow/v1/webhook/:token',
          destination: `${webhookBase}/:token`,
        },
      ];

      if (!userRewrites) {
        return { beforeFiles: ours, afterFiles: [], fallback: [] };
      }

      const u = await userRewrites();
      if (Array.isArray(u)) {
        // User returned the flat-array shape. Keep their rules in afterFiles
        // so ours still fire before route matching.
        return { beforeFiles: ours, afterFiles: u, fallback: [] };
      }
      return {
        beforeFiles: [...ours, ...(u.beforeFiles ?? [])],
        afterFiles: u.afterFiles ?? [],
        fallback: u.fallback ?? [],
      };
    },
  };

  // (6) Hand off to the upstream withWorkflow.
  return withWorkflow(merged);
}
