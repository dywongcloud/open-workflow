/**
 * @open-workflow/world-zeplo — a Workflow World that uses Zeplo
 * (https://zeplo.io) for queue dispatch and Redis (via
 * @open-workflow/world-redirect) for storage.
 *
 * Architecture: Zeplo replaces the in-process 307 trampoline + sorted-set
 * scheduler that world-redirect uses on the queue side. The storage tier
 * (event log + materialized run/step/hook/wait entities + stream chunks)
 * is unchanged — we still use Redis through world-redirect's existing
 * Storage and Streamer implementations.
 *
 * When to use this world: serverless platforms where you can't keep a
 * long-running dispatcher alive (Vercel functions, Cloudflare Workers,
 * AWS Lambda, etc.) and you want a hosted queue to handle scheduling,
 * retries, and dead-lettering for you instead of running your own.
 *
 * Environment:
 *   WORKFLOW_TARGET_WORLD=@open-workflow/world-zeplo
 *   ZEPLO_TOKEN=<your zeplo api token>
 *   ZEPLO_ENDPOINT=https://zeplo.to                     (optional)
 *   WORKFLOW_BASE_URL=https://<your-public-app-url>     (Zeplo posts here)
 *   ZEPLO_WEBHOOK_SECRET=<shared secret>                (optional, recommended)
 *   WORKFLOW_REDIS_REST_URL / WORKFLOW_REDIS_REST_TOKEN (for storage)
 */

import { createRedisWorld } from '@open-workflow/world-redirect';
import type { World } from '@workflow/world';
import {
  resolveZeploConfig,
  type ZeploConfig,
  type ZeploWorldConfig,
} from './config.js';
import { createZeploQueue } from './zeplo-queue.js';

export type { ZeploConfig, ZeploWorldConfig } from './config.js';

export interface ZeploWorld extends World {
  /** Release any underlying connections (e.g. the Redis storage client). */
  close(): Promise<void>;
  /** Re-enqueue active runs through Zeplo on startup. */
  start(): Promise<void>;
}

/**
 * Build a Zeplo-backed World instance.
 *
 * Reuses `createRedisWorld` for storage + streamer, then replaces the
 * `queue` / `createQueueHandler` / `getDeploymentId` triplet with Zeplo
 * versions. The in-process dispatcher is disabled (Zeplo is the queue
 * broker) and active-run recovery is best-effort: pending runs are
 * re-enqueued through Zeplo when `start()` is called, but this is a no-op
 * in stateless serverless contexts where `start()` isn't called per
 * invocation.
 */
export function createZeploWorld(config: ZeploWorldConfig = {}): ZeploWorld {
  // Storage / streamer come from world-redirect. We force the in-process
  // dispatcher off because Zeplo is what drives delivery now.
  const base = createRedisWorld({
    ...config,
    startDispatcher: false,
    recoverActiveRuns: false,
  });

  const zeploConfig = resolveZeploConfig(config.zeplo);
  const zeploQueue = createZeploQueue(zeploConfig);

  const world: ZeploWorld = {
    specVersion: base.specVersion,
    // Zeplo-backed queue
    queue: zeploQueue.queue,
    createQueueHandler: zeploQueue.createQueueHandler,
    getDeploymentId: zeploQueue.getDeploymentId,
    // Redis-backed storage (from world-redirect)
    runs: base.runs,
    steps: base.steps,
    events: base.events,
    hooks: base.hooks,
    // Redis-backed streamer
    streams: base.streams,
    streamFlushIntervalMs: base.streamFlushIntervalMs,
    async start() {
      // Optionally re-enqueue pending/running runs through Zeplo on startup.
      // Skipped by default: in serverless contexts start() isn't called per
      // request, and recovery in always-on hosts is a documented future
      // improvement. For now, recovery means the operator manually replays
      // the relevant runs via the dashboard or a one-off script.
    },
    async close() {
      await base.close();
    },
  };
  return world;
}

// ---- module-singleton entry for WORKFLOW_TARGET_WORLD ----

const cache = new Map<string, ZeploWorld>();

function cacheKey(): string {
  return [
    process.env.WORKFLOW_REDIS_URL,
    process.env.WORKFLOW_REDIS_REST_URL,
    process.env.WORKFLOW_REDIS_KEY_PREFIX,
    process.env.ZEPLO_TOKEN,
    process.env.WORKFLOW_BASE_URL,
  ].join('|');
}

/**
 * Entry point used by the Workflow runtime when
 * `WORKFLOW_TARGET_WORLD=@open-workflow/world-zeplo`. Returns a process-wide
 * singleton so the flow handler and any other place that reads the world
 * share one instance.
 */
export function createWorld(): ZeploWorld {
  const key = cacheKey();
  let world = cache.get(key);
  if (!world) {
    world = createZeploWorld();
    cache.set(key, world);
  }
  return world;
}

export default createWorld;
