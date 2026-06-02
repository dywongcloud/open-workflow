/**
 * @open-workflow/world-sheets — Workflow World backed by a Google Sheet.
 *
 * Storage: one tab per entity type (runs/events/steps/hooks/waits/streams)
 * plus a `schedule` tab that acts as both the queue and the scheduler.
 * Every record is a row a human can read, filter, sort, and comment on in
 * the Sheets UI. Binary payloads are stored as base64(CBOR) in single cells.
 *
 * Queue: rows in the `schedule` tab with a `runAtMs` timestamp. An
 * in-process dispatcher polls every dispatcherPollMs, claims due rows by
 * stamping `claimedAt`, and POSTs to `{baseUrl}/.well-known/workflow/v1/...`.
 *
 * Streamer: chunks as rows in the `streams` tab. Live `get()` polls.
 *
 * When to use this:
 *   - admin / ops workflows where the team already lives in spreadsheets
 *   - low-volume orchestration (Sheets API allows ~60 reads + 60 writes
 *     per minute per user; that's plenty for a few hundred runs a day,
 *     not for a hot path)
 *   - demo / educational contexts ("look — this whole durable runtime is
 *     just a spreadsheet")
 *
 * When NOT to use this:
 *   - High-throughput workloads (use world-redirect)
 *   - Multi-process hosts that need strong concurrency guarantees
 *     (Sheets has no transactions; the in-process mutex doesn't extend
 *     across host instances)
 *   - Sub-second dispatch latency
 */

import {
  SPEC_VERSION_SUPPORTS_EVENT_SOURCING,
  type World,
} from '@workflow/world';
import {
  resolveSheetsConfig,
  type ResolvedSheetsConfig,
  type SheetsWorldConfig,
} from './config.js';
import { TABS } from './schema.js';
import { SheetsClient } from './sheets.js';
import { createSheetsStorage } from './storage.js';
import { createSheetsStreamer } from './streamer.js';
import { createSheetsQueue, type SheetsQueue } from './queue.js';

export type { SheetsWorldConfig } from './config.js';

export interface SheetsWorld extends World {
  start(): Promise<void>;
  close(): Promise<void>;
  /** Manually drain the schedule once (useful for tests / external triggers). */
  drainOnce(): Promise<number>;
  /** Direct handle to the Sheets client (escape hatch). */
  readonly sheets: SheetsClient;
}

function ownerConfig() {
  return {
    ownerId: process.env.WORKFLOW_OWNER_ID ?? 'owf-owner',
    projectId: process.env.WORKFLOW_PROJECT_ID ?? 'owf-project',
    environment:
      process.env.WORKFLOW_ENVIRONMENT ?? 'development',
  };
}

async function bootstrap(
  sheets: SheetsClient,
  config: ResolvedSheetsConfig
): Promise<void> {
  if (!config.autoBootstrap) return;
  // ensureSheet is idempotent — creates the tab + writes headers if missing.
  for (const tab of Object.values(TABS)) {
    await sheets.ensureSheet(tab);
  }
}

export function createSheetsWorld(config: SheetsWorldConfig = {}): SheetsWorld {
  const resolved = resolveSheetsConfig(config);
  const sheets = new SheetsClient(
    resolved.spreadsheetId,
    resolved.accessTokenProvider,
    resolved.tabPrefix
  );
  const storage = createSheetsStorage(sheets, ownerConfig());
  const streamer = createSheetsStreamer(sheets);
  const queue: SheetsQueue = createSheetsQueue(sheets, resolved);

  const world: SheetsWorld = {
    specVersion: SPEC_VERSION_SUPPORTS_EVENT_SOURCING,
    queue: queue.queue,
    createQueueHandler: queue.createQueueHandler,
    getDeploymentId: queue.getDeploymentId,
    runs: storage.runs,
    steps: storage.steps,
    events: storage.events,
    hooks: storage.hooks,
    streams: streamer.streams,
    streamFlushIntervalMs: streamer.streamFlushIntervalMs,
    async start() {
      await bootstrap(sheets, resolved);
      if (resolved.startDispatcher) queue.startDispatcher();
      if (resolved.recoverActiveRuns) {
        // Re-enqueue pending/running runs through the schedule sheet.
        for (const status of ['pending', 'running'] as const) {
          const runs = await storage.raw.runsForStatus(status);
          for (const r of runs) {
            await queue.queue(
              `__wkf_workflow_${r.workflowName}`,
              { runId: r.runId } as any
            );
          }
        }
      }
    },
    async close() {
      await queue.stopDispatcher();
    },
    drainOnce: queue.drainOnce,
    sheets,
  };
  return world;
}

// ---- singleton entry for WORKFLOW_TARGET_WORLD ----

const cache = new Map<string, SheetsWorld>();
function cacheKey() {
  return [
    process.env.GOOGLE_SHEETS_SPREADSHEET_ID,
    process.env.WORKFLOW_SHEETS_TAB_PREFIX,
    process.env.WORKFLOW_BASE_URL,
  ].join('|');
}

export function createWorld(): SheetsWorld {
  const key = cacheKey();
  let world = cache.get(key);
  if (!world) {
    world = createSheetsWorld();
    cache.set(key, world);
  }
  return world;
}

export default createWorld;
