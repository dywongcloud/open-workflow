import type { KV } from './types.js';

export interface KVWorldConfig {
  /** Pre-resolved KV namespace. If omitted, looked up via `EDGEONE_KV_BINDING` (default `EDGEONE_KV`). */
  kv?: KV;
  /** Binding name to look up on globalThis / process.env. Defaults to `EDGEONE_KV`. */
  bindingName?: string;
  /** Allow in-process Map fallback (useful for tests / dev). Default false. */
  allowInMemoryFallback?: boolean;
  /** Key namespace within the KV. Default `owf`. */
  keyPrefix?: string;
  /** Reported by getDeploymentId. Default `dpl_edgeone_kv_local`. */
  deploymentId?: string;
  /** Where the workflow runtime is hosted publicly. */
  baseUrl?: string;
  /** Start the in-process dispatcher pump on `start()`. Default true. */
  startDispatcher?: boolean;
  /** Dispatcher poll interval (ms). Default 1500. */
  dispatcherPollMs?: number;
  /** Max in-flight dispatches per tick. Default 8. */
  dispatchBatch?: number;
  /** Lease TTL when claiming a job. EdgeOne KV minimum is 60s. Default 60. */
  leaseSeconds?: number;
  /** Max delivery attempts before a job is dropped. Default 10. */
  maxAttempts?: number;
  /** Base backoff (ms) for redelivery after a failed dispatch. Default 5000. */
  retryBaseMs?: number;
  /** Re-enqueue pending/running runs on start. Default true. */
  recoverActiveRuns?: boolean;
  /** Stream flush interval reported to the runtime (also the poll interval). Default unset. */
  streamFlushIntervalMs?: number;
}

export interface ResolvedKVConfig {
  kv: KV | undefined;
  bindingName: string;
  allowInMemoryFallback: boolean;
  keyPrefix: string;
  deploymentId: string;
  baseUrl: string;
  startDispatcher: boolean;
  dispatcherPollMs: number;
  dispatchBatch: number;
  leaseSeconds: number;
  maxAttempts: number;
  retryBaseMs: number;
  recoverActiveRuns: boolean;
  streamFlushIntervalMs?: number | undefined;
}

function envBaseUrl(): string {
  if (process.env.WORKFLOW_BASE_URL) return process.env.WORKFLOW_BASE_URL;
  if (process.env.APP_BASE_URL) return process.env.APP_BASE_URL;
  return `http://localhost:${process.env.PORT ?? 3000}`;
}

function numEnv(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

export function resolveKVConfig(partial: KVWorldConfig = {}): ResolvedKVConfig {
  const env = process.env;
  const streamFlush = partial.streamFlushIntervalMs ??
    (env.WORKFLOW_EDGEONE_KV_STREAM_FLUSH_MS
      ? numEnv(env.WORKFLOW_EDGEONE_KV_STREAM_FLUSH_MS, 1000)
      : undefined);
  return {
    kv: partial.kv,
    bindingName:
      partial.bindingName ??
      env.WORKFLOW_EDGEONE_KV_BINDING ??
      env.EDGEONE_KV_BINDING ??
      'EDGEONE_KV',
    allowInMemoryFallback:
      partial.allowInMemoryFallback ??
      env.WORKFLOW_EDGEONE_KV_MEMORY === '1',
    keyPrefix:
      partial.keyPrefix ?? env.WORKFLOW_EDGEONE_KV_PREFIX ?? 'owf',
    deploymentId:
      partial.deploymentId ??
      env.WORKFLOW_DEPLOYMENT_ID ??
      'dpl_edgeone_kv_local',
    baseUrl: (partial.baseUrl ?? envBaseUrl()).replace(/\/$/, ''),
    startDispatcher:
      partial.startDispatcher ??
      (env.WORKFLOW_EDGEONE_KV_DISABLE_DISPATCHER ? false : true),
    dispatcherPollMs:
      partial.dispatcherPollMs ??
      numEnv(env.WORKFLOW_EDGEONE_KV_DISPATCHER_POLL_MS, 1500),
    dispatchBatch:
      partial.dispatchBatch ??
      numEnv(env.WORKFLOW_EDGEONE_KV_DISPATCH_BATCH, 8),
    leaseSeconds:
      partial.leaseSeconds ??
      numEnv(env.WORKFLOW_EDGEONE_KV_LEASE_SECONDS, 60),
    maxAttempts:
      partial.maxAttempts ??
      numEnv(env.WORKFLOW_EDGEONE_KV_MAX_ATTEMPTS, 10),
    retryBaseMs:
      partial.retryBaseMs ??
      numEnv(env.WORKFLOW_EDGEONE_KV_RETRY_BASE_MS, 5000),
    recoverActiveRuns: partial.recoverActiveRuns ?? true,
    streamFlushIntervalMs: streamFlush,
  };
}
