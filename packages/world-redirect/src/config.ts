import type { RedisClient } from './client/types.js';

export interface RedisWorldConfig {
  /** Standard RESP Redis connection URL (e.g. redis://localhost:6379). */
  redisUrl?: string;
  /** Upstash REST endpoint URL. */
  upstashUrl?: string;
  /** Upstash REST token. */
  upstashToken?: string;
  /** Provide a fully-constructed client, bypassing URL-based selection. */
  client?: RedisClient;

  /** Namespace for all Redis keys. Default "owf". */
  keyPrefix?: string;
  /**
   * Base URL where the workflow HTTP endpoints
   * (`/.well-known/workflow/v1/{flow,step,webhook}`) are served.
   * The dispatcher POSTs here. Default: WORKFLOW_BASE_URL or
   * http://localhost:{PORT||3000}.
   */
  baseUrl?: string;
  /** Deployment identifier reported by getDeploymentId(). */
  deploymentId?: string;

  /**
   * Whether `start()` launches the in-process 307 dispatch pump that drains
   * the Redis schedule. Set false in environments where an external trigger
   * (cron / queue / manual tick) drives dispatch. Default true.
   */
  startDispatcher?: boolean;
  /** Dispatcher poll interval in ms when idle. Default 50. */
  dispatcherPollMs?: number;
  /** Max HTTP delivery attempts before a job is dead-lettered. Default 25. */
  maxAttempts?: number;
  /** Base backoff (ms) for redelivery after a failed dispatch. Default 5000. */
  retryBaseMs?: number;
  /** Re-enqueue pending/running runs on start(). Default true. */
  recoverActiveRuns?: boolean;
  /** Streamer flush interval hint (ms). */
  streamFlushIntervalMs?: number;
}

export interface ResolvedRedisWorldConfig
  extends Required<
    Omit<
      RedisWorldConfig,
      | 'client'
      | 'redisUrl'
      | 'upstashUrl'
      | 'upstashToken'
      | 'streamFlushIntervalMs'
    >
  > {
  redisUrl?: string;
  upstashUrl?: string;
  upstashToken?: string;
  client?: RedisClient;
  streamFlushIntervalMs?: number;
}

function envBaseUrl(): string {
  if (process.env.WORKFLOW_BASE_URL) return process.env.WORKFLOW_BASE_URL;
  const port = process.env.PORT ?? '3000';
  return `http://localhost:${port}`;
}

export function resolveConfig(
  partial: RedisWorldConfig = {}
): ResolvedRedisWorldConfig {
  const env = process.env;
  return {
    redisUrl: partial.redisUrl ?? env.WORKFLOW_REDIS_URL ?? env.REDIS_URL,
    upstashUrl:
      partial.upstashUrl ??
      env.WORKFLOW_REDIS_REST_URL ??
      env.UPSTASH_REDIS_REST_URL,
    upstashToken:
      partial.upstashToken ??
      env.WORKFLOW_REDIS_REST_TOKEN ??
      env.UPSTASH_REDIS_REST_TOKEN,
    client: partial.client,
    keyPrefix:
      partial.keyPrefix ?? env.WORKFLOW_REDIS_KEY_PREFIX ?? 'owf',
    baseUrl: (partial.baseUrl ?? envBaseUrl()).replace(/\/$/, ''),
    deploymentId:
      partial.deploymentId ?? env.WORKFLOW_DEPLOYMENT_ID ?? 'dpl_redis_local',
    startDispatcher:
      partial.startDispatcher ??
      (env.WORKFLOW_REDIS_DISABLE_DISPATCHER ? false : true),
    dispatcherPollMs:
      partial.dispatcherPollMs ??
      numEnv(env.WORKFLOW_REDIS_DISPATCHER_POLL_MS, 50),
    maxAttempts:
      partial.maxAttempts ?? numEnv(env.WORKFLOW_REDIS_MAX_ATTEMPTS, 25),
    retryBaseMs:
      partial.retryBaseMs ?? numEnv(env.WORKFLOW_REDIS_RETRY_BASE_MS, 5000),
    recoverActiveRuns: partial.recoverActiveRuns ?? true,
    streamFlushIntervalMs: partial.streamFlushIntervalMs,
  };
}

function numEnv(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}
