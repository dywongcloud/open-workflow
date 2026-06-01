/**
 * A minimal, uniform Redis interface used by world-redis.
 *
 * Two adapters implement it:
 *  - node-redis (standard RESP TCP Redis: self-hosted, ElastiCache, etc.)
 *  - Upstash REST (serverless HTTP Redis)
 *
 * All values are strings. Binary payloads (CBOR-encoded entities / events)
 * are base64-encoded by callers before they reach this layer, so both the
 * RESP and REST transports behave identically. This keeps the storage,
 * queue and streamer modules transport-agnostic.
 */

export interface ZRangeByScoreOptions {
  /** LIMIT offset count */
  offset?: number;
  count?: number;
}

export interface SetOptions {
  /** Only set when the key does not already exist (SET ... NX). */
  nx?: boolean;
  /** Expiry in milliseconds (SET ... PX). */
  pxMs?: number;
}

/**
 * Optional pub/sub capability. Only the node-redis adapter implements this.
 * The streamer feature-detects it and falls back to polling when absent
 * (e.g. on Upstash REST, which has no persistent SUBSCRIBE).
 */
export interface PubSub {
  publish(channel: string, message: string): Promise<void>;
  /**
   * Subscribe to a channel. Returns an unsubscribe function.
   * The handler is invoked for every message published to the channel.
   */
  subscribe(
    channel: string,
    handler: (message: string) => void
  ): Promise<() => Promise<void>>;
}

export interface RedisClient {
  /** Human-readable transport label, used in logs. */
  readonly label: string;

  // --- strings ---
  get(key: string): Promise<string | null>;
  /** Returns true when the value was set (relevant for `nx`). */
  set(key: string, value: string, opts?: SetOptions): Promise<boolean>;
  del(...keys: string[]): Promise<number>;
  incr(key: string): Promise<number>;
  pexpire(key: string, ms: number): Promise<void>;

  // --- sorted sets ---
  zadd(key: string, score: number, member: string): Promise<void>;
  zrem(key: string, ...members: string[]): Promise<number>;
  zcard(key: string): Promise<number>;
  zscore(key: string, member: string): Promise<number | null>;
  /** Inclusive range by score (min..max), optionally limited. */
  zrangeByScore(
    key: string,
    min: number,
    max: number,
    opts?: ZRangeByScoreOptions
  ): Promise<string[]>;
  /** Range by rank. `rev` reverses order (highest rank first). */
  zrangeByRank(
    key: string,
    start: number,
    stop: number,
    rev?: boolean
  ): Promise<string[]>;

  // --- hashes ---
  hset(key: string, field: string, value: string): Promise<void>;
  hsetMany(key: string, entries: Record<string, string>): Promise<void>;
  hget(key: string, field: string): Promise<string | null>;
  hgetAll(key: string): Promise<Record<string, string>>;
  hdel(key: string, ...fields: string[]): Promise<number>;
  hlen(key: string): Promise<number>;

  // --- lists ---
  rpush(key: string, ...values: string[]): Promise<number>;
  lrange(key: string, start: number, stop: number): Promise<string[]>;
  llen(key: string): Promise<number>;

  /** Run a Lua script server-side. Used for atomic compare-and-delete. */
  eval(script: string, keys: string[], args: string[]): Promise<unknown>;

  /** Optional pub/sub (node-redis only). */
  readonly pubsub?: PubSub;

  /** Release connections. */
  close(): Promise<void>;
}
