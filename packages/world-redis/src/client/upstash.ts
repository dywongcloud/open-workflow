import { Redis as UpstashRedis } from '@upstash/redis';
import type { RedisClient, SetOptions, ZRangeByScoreOptions } from './types.js';

/**
 * Adapter over `@upstash/redis` REST client for serverless/edge Redis.
 *
 * `automaticDeserialization: false` is REQUIRED: world-redis stores opaque
 * base64 strings, and Upstash's default JSON auto-deserialization would
 * corrupt values that happen to look like JSON/numbers. With it disabled,
 * values round-trip as raw strings exactly like the RESP adapter.
 *
 * Upstash REST has no persistent SUBSCRIBE, so this adapter intentionally
 * omits `pubsub`; the streamer falls back to polling.
 */
export class UpstashRedisClient implements RedisClient {
  readonly label = 'upstash-rest';
  private redis: UpstashRedis;

  constructor(url: string, token: string) {
    this.redis = new UpstashRedis({
      url,
      token,
      automaticDeserialization: false,
      // Retain default retry behaviour; REST calls are independent requests.
    });
  }

  async get(key: string): Promise<string | null> {
    return (await this.redis.get<string>(key)) ?? null;
  }

  async set(key: string, value: string, opts?: SetOptions): Promise<boolean> {
    const options: Record<string, unknown> = {};
    if (opts?.nx) options.nx = true;
    if (opts?.pxMs != null) options.px = opts.pxMs;
    const res = await this.redis.set(
      key,
      value,
      Object.keys(options).length ? options : undefined
    );
    return res === 'OK';
  }

  async del(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    return await this.redis.del(...keys);
  }

  async incr(key: string): Promise<number> {
    return await this.redis.incr(key);
  }

  async pexpire(key: string, ms: number): Promise<void> {
    await this.redis.pexpire(key, ms);
  }

  async zadd(key: string, score: number, member: string): Promise<void> {
    await this.redis.zadd(key, { score, member });
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    if (members.length === 0) return 0;
    return await this.redis.zrem(key, ...members);
  }

  async zcard(key: string): Promise<number> {
    return await this.redis.zcard(key);
  }

  async zscore(key: string, member: string): Promise<number | null> {
    const res = await this.redis.zscore(key, member);
    return res == null ? null : Number(res);
  }

  async zrangeByScore(
    key: string,
    min: number,
    max: number,
    opts?: ZRangeByScoreOptions
  ): Promise<string[]> {
    const options: Record<string, unknown> = { byScore: true };
    if (opts?.count != null) {
      options.offset = opts.offset ?? 0;
      options.count = opts.count;
    }
    return (await this.redis.zrange(key, min, max, options)) as string[];
  }

  async zrangeByRank(
    key: string,
    start: number,
    stop: number,
    rev?: boolean
  ): Promise<string[]> {
    return (await this.redis.zrange(
      key,
      start,
      stop,
      rev ? { rev: true } : undefined
    )) as string[];
  }

  async hset(key: string, field: string, value: string): Promise<void> {
    await this.redis.hset(key, { [field]: value });
  }

  async hsetMany(key: string, entries: Record<string, string>): Promise<void> {
    if (Object.keys(entries).length === 0) return;
    await this.redis.hset(key, entries);
  }

  async hget(key: string, field: string): Promise<string | null> {
    return (await this.redis.hget<string>(key, field)) ?? null;
  }

  async hgetAll(key: string): Promise<Record<string, string>> {
    const res = await this.redis.hgetall<Record<string, string>>(key);
    return res ?? {};
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    if (fields.length === 0) return 0;
    return await this.redis.hdel(key, ...fields);
  }

  async hlen(key: string): Promise<number> {
    return await this.redis.hlen(key);
  }

  async rpush(key: string, ...values: string[]): Promise<number> {
    if (values.length === 0) return await this.llen(key);
    return await this.redis.rpush(key, ...values);
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    return (await this.redis.lrange(key, start, stop)) as string[];
  }

  async llen(key: string): Promise<number> {
    return await this.redis.llen(key);
  }

  async eval(
    script: string,
    keys: string[],
    args: string[]
  ): Promise<unknown> {
    return await this.redis.eval(script, keys, args);
  }

  async close(): Promise<void> {
    // REST client holds no persistent connections.
  }
}
