import { createClient } from 'redis';
import type {
  PubSub,
  RedisClient,
  SetOptions,
  ZRangeByScoreOptions,
} from './types.js';

type RawClient = ReturnType<typeof createClient>;

/**
 * Adapter over `redis` (node-redis v4) for standard RESP/TCP Redis servers.
 * Connection is established lazily on first command.
 */
export class NodeRedisClient implements RedisClient {
  readonly label = 'node-redis';
  private client: RawClient;
  private connectPromise: Promise<void> | null = null;
  private subscriber: RawClient | null = null;

  constructor(url: string) {
    this.client = createClient({ url });
    // Prevent unhandled 'error' events from crashing the process; surface them.
    this.client.on('error', (err: unknown) => {
      console.error('[world-redis] node-redis client error:', err);
    });
  }

  private async ready(): Promise<RawClient> {
    if (!this.connectPromise) {
      this.connectPromise = this.client.connect().then(() => undefined);
    }
    await this.connectPromise;
    return this.client;
  }

  async get(key: string): Promise<string | null> {
    const c = await this.ready();
    return (await c.get(key)) as string | null;
  }

  async set(key: string, value: string, opts?: SetOptions): Promise<boolean> {
    const c = await this.ready();
    const options: Record<string, unknown> = {};
    if (opts?.nx) options.NX = true;
    if (opts?.pxMs != null) options.PX = opts.pxMs;
    const res = await c.set(key, value, options);
    // With NX, node-redis returns null when the key already existed.
    return res === 'OK' || res === null ? res === 'OK' : true;
  }

  async del(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    const c = await this.ready();
    return await c.del(keys);
  }

  async incr(key: string): Promise<number> {
    const c = await this.ready();
    return await c.incr(key);
  }

  async pexpire(key: string, ms: number): Promise<void> {
    const c = await this.ready();
    await c.pExpire(key, ms);
  }

  async zadd(key: string, score: number, member: string): Promise<void> {
    const c = await this.ready();
    await c.zAdd(key, { score, value: member });
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    if (members.length === 0) return 0;
    const c = await this.ready();
    return await c.zRem(key, members);
  }

  async zcard(key: string): Promise<number> {
    const c = await this.ready();
    return await c.zCard(key);
  }

  async zscore(key: string, member: string): Promise<number | null> {
    const c = await this.ready();
    const res = await c.zScore(key, member);
    return res === null ? null : Number(res);
  }

  async zrangeByScore(
    key: string,
    min: number,
    max: number,
    opts?: ZRangeByScoreOptions
  ): Promise<string[]> {
    const c = await this.ready();
    const options =
      opts?.count != null
        ? { LIMIT: { offset: opts.offset ?? 0, count: opts.count } }
        : undefined;
    return (await c.zRangeByScore(key, min, max, options)) as string[];
  }

  async zrangeByRank(
    key: string,
    start: number,
    stop: number,
    rev?: boolean
  ): Promise<string[]> {
    const c = await this.ready();
    return (await c.zRange(key, start, stop, rev ? { REV: true } : undefined)) as string[];
  }

  async hset(key: string, field: string, value: string): Promise<void> {
    const c = await this.ready();
    await c.hSet(key, field, value);
  }

  async hsetMany(key: string, entries: Record<string, string>): Promise<void> {
    if (Object.keys(entries).length === 0) return;
    const c = await this.ready();
    await c.hSet(key, entries);
  }

  async hget(key: string, field: string): Promise<string | null> {
    const c = await this.ready();
    return (await c.hGet(key, field)) as string | null;
  }

  async hgetAll(key: string): Promise<Record<string, string>> {
    const c = await this.ready();
    return (await c.hGetAll(key)) as Record<string, string>;
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    if (fields.length === 0) return 0;
    const c = await this.ready();
    return await c.hDel(key, fields);
  }

  async hlen(key: string): Promise<number> {
    const c = await this.ready();
    return await c.hLen(key);
  }

  async rpush(key: string, ...values: string[]): Promise<number> {
    if (values.length === 0) return await this.llen(key);
    const c = await this.ready();
    return await c.rPush(key, values);
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    const c = await this.ready();
    return (await c.lRange(key, start, stop)) as string[];
  }

  async llen(key: string): Promise<number> {
    const c = await this.ready();
    return await c.lLen(key);
  }

  async eval(
    script: string,
    keys: string[],
    args: string[]
  ): Promise<unknown> {
    const c = await this.ready();
    return await c.eval(script, { keys, arguments: args });
  }

  get pubsub(): PubSub {
    return {
      publish: async (channel, message) => {
        const c = await this.ready();
        await c.publish(channel, message);
      },
      subscribe: async (channel, handler) => {
        await this.ready();
        if (!this.subscriber) {
          this.subscriber = this.client.duplicate();
          this.subscriber.on('error', (err: unknown) => {
            console.error('[world-redis] node-redis subscriber error:', err);
          });
          await this.subscriber.connect();
        }
        const sub = this.subscriber;
        await sub.subscribe(channel, (message: string) => handler(message));
        return async () => {
          await sub.unsubscribe(channel);
        };
      },
    };
  }

  async close(): Promise<void> {
    try {
      if (this.subscriber) {
        await this.subscriber.quit().catch(() => undefined);
      }
      if (this.connectPromise) {
        await this.client.quit().catch(() => undefined);
      }
    } catch {
      // best effort
    }
  }
}
