import type { ResolvedRedisWorldConfig } from '../config.js';
import { MemoryRedisClient } from './memory.js';
import { NodeRedisClient } from './node-redis.js';
import type { RedisClient } from './types.js';
import { UpstashRedisClient } from './upstash.js';

export type { RedisClient, PubSub, SetOptions } from './types.js';
export { MemoryRedisClient } from './memory.js';
export { NodeRedisClient } from './node-redis.js';
export { UpstashRedisClient } from './upstash.js';

/**
 * Construct a RedisClient from resolved config.
 *
 * Selection order:
 *  1. explicit `client`
 *  2. `memory` (in-process, non-durable — dev/test only)
 *  3. Upstash REST (url + token)
 *  4. RESP url
 */
export function createRedisClient(
  config: ResolvedRedisWorldConfig
): RedisClient {
  if (config.client) return config.client;
  if (config.redisUrl === 'memory') {
    return new MemoryRedisClient();
  }
  if (config.upstashUrl && config.upstashToken) {
    return new UpstashRedisClient(config.upstashUrl, config.upstashToken);
  }
  if (config.redisUrl) {
    return new NodeRedisClient(config.redisUrl);
  }
  throw new Error(
    '[world-redirect] No Redis connection configured. Set one of:\n' +
      '  - WORKFLOW_REDIS_URL (e.g. redis://localhost:6379), or\n' +
      '  - WORKFLOW_REDIS_REST_URL + WORKFLOW_REDIS_REST_TOKEN (Upstash REST),\n' +
      'or pass { redisUrl } / { upstashUrl, upstashToken } / { client } to createWorld().'
  );
}
