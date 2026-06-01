import { randomUUID } from 'node:crypto';
import type { RedisClient } from './client/types.js';
import type { Keys } from './keys.js';

const UNLOCK_SCRIPT = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end`;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Serializes the read-modify-write sequence in events.create on a per-run
 * basis, so concurrent events for the same run (e.g. parallel steps
 * completing) don't interleave their validate-then-write steps.
 *
 * Two layers:
 *  - an in-process async mutex (covers the common single-host deployment), and
 *  - a Redis lock with TTL + atomic compare-and-delete release (covers
 *    multiple host instances sharing one Redis).
 */
export class RunLocks {
  private inProcess = new Map<string, Promise<unknown>>();

  constructor(
    private readonly redis: RedisClient,
    private readonly keys: Keys,
    private readonly ttlMs = 15_000,
    private readonly acquireTimeoutMs = 20_000
  ) {}

  async withLock<T>(runId: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.inProcess.get(runId);
    const box: { task?: Promise<T> } = {};
    const task = (async () => {
      if (prev) await prev.catch(() => undefined);
      const token = await this.acquireRedis(runId);
      try {
        return await fn();
      } finally {
        await this.releaseRedis(runId, token);
        if (this.inProcess.get(runId) === box.task) {
          this.inProcess.delete(runId);
        }
      }
    })();
    box.task = task;
    this.inProcess.set(runId, task);
    return task;
  }

  private async acquireRedis(runId: string): Promise<string> {
    const key = this.keys.runLock(runId);
    const token = randomUUID();
    const deadline = Date.now() + this.acquireTimeoutMs;
    let delay = 5;
    while (Date.now() < deadline) {
      const ok = await this.redis.set(key, token, {
        nx: true,
        pxMs: this.ttlMs,
      });
      if (ok) return token;
      await sleep(delay);
      delay = Math.min(delay * 2, 100);
    }
    // Couldn't acquire within the timeout — proceed anyway (the in-process
    // mutex still guards same-process correctness). Returning '' means
    // release() is a no-op so we never clobber another holder's lock.
    console.warn(
      `[world-redis] run lock acquire timed out for ${runId}; proceeding without distributed lock`
    );
    return '';
  }

  private async releaseRedis(runId: string, token: string): Promise<void> {
    if (!token) return;
    try {
      await this.redis.eval(UNLOCK_SCRIPT, [this.keys.runLock(runId)], [token]);
    } catch {
      // best effort
    }
  }
}
