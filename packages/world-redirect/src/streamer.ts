import type {
  GetChunksOptions,
  StreamChunksResponse,
  Streamer,
  StreamInfoResponse,
} from '@workflow/world';
import { base64ToBytes, bytesToBase64 } from './codec.js';
import type { RedisClient } from './client/types.js';
import type { Keys } from './keys.js';

function toBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
}

function encodeChunkCursor(index: number): string {
  return Buffer.from(JSON.stringify({ i: index })).toString('base64url');
}

function decodeChunkCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString());
    return typeof parsed?.i === 'number' && parsed.i >= 0 ? parsed.i : 0;
  } catch {
    return 0;
  }
}

export function createStreamer(
  redis: RedisClient,
  keys: Keys,
  flushIntervalMs?: number
): Streamer {
  const pollMs = Math.max(5, flushIntervalMs ?? 25);

  async function markStream(runId: string, name: string): Promise<void> {
    await redis.zadd(keys.streamList(runId), Date.now(), name);
  }

  const streams: Streamer['streams'] = {
    async write(runId, name, chunk) {
      await redis.rpush(
        keys.streamChunks(runId, name),
        bytesToBase64(toBytes(chunk))
      );
      await markStream(runId, name);
      if (redis.pubsub) {
        await redis.pubsub.publish(keys.streamChannel(runId, name), 'c');
      }
    },

    async writeMulti(runId, name, chunks) {
      if (chunks.length === 0) return;
      await redis.rpush(
        keys.streamChunks(runId, name),
        ...chunks.map((c) => bytesToBase64(toBytes(c)))
      );
      await markStream(runId, name);
      if (redis.pubsub) {
        await redis.pubsub.publish(keys.streamChannel(runId, name), 'c');
      }
    },

    async close(runId, name) {
      await redis.hset(keys.streamMeta(runId, name), 'done', '1');
      await markStream(runId, name);
      if (redis.pubsub) {
        await redis.pubsub.publish(keys.streamChannel(runId, name), 'done');
      }
    },

    async get(runId, name, startIndex = 0) {
      const chunksKey = keys.streamChunks(runId, name);
      const metaKey = keys.streamMeta(runId, name);

      // Resolve a negative startIndex relative to the current tail.
      let cursor = startIndex;
      if (startIndex < 0) {
        const total = await redis.llen(chunksKey);
        cursor = Math.max(0, total + startIndex);
      }

      let cleanup = () => {};
      return new ReadableStream<Uint8Array>({
        start: (controller) => {
          let closed = false;
          let pumping = false;
          let again = false;

          const finish = () => {
            if (closed) return;
            closed = true;
            try {
              controller.close();
            } catch {
              // already closed
            }
            cleanup();
          };

          const pump = async () => {
            if (closed) return;
            if (pumping) {
              again = true;
              return;
            }
            pumping = true;
            try {
              do {
                again = false;
                const items = await redis.lrange(chunksKey, cursor, -1);
                for (const b64 of items) {
                  if (closed) return;
                  controller.enqueue(base64ToBytes(b64));
                  cursor++;
                }
                const done = await redis.hget(metaKey, 'done');
                if (done === '1') {
                  // Drain anything appended between the lrange and the done read.
                  const tail = await redis.lrange(chunksKey, cursor, -1);
                  for (const b64 of tail) {
                    controller.enqueue(base64ToBytes(b64));
                    cursor++;
                  }
                  finish();
                  return;
                }
              } while (again && !closed);
            } catch (err) {
              if (!closed) {
                closed = true;
                controller.error(err);
                cleanup();
              }
            } finally {
              pumping = false;
            }
          };

          let unsub: (() => Promise<void>) | undefined;
          const timer = setInterval(() => void pump(), pollMs);
          cleanup = () => {
            clearInterval(timer);
            if (unsub) void unsub();
          };

          if (redis.pubsub) {
            void redis.pubsub
              .subscribe(keys.streamChannel(runId, name), () => void pump())
              .then((fn) => {
                unsub = fn;
              });
          }

          void pump();
        },
        cancel: () => {
          cleanup();
        },
      });
    },

    async list(runId) {
      return await redis.zrangeByRank(keys.streamList(runId), 0, -1);
    },

    async getChunks(runId, name, options?: GetChunksOptions): Promise<StreamChunksResponse> {
      const chunksKey = keys.streamChunks(runId, name);
      const total = await redis.llen(chunksKey);
      const limit = Math.min(Math.max(1, options?.limit ?? 100), 1000);
      const start = decodeChunkCursor(options?.cursor);
      const items = await redis.lrange(chunksKey, start, start + limit - 1);
      const data = items.map((b64, k) => ({
        index: start + k,
        data: base64ToBytes(b64),
      }));
      const consumed = start + items.length;
      const hasMore = consumed < total;
      const done =
        (await redis.hget(keys.streamMeta(runId, name), 'done')) === '1';
      return {
        data,
        cursor: hasMore ? encodeChunkCursor(consumed) : null,
        hasMore,
        done,
      };
    },

    async getInfo(runId, name): Promise<StreamInfoResponse> {
      const total = await redis.llen(keys.streamChunks(runId, name));
      const done =
        (await redis.hget(keys.streamMeta(runId, name), 'done')) === '1';
      return { tailIndex: total - 1, done };
    },
  };

  return {
    ...(flushIntervalMs !== undefined && { streamFlushIntervalMs: flushIntervalMs }),
    streams,
  };
}
