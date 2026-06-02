/**
 * KV-backed streamer. Chunks live at
 *
 *   {p}/chunk/<runId>/<name>/<paddedIdx>   → CBOR({chunkIdx, done, data?})
 *
 * The padded index keeps `list({prefix})` in chronological order. `get()` is
 * a polling ReadableStream — there is no pub/sub in KV, so live tail latency
 * is bounded by the poll interval (default 1000 ms).
 */

import type {
  GetChunksOptions,
  StreamChunksResponse,
  Streamer,
  StreamInfoResponse,
} from '@workflow/world';
import { bytesToBase64, base64ToBytes, decodeBlob, encodeBlob, padIdx } from './codec.js';
import type { Keys } from './keys.js';
import type { KV } from './types.js';

interface StoredChunk {
  chunkIdx: number;
  done: boolean;
  dataB64?: string;
}

function toBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
}

function encodeChunkCursor(i: number): string {
  return Buffer.from(String(i)).toString('base64url');
}
function decodeChunkCursor(c: string | undefined): number {
  if (!c) return 0;
  const n = Number.parseInt(Buffer.from(c, 'base64url').toString(), 10);
  return Number.isFinite(n) && n >= 0 ? n : 0;
}

async function listAll(kv: KV, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const opts: { prefix: string; cursor?: string; limit?: number } = {
      prefix,
      limit: 1000,
    };
    if (cursor) opts.cursor = cursor;
    const page = await kv.list(opts);
    out.push(...page.keys);
    cursor = page.complete ? undefined : page.cursor;
  } while (cursor);
  return out;
}

export function createKVStreamer(
  kv: KV,
  keys: Keys,
  flushIntervalMs?: number
): Streamer {
  const pollMs = Math.max(250, flushIntervalMs ?? 1000);

  async function loadChunks(runId: string, name: string): Promise<StoredChunk[]> {
    const prefix = keys.streamChunksPrefix(runId, name);
    const ks = await listAll(kv, prefix);
    // Keys are sorted by listAll's underlying list semantics; padded indexes
    // guarantee correct chronological order via lex sort.
    ks.sort();
    const out: StoredChunk[] = [];
    for (const k of ks) {
      const c = await kv.get(k);
      if (c) {
        const decoded = decodeBlob<StoredChunk>(c);
        if (decoded) out.push(decoded);
      }
    }
    return out;
  }

  async function nextChunkIdx(runId: string, name: string): Promise<number> {
    const chunks = await loadChunks(runId, name);
    if (chunks.length === 0) return 0;
    return (chunks[chunks.length - 1]!.chunkIdx ?? -1) + 1;
  }

  async function writeChunk(
    runId: string,
    name: string,
    chunk: StoredChunk
  ): Promise<void> {
    const key = keys.streamChunk(runId, name, padIdx(chunk.chunkIdx));
    await kv.put(key, encodeBlob(chunk));
  }

  const streams: Streamer['streams'] = {
    async write(runId, name, chunk) {
      const idx = await nextChunkIdx(runId, name);
      await writeChunk(runId, name, {
        chunkIdx: idx,
        done: false,
        dataB64: bytesToBase64(toBytes(chunk)),
      });
    },

    async writeMulti(runId, name, chunks) {
      if (chunks.length === 0) return;
      let idx = await nextChunkIdx(runId, name);
      for (const c of chunks) {
        await writeChunk(runId, name, {
          chunkIdx: idx++,
          done: false,
          dataB64: bytesToBase64(toBytes(c)),
        });
      }
    },

    async close(runId, name) {
      const idx = await nextChunkIdx(runId, name);
      await writeChunk(runId, name, { chunkIdx: idx, done: true });
    },

    async get(runId, name, startIndex = 0) {
      let cursor = startIndex;
      if (startIndex < 0) {
        const chunks = await loadChunks(runId, name);
        const total = chunks.filter((c) => !c.done).length;
        cursor = Math.max(0, total + startIndex);
      }
      let cleanup = () => {};
      return new ReadableStream<Uint8Array>({
        start: (controller) => {
          let closed = false;
          const pump = async () => {
            if (closed) return;
            try {
              const chunks = await loadChunks(runId, name);
              for (const c of chunks) {
                if (closed) return;
                if (c.chunkIdx < cursor) continue;
                if (c.done) {
                  closed = true;
                  try {
                    controller.close();
                  } catch {}
                  cleanup();
                  return;
                }
                if (c.dataB64) {
                  controller.enqueue(base64ToBytes(c.dataB64));
                }
                cursor = c.chunkIdx + 1;
              }
            } catch (err) {
              if (!closed) {
                closed = true;
                controller.error(err);
                cleanup();
              }
            }
          };
          const timer = setInterval(() => void pump(), pollMs);
          cleanup = () => clearInterval(timer);
          void pump();
        },
        cancel: () => {
          cleanup();
        },
      });
    },

    async list(runId) {
      const allKeys = await listAll(kv, `${keys.root}/chunk/${runId}/`);
      const names = new Set<string>();
      const prefixLen = `${keys.root}/chunk/${runId}/`.length;
      for (const k of allKeys) {
        const rest = k.slice(prefixLen);
        const slash = rest.indexOf('/');
        if (slash >= 0) names.add(rest.slice(0, slash));
      }
      return Array.from(names);
    },

    async getChunks(
      runId,
      name,
      options?: GetChunksOptions
    ): Promise<StreamChunksResponse> {
      const chunks = await loadChunks(runId, name);
      const data = chunks.filter((c) => !c.done);
      const limit = Math.min(Math.max(1, options?.limit ?? 100), 1000);
      const start = decodeChunkCursor(options?.cursor);
      const slice = data.slice(start, start + limit).map((c, k) => ({
        index: start + k,
        data: c.dataB64 ? base64ToBytes(c.dataB64) : new Uint8Array(),
      }));
      const consumed = start + slice.length;
      const hasMore = consumed < data.length;
      const done = chunks.some((c) => c.done);
      return {
        data: slice,
        cursor: hasMore ? encodeChunkCursor(consumed) : null,
        hasMore,
        done,
      };
    },

    async getInfo(runId, name): Promise<StreamInfoResponse> {
      const chunks = await loadChunks(runId, name);
      const data = chunks.filter((c) => !c.done);
      return {
        tailIndex: data.length - 1,
        done: chunks.some((c) => c.done),
      };
    },
  };

  return {
    ...(flushIntervalMs !== undefined && {
      streamFlushIntervalMs: flushIntervalMs,
    }),
    streams,
  };
}
