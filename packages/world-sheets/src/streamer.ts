import type {
  GetChunksOptions,
  StreamChunksResponse,
  Streamer,
  StreamInfoResponse,
} from '@workflow/world';
import { rowToRecord, TAB_COLUMNS } from './schema.js';
import type { SheetsClient } from './sheets.js';

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

interface ChunkRow {
  runId: string;
  name: string;
  chunkIdx: number;
  done: boolean;
  dataB64: string;
}

/**
 * Polling-based streamer. Live get() polls the streams sheet every
 * flushIntervalMs (default 2000 — Sheets rate limit). For low-volume
 * orchestration where you want stream output visible in the spreadsheet,
 * this is fine; for high-throughput log streaming, choose another world.
 */
export function createSheetsStreamer(
  sheets: SheetsClient,
  flushIntervalMs?: number
): Streamer {
  const pollMs = Math.max(500, flushIntervalMs ?? 2000);

  async function loadChunksForStream(
    runId: string,
    name: string
  ): Promise<ChunkRow[]> {
    const rows = await sheets.getAllRows('streams');
    const out: ChunkRow[] = [];
    for (const row of rows) {
      const r = rowToRecord('streams', row);
      if (r.runId === runId && r.name === name) {
        out.push({
          runId: r.runId,
          name: r.name,
          chunkIdx: Number(r.chunkIdx ?? '0'),
          done: r.done === 'true',
          dataB64: r.dataB64 ?? '',
        });
      }
    }
    out.sort((a, b) => a.chunkIdx - b.chunkIdx);
    return out;
  }

  async function nextChunkIdx(runId: string, name: string): Promise<number> {
    const chunks = await loadChunksForStream(runId, name);
    return chunks.length === 0
      ? 0
      : (chunks[chunks.length - 1]!.chunkIdx ?? -1) + 1;
  }

  async function appendChunkRows(
    runId: string,
    name: string,
    payloads: Array<{ data?: string; done: boolean }>,
    startIdx: number
  ): Promise<void> {
    const now = new Date().toISOString();
    const rows = payloads.map((p, i) => {
      const rec: Record<string, string> = {
        runId,
        name,
        chunkIdx: String(startIdx + i),
        done: String(p.done),
        createdAtIso: now,
        dataB64: p.data ?? '',
      };
      return TAB_COLUMNS.streams.map((c) => rec[c] ?? '');
    });
    await sheets.appendRows('streams', rows);
  }

  const streams: Streamer['streams'] = {
    async write(runId, name, chunk) {
      const idx = await nextChunkIdx(runId, name);
      await appendChunkRows(
        runId,
        name,
        [{ data: Buffer.from(toBytes(chunk)).toString('base64'), done: false }],
        idx
      );
    },

    async writeMulti(runId, name, chunks) {
      if (chunks.length === 0) return;
      const idx = await nextChunkIdx(runId, name);
      await appendChunkRows(
        runId,
        name,
        chunks.map((c) => ({
          data: Buffer.from(toBytes(c)).toString('base64'),
          done: false,
        })),
        idx
      );
    },

    async close(runId, name) {
      const idx = await nextChunkIdx(runId, name);
      await appendChunkRows(runId, name, [{ done: true }], idx);
    },

    async get(runId, name, startIndex = 0) {
      // Resolve negative startIndex against the current tail.
      let cursor = startIndex;
      if (startIndex < 0) {
        const chunks = await loadChunksForStream(runId, name);
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
              const chunks = await loadChunksForStream(runId, name);
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
                  controller.enqueue(
                    new Uint8Array(Buffer.from(c.dataB64, 'base64'))
                  );
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
      const rows = await sheets.getAllRows('streams');
      const names = new Set<string>();
      for (const row of rows) {
        const r = rowToRecord('streams', row);
        if (r.runId === runId && r.name) names.add(r.name);
      }
      return Array.from(names);
    },

    async getChunks(
      runId,
      name,
      options?: GetChunksOptions
    ): Promise<StreamChunksResponse> {
      const chunks = await loadChunksForStream(runId, name);
      const data = chunks.filter((c) => !c.done);
      const limit = Math.min(Math.max(1, options?.limit ?? 100), 1000);
      const start = decodeChunkCursor(options?.cursor);
      const slice = data.slice(start, start + limit).map((c, k) => ({
        index: start + k,
        data: new Uint8Array(Buffer.from(c.dataB64, 'base64')),
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
      const chunks = await loadChunksForStream(runId, name);
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
