import { afterAll, describe, expect, it } from 'vitest';
import { MemoryRedisClient } from './client/memory.js';
import { NodeRedisClient } from './client/node-redis.js';
import type { RedisClient } from './client/types.js';
import { Keys } from './keys.js';
import { createStreamer } from './streamer.js';

const dec = new TextDecoder();

async function drain(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  let out = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) out += dec.decode(value);
  }
  return out;
}

const backends: { name: string; make: () => RedisClient }[] = [
  { name: 'memory', make: () => new MemoryRedisClient() },
  ...(process.env.TEST_REDIS_URL
    ? [
        {
          name: 'node-redis',
          make: () => new NodeRedisClient(process.env.TEST_REDIS_URL as string),
        },
      ]
    : []),
];

describe.each(backends)('streamer [$name]', ({ make }) => {
  const client = make();
  let n = 0;
  const keys = () => new Keys(`strm${Date.now()}_${++n}`);
  afterAll(async () => {
    await client.close();
  });

  it('writes, reads chunks, reports info, and lists', async () => {
    const s = createStreamer(client, keys());
    await s.streams.write('r1', 'out', 'hello ');
    await s.streams.writeMulti!('r1', 'out', ['world', '!']);
    const info = await s.streams.getInfo('r1', 'out');
    expect(info.tailIndex).toBe(2);
    expect(info.done).toBe(false);
    const chunks = await s.streams.getChunks('r1', 'out', { limit: 2 });
    expect(chunks.data).toHaveLength(2);
    expect(chunks.hasMore).toBe(true);
    expect(dec.decode(chunks.data[0]!.data)).toBe('hello ');
    const names = await s.streams.list('r1');
    expect(names).toContain('out');
  });

  it('live get() receives chunks written after subscription, until close', async () => {
    const s = createStreamer(client, keys(), 10);
    const runId = 'r2';
    const collected = drain(await s.streams.get(runId, 'live'));
    // Write after the reader has started.
    await new Promise((r) => setTimeout(r, 20));
    await s.streams.write(runId, 'live', 'a');
    await s.streams.write(runId, 'live', 'b');
    await new Promise((r) => setTimeout(r, 20));
    await s.streams.write(runId, 'live', 'c');
    await s.streams.close(runId, 'live');
    expect(await collected).toBe('abc');
  });

  it('get() with negative startIndex reads from the tail', async () => {
    const s = createStreamer(client, keys());
    const runId = 'r3';
    await s.streams.writeMulti!(runId, 'h', ['0', '1', '2', '3', '4']);
    await s.streams.close(runId, 'h');
    const tail = await drain(await s.streams.get(runId, 'h', -2));
    expect(tail).toBe('34');
  });
});
