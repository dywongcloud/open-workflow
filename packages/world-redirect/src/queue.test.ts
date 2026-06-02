import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { MemoryRedisClient } from './client/memory.js';
import { createRedisWorld, type RedisWorld } from './index.js';

type FlowHandler = (req: Request) => Promise<Response>;

interface Harness {
  world: RedisWorld;
  url: string;
  requests: string[];
  close: () => Promise<void>;
}

async function nodeToWeb(req: http.IncomingMessage, base: string): Promise<Request> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  const body = Buffer.concat(chunks);
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === 'string') headers[k] = v;
  }
  return new Request(base + req.url, {
    method: req.method,
    headers,
    body: body.length ? body : undefined,
  });
}

async function startHarness(
  buildHandlers: (world: RedisWorld) => {
    flow: (m: any, meta: any) => Promise<void | { timeoutSeconds: number }>;
    step?: (m: any, meta: any) => Promise<void | { timeoutSeconds: number }>;
  }
): Promise<Harness> {
  const requests: string[] = [];
  let flowHandler: FlowHandler;
  let stepHandler: FlowHandler;

  const server = http.createServer((req, res) => {
    void (async () => {
      requests.push(req.url ?? '');
      const isStep = (req.url ?? '').startsWith(
        '/.well-known/workflow/v1/step'
      );
      const handler = isStep ? stepHandler : flowHandler;
      const request = await nodeToWeb(req, url);
      const response = await handler(request);
      res.statusCode = response.status;
      response.headers.forEach((value, key) => res.setHeader(key, value));
      const buf = Buffer.from(await response.arrayBuffer());
      res.end(buf);
    })().catch((err) => {
      res.statusCode = 500;
      res.end(String(err));
    });
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;
  const url = `http://127.0.0.1:${port}`;

  const world = createRedisWorld({
    client: new MemoryRedisClient(),
    keyPrefix: `q${Date.now()}_${port}`,
    baseUrl: url,
    startDispatcher: true,
    dispatcherPollMs: 10,
    retryBaseMs: 20,
    recoverActiveRuns: false,
  });

  const handlers = buildHandlers(world);
  flowHandler = world.createQueueHandler('__wkf_workflow_', handlers.flow);
  stepHandler = world.createQueueHandler(
    '__wkf_step_',
    handlers.step ?? (async () => undefined)
  );

  return {
    world,
    url,
    requests,
    close: async () => {
      await world.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

describe('queue dispatch + 307 trampoline', () => {
  let harness: Harness | null = null;
  afterEach(async () => {
    if (harness) await harness.close();
    harness = null;
  });

  it('dispatches a job to the flow handler', async () => {
    const invocations: number[] = [];
    harness = await startHarness(() => ({
      flow: async (m) => {
        invocations.push(m.step ?? 0);
      },
    }));
    await harness.world.queue('__wkf_workflow_demo', { runId: 'r1', step: 0 } as any);
    harness.world.start();
    await harness.world.waitForIdle();
    expect(invocations).toEqual([0]);
  });

  it('continues immediate self-enqueues via 307 redirects in one chain', async () => {
    const invocations: number[] = [];
    harness = await startHarness((world) => ({
      flow: async (m) => {
        const step = m.step ?? 0;
        invocations.push(step);
        if (step < 3) {
          await world.queue('__wkf_workflow_demo', {
            runId: m.runId,
            step: step + 1,
          } as any);
        }
      },
    }));
    await harness.world.queue('__wkf_workflow_demo', { runId: 'r1', step: 0 } as any);
    harness.world.start();
    await harness.world.waitForIdle();
    expect(invocations).toEqual([0, 1, 2, 3]);
    // The continuation steps were driven by 307 redirects (hop counter rises),
    // not by independent dispatcher polls.
    expect(harness.requests.some((u) => u.includes('hop=1'))).toBe(true);
    expect(harness.requests.some((u) => u.includes('hop=2'))).toBe(true);
  });

  it('durably reschedules on timeoutSeconds (sleep semantics)', async () => {
    let deliveries = 0;
    let completed = false;
    harness = await startHarness(() => ({
      flow: async () => {
        deliveries++;
        if (deliveries === 1) return { timeoutSeconds: 0.05 };
        completed = true;
      },
    }));
    await harness.world.queue('__wkf_workflow_demo', { runId: 'r1' } as any);
    harness.world.start();
    await harness.world.waitForIdle(5000);
    expect(deliveries).toBeGreaterThanOrEqual(2);
    expect(completed).toBe(true);
  });

  it('retries on a 5xx (thrown handler) until success', async () => {
    let deliveries = 0;
    let completed = false;
    harness = await startHarness(() => ({
      flow: async () => {
        deliveries++;
        if (deliveries === 1) throw new Error('boom');
        completed = true;
      },
    }));
    await harness.world.queue('__wkf_workflow_demo', { runId: 'r1' } as any);
    harness.world.start();
    await harness.world.waitForIdle(5000);
    expect(deliveries).toBe(2);
    expect(completed).toBe(true);
  });

  it('satisfies the health-check probe on both workflow and step endpoints', async () => {
    harness = await startHarness(() => ({
      flow: async () => undefined,
    }));
    const { healthCheck, setWorld } = await import('@workflow/core/runtime');
    setWorld(harness.world);
    try {
      const wf = await healthCheck(harness.world, 'workflow', { timeout: 5000 });
      expect(wf.healthy).toBe(true);
      const step = await healthCheck(harness.world, 'step', { timeout: 5000 });
      expect(step.healthy).toBe(true);
    } finally {
      setWorld(undefined);
    }
  });

  it('delays delivery by delaySeconds', async () => {
    const times: number[] = [];
    const enqueuedAt = Date.now();
    harness = await startHarness(() => ({
      flow: async () => {
        times.push(Date.now() - enqueuedAt);
      },
    }));
    await harness.world.queue(
      '__wkf_workflow_demo',
      { runId: 'r1' } as any,
      { delaySeconds: 0.2 }
    );
    harness.world.start();
    await harness.world.waitForIdle(5000);
    expect(times).toHaveLength(1);
    expect(times[0]).toBeGreaterThanOrEqual(180);
  });
});
