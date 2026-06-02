import { execSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const dir = path.dirname(fileURLToPath(import.meta.url));
const flowPath = path.join(dir, '.well-known', 'workflow', 'v1', 'flow.mjs');

// Self-contained: uses the in-memory world so no external Redis is required.
// Set TEST_REDIS_URL to run the same suite against a real Redis instead.
process.env.WORKFLOW_TARGET_WORLD = '@open-workflow/world-redirect';
process.env.WORKFLOW_REDIS_URL = process.env.TEST_REDIS_URL ?? 'memory';
process.env.WORKFLOW_REDIS_KEY_PREFIX = `e2e_${Date.now()}`;

type Host = Awaited<ReturnType<typeof import('@open-workflow/host').startHost>>;
let host: Host;
let workflowIds: Record<string, string> = {};

function loadWorkflowIds(): Record<string, string> {
  const manifest = JSON.parse(
    readFileSync(path.join(dir, '.well-known/workflow/v1/manifest.json'), 'utf8')
  );
  const ids: Record<string, string> = {};
  for (const file of Object.values(manifest.workflows ?? {})) {
    for (const [fnName, entry] of Object.entries(file as Record<string, any>)) {
      ids[fnName] = entry.workflowId;
    }
  }
  return ids;
}

async function waitFor<T>(
  fn: () => Promise<T>,
  predicate: (v: T) => boolean,
  timeoutMs = 20_000
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T = await fn();
  while (Date.now() < deadline) {
    last = await fn();
    if (predicate(last)) return last;
    await new Promise((r) => setTimeout(r, 50));
  }
  return last;
}

beforeAll(async () => {
  if (!existsSync(flowPath)) {
    execSync('pnpm exec workflow build --target standalone', {
      cwd: dir,
      stdio: 'inherit',
    });
  }
  workflowIds = loadWorkflowIds();
  const { startHost } = await import('@open-workflow/host');
  host = await startHost({ workDir: dir, port: 0 });
}, 60_000);

afterAll(async () => {
  await host?.close();
});

describe('open-workflow standalone e2e (Redis + 307)', () => {
  it('runs a workflow with steps and durable retries', async () => {
    const { start } = await import('workflow/api');
    const run = await start({ workflowId: workflowIds.hello! }, ['World']);
    const status = await waitFor(
      () => run.status,
      (s) => s === 'completed' || s === 'failed'
    );
    expect(status).toBe('completed');
    const result = (await run.returnValue) as any;
    expect(result.message).toContain('Hello, World!');
    expect(result.message).toContain('attempt 2');
  });

  it('runs parallel steps and a durable sleep', async () => {
    const { start } = await import('workflow/api');
    const run = await start({ workflowId: workflowIds.controlFlow! }, []);
    const status = await waitFor(
      () => run.status,
      (s) => s === 'completed' || s === 'failed'
    );
    expect(status).toBe('completed');
    const result = (await run.returnValue) as any;
    expect(result.values).toEqual([2, 4, 6]);
    expect(result.sum).toBe(12);
  });

  it('suspends on a webhook and resumes via the public endpoint', async () => {
    const { start } = await import('workflow/api');
    const run = await start({ workflowId: workflowIds.approval! }, []);

    // Wait for the webhook hook to be registered, then resume it over HTTP.
    const hooks = await waitFor(
      () => host.world.hooks.list({ runId: run.runId }),
      (page) => page.data.length > 0
    );
    const token = hooks.data[0]!.token;
    const res = await fetch(
      `${host.url}/.well-known/workflow/v1/webhook/${token}`,
      { method: 'POST', body: 'approved!' }
    );
    expect(res.status).toBeLessThan(400);

    const status = await waitFor(
      () => run.status,
      (s) => s === 'completed' || s === 'failed'
    );
    expect(status).toBe('completed');
    const result = (await run.returnValue) as any;
    expect(result.approved).toBe(true);
    expect(result.payload).toBe('approved!');
  });
});
