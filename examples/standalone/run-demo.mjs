// End-to-end demo: runs the `hello` workflow on open-workflow (Redis + 307),
// entirely self-hosted. Requires a Redis reachable at WORKFLOW_REDIS_URL
// (defaults to redis://127.0.0.1:6379) and a prior `pnpm build`.

const PORT = Number(process.env.PORT ?? 3010);
process.env.WORKFLOW_TARGET_WORLD = '@open-workflow/world-redirect';
process.env.WORKFLOW_REDIS_URL ??= 'redis://127.0.0.1:6379';
process.env.WORKFLOW_REDIS_KEY_PREFIX ??= `demo_${Date.now()}`;
process.env.WORKFLOW_BASE_URL = `http://localhost:${PORT}`;

const { startHost } = await import('@open-workflow/host');
const { start } = await import('workflow/api');

const host = await startHost({ workDir: process.cwd(), port: PORT });
console.log(`host up at ${host.url}`);

try {
  const run = await start(
    { workflowId: 'workflow//./workflows/hello//hello' },
    ['World']
  );
  console.log('started run', run.runId);

  const deadline = Date.now() + 30_000;
  let status = 'pending';
  while (Date.now() < deadline) {
    status = await run.status;
    if (status === 'completed' || status === 'failed' || status === 'cancelled') {
      break;
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  console.log('final status:', status);
  if (status === 'completed') {
    console.log('return value:', await run.returnValue);
  }
  process.exitCode = status === 'completed' ? 0 : 1;
} finally {
  await host.close();
}
