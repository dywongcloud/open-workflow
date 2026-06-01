#!/usr/bin/env node
import { startHost } from './index.js';

async function main(): Promise<void> {
  const host = await startHost();
  console.log(`[open-workflow] host listening on ${host.url}`);
  console.log(`[open-workflow] flow:    ${host.url}/.well-known/workflow/v1/flow`);
  console.log(`[open-workflow] webhook: ${host.url}/.well-known/workflow/v1/webhook/<token>`);
  console.log('[open-workflow] 307 dispatch pump running. Ctrl-C to stop.');

  const shutdown = async () => {
    console.log('\n[open-workflow] shutting down...');
    await host.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[open-workflow] failed to start host:', err);
  process.exit(1);
});
