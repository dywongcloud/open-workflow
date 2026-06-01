import { readFile } from 'node:fs/promises';
import { createServer, type Server } from 'node:http';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { getWorld } from '@workflow/core/runtime';
import type { World } from '@workflow/world';
import { toWebRequest, writeWebResponse } from './http.js';

type RouteHandler = (req: Request) => Promise<Response> | Response;

export interface HostOptions {
  /**
   * Directory containing the standalone build output
   * (`.well-known/workflow/v1/{flow,webhook}.mjs`). Default: process.cwd().
   */
  workDir?: string;
  /** Port to listen on. Default: process.env.PORT or 3000. */
  port?: number;
  /** Hostname to bind. Default: 0.0.0.0. */
  hostname?: string;
  /**
   * Public base URL the dispatcher uses to reach this host's endpoints.
   * Default: http://localhost:{port}. Set this when behind a proxy.
   */
  baseUrl?: string;
  /** Convenience: sets WORKFLOW_REDIS_URL for the world. */
  redisUrl?: string;
}

export interface RunningHost {
  url: string;
  server: Server;
  world: World;
  close(): Promise<void>;
}

const WELL_KNOWN = '/.well-known/workflow/v1';

async function importRoute(file: string): Promise<Record<string, RouteHandler> | null> {
  try {
    return (await import(pathToFileURL(file).href)) as Record<
      string,
      RouteHandler
    >;
  } catch {
    return null;
  }
}

/**
 * Start a self-hostable workflow host backed by @open-workflow/world-redis.
 *
 * Serves the standalone build's flow + webhook endpoints and runs the 307
 * dispatch pump. The flow handler and the dispatcher share one world-redis
 * singleton (resolved from node_modules), so enqueues from the runtime and the
 * pump coordinate through the same Redis.
 */
export async function startHost(options: HostOptions = {}): Promise<RunningHost> {
  const workDir = path.resolve(options.workDir ?? process.cwd());
  const requestedPort = options.port ?? Number(process.env.PORT ?? 3000);
  const hostname = options.hostname ?? '0.0.0.0';

  // Configure target world / connection up front; baseUrl is resolved after
  // listen() so an ephemeral port (0) produces a correct dispatcher URL.
  process.env.WORKFLOW_TARGET_WORLD ??= '@open-workflow/world-redis';
  if (options.redisUrl) process.env.WORKFLOW_REDIS_URL = options.redisUrl;

  const v1Dir = path.join(workDir, '.well-known', 'workflow', 'v1');
  const flowModule = await importRoute(path.join(v1Dir, 'flow.mjs'));
  const webhookModule = await importRoute(path.join(v1Dir, 'webhook.mjs'));

  const flowHandler = flowModule?.POST;
  if (!flowHandler) {
    throw new Error(
      `[open-workflow host] No flow handler found at ${v1Dir}/flow.mjs. ` +
        'Run `workflow build` (target standalone) in this directory first.'
    );
  }

  // Assigned after listen(); captured by the request handler closure.
  let baseUrl = '';

  const server = createServer((req, res) => {
    void (async () => {
      const url = req.url ?? '/';
      const pathname = url.split('?')[0] ?? '/';
      const method = req.method ?? 'GET';

      if (pathname === '/health' || pathname === '/') {
        res.statusCode = 200;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ ok: true, world: 'open-workflow/redis' }));
        return;
      }

      if (pathname === `${WELL_KNOWN}/flow` || pathname === `${WELL_KNOWN}/step`) {
        const request = await toWebRequest(req, baseUrl);
        await writeWebResponse(res, await flowHandler(request));
        return;
      }

      if (pathname === `${WELL_KNOWN}/manifest.json`) {
        try {
          const manifest = await readFile(
            path.join(v1Dir, 'manifest.json'),
            'utf8'
          );
          res.statusCode = 200;
          res.setHeader('content-type', 'application/json');
          res.end(manifest);
        } catch {
          res.statusCode = 404;
          res.end('manifest not found');
        }
        return;
      }

      if (pathname.startsWith(`${WELL_KNOWN}/webhook/`)) {
        const webhookHandler = webhookModule?.[method] ?? webhookModule?.POST;
        if (!webhookHandler) {
          res.statusCode = 404;
          res.end('no webhook handler');
          return;
        }
        const request = await toWebRequest(req, baseUrl);
        await writeWebResponse(res, await webhookHandler(request));
        return;
      }

      res.statusCode = 404;
      res.end('not found');
    })().catch((err) => {
      console.error('[open-workflow host] request error:', err);
      if (!res.headersSent) res.statusCode = 500;
      res.end('internal error');
    });
  });

  await new Promise<void>((resolve) =>
    server.listen(requestedPort, hostname, resolve)
  );

  const address = server.address();
  const actualPort =
    typeof address === 'object' && address ? address.port : requestedPort;
  baseUrl = (
    options.baseUrl ??
    process.env.WORKFLOW_BASE_URL ??
    `http://localhost:${actualPort}`
  ).replace(/\/$/, '');
  process.env.WORKFLOW_BASE_URL = baseUrl;

  // Obtain the world through core's loader so the host shares the exact same
  // instance the flow handler uses (cached on globalThis, resolved from the
  // app's WORKFLOW_TARGET_WORLD). Created after baseUrl is known so the
  // dispatcher targets the right port.
  const world = await getWorld();
  await world.start?.();

  return {
    url: baseUrl,
    server,
    world,
    async close() {
      await world.close?.();
      await new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve()))
      );
    },
  };
}
