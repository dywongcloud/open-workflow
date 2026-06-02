/**
 * Resolve a KV namespace from the EdgeOne Pages runtime.
 *
 * EdgeOne Pages exposes KV bindings through different surfaces depending on
 * how the function is invoked:
 *   1. `globalThis[<bindingName>]` — direct injection (Workers-style global).
 *   2. `process.env[<bindingName>]` — value is the namespace object, not a
 *      string. Some runtimes set this for Node-compat handlers.
 *   3. A platform-specific accessor like `getEdgeOneContext().env.<name>`.
 *
 * We try each in order, falling back to a clear error if nothing matches.
 * Callers who already have a handle can also pass it directly to
 * `createKVWorld({ kv })` and bypass auto-discovery entirely.
 */

import type { KV, KVNamespaceLike } from './types.js';
import { adaptNamespace, InMemoryKV } from './adapters.js';

export interface ResolveOptions {
  /** Binding name to look up. Defaults to `EDGEONE_KV` (or env var override). */
  bindingName?: string;
  /** Allow falling back to an in-process map for dev (default: false). */
  allowInMemoryFallback?: boolean;
}

function readBindingName(opts: ResolveOptions): string {
  return (
    opts.bindingName ||
    process.env.WORKFLOW_EDGEONE_KV_BINDING ||
    process.env.EDGEONE_KV_BINDING ||
    'EDGEONE_KV'
  );
}

function lookupCandidate(name: string): unknown {
  const g = globalThis as Record<string, unknown>;
  if (g[name] && typeof g[name] === 'object') return g[name];

  const env = process.env as unknown as Record<string, unknown>;
  if (env[name] && typeof env[name] === 'object') return env[name];

  const ctxFn = (g as any).getEdgeOneContext || (g as any).getEdgeContext;
  if (typeof ctxFn === 'function') {
    try {
      const ctx = ctxFn();
      const cand = ctx?.env?.[name] ?? ctx?.[name];
      if (cand && typeof cand === 'object') return cand;
    } catch {
      // ignore — fall through to error
    }
  }
  return null;
}

function isNamespaceLike(x: unknown): x is KVNamespaceLike {
  if (!x || typeof x !== 'object') return false;
  const o = x as Record<string, unknown>;
  return (
    typeof o.get === 'function' &&
    typeof o.put === 'function' &&
    typeof o.delete === 'function' &&
    typeof o.list === 'function'
  );
}

export function resolveEdgeOneKV(opts: ResolveOptions = {}): KV {
  const bindingName = readBindingName(opts);
  const cand = lookupCandidate(bindingName);
  if (isNamespaceLike(cand)) return adaptNamespace(cand);

  if (opts.allowInMemoryFallback || process.env.WORKFLOW_EDGEONE_KV_MEMORY === '1') {
    return new InMemoryKV();
  }

  throw new Error(
    `EdgeOne KV binding '${bindingName}' not found. Bind a KV namespace ` +
      `in your EdgeOne Pages function settings, or set ` +
      `WORKFLOW_EDGEONE_KV_BINDING=<your-binding-name>. For local dev, set ` +
      `WORKFLOW_EDGEONE_KV_MEMORY=1 to use an in-process map.`
  );
}
