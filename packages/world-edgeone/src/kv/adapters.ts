/**
 * Adapters for KV-namespace-like backends.
 *
 * EdgeOne Pages KV (and Workers KV — the API is intentionally similar) shape:
 *
 *   get(key, options?) -> Promise<string | null>
 *   put(key, value, options?) -> Promise<void>   // options: { expirationTtl?: number }
 *   delete(key) -> Promise<void>
 *   list({ prefix, cursor, limit }) -> Promise<{
 *     keys: Array<{ name: string, expiration?, metadata? }>,
 *     list_complete: boolean,
 *     cursor?: string
 *   }>
 *
 * `adaptNamespace` normalises this to our internal `KV` shape and supplies a
 * read-then-write fallback for `ifNotExists` semantics since vanilla Workers
 * KV / EdgeOne KV does not support atomic put-if-absent. The race window is
 * intentionally narrow but cannot be eliminated without an atomic primitive;
 * the hook token claim flow (see `storage.ts`) is the only operation that
 * relies on it and it has a `hook_conflict` event fallback for losers.
 */

import type { KV, KVNamespaceLike } from './types.js';

export class InMemoryKV implements KV {
  private map = new Map<string, { value: string; expiresAt?: number }>();
  private cleanExpired(): void {
    const now = Date.now();
    for (const [k, v] of this.map) {
      if (v.expiresAt && v.expiresAt < now) this.map.delete(k);
    }
  }
  async get(key: string): Promise<string | null> {
    this.cleanExpired();
    return this.map.get(key)?.value ?? null;
  }
  async put(
    key: string,
    value: string,
    opts?: { ttlSeconds?: number; ifNotExists?: boolean }
  ): Promise<{ ok: boolean }> {
    this.cleanExpired();
    if (opts?.ifNotExists && this.map.has(key)) return { ok: false };
    const entry: { value: string; expiresAt?: number } = { value };
    if (opts?.ttlSeconds) entry.expiresAt = Date.now() + opts.ttlSeconds * 1000;
    this.map.set(key, entry);
    return { ok: true };
  }
  async delete(key: string): Promise<void> {
    this.map.delete(key);
  }
  async list(opts: {
    prefix: string;
    cursor?: string;
    limit?: number;
  }): Promise<{ keys: string[]; cursor?: string; complete: boolean }> {
    this.cleanExpired();
    const limit = opts.limit ?? 1000;
    const cursorIdx = opts.cursor ? Number(opts.cursor) : 0;
    const all = [...this.map.keys()].filter((k) => k.startsWith(opts.prefix)).sort();
    const slice = all.slice(cursorIdx, cursorIdx + limit);
    const next = cursorIdx + slice.length;
    const complete = next >= all.length;
    return {
      keys: slice,
      cursor: complete ? undefined : String(next),
      complete,
    };
  }
}

interface NamespaceListResponse {
  keys?: Array<{ name: string } | string>;
  list_complete?: boolean;
  cursor?: string;
}

export function adaptNamespace(ns: KVNamespaceLike): KV {
  return {
    async get(key: string): Promise<string | null> {
      const out = await ns.get(key);
      return out == null ? null : String(out);
    },
    async put(
      key: string,
      value: string,
      opts?: { ttlSeconds?: number; ifNotExists?: boolean }
    ): Promise<{ ok: boolean }> {
      if (opts?.ifNotExists) {
        // No atomic put-if-absent on EdgeOne / Workers KV; do a check-then-put.
        // Races are theoretically possible; storage relies on this only for
        // the hook token claim, where losers get a `hook_conflict` event
        // anyway, so the worst case is an extra event row, not corruption.
        const existing = await ns.get(key);
        if (existing != null) return { ok: false };
      }
      const putOpts: Record<string, unknown> = {};
      if (opts?.ttlSeconds && opts.ttlSeconds >= 60) {
        // EdgeOne / Workers KV require expirationTtl >= 60 seconds.
        putOpts.expirationTtl = opts.ttlSeconds;
      }
      await ns.put(key, value, Object.keys(putOpts).length ? putOpts : undefined);
      return { ok: true };
    },
    async delete(key: string): Promise<void> {
      await ns.delete(key);
    },
    async list(opts: {
      prefix: string;
      cursor?: string;
      limit?: number;
    }): Promise<{ keys: string[]; cursor?: string; complete: boolean }> {
      const listOpts: Record<string, unknown> = { prefix: opts.prefix };
      if (opts.cursor) listOpts.cursor = opts.cursor;
      if (opts.limit) listOpts.limit = opts.limit;
      const raw = (await ns.list(listOpts)) as NamespaceListResponse;
      const keys = (raw.keys ?? []).map((k) =>
        typeof k === 'string' ? k : k.name
      );
      const complete = raw.list_complete !== false;
      const result: { keys: string[]; cursor?: string; complete: boolean } = {
        keys,
        complete,
      };
      if (raw.cursor) result.cursor = raw.cursor;
      return result;
    },
  };
}
