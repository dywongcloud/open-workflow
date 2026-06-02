/**
 * Minimal KV adapter interface — designed so EdgeOne Pages KV, Cloudflare
 * Workers KV, Deno KV, or even a plain Map can all back this world. The
 * EdgeOne binding (`@edgeone/pages-kv` style namespace) is mapped onto
 * this shape in `binding.ts`.
 */
export interface KV {
  /** Returns the raw string value or null if the key is absent. */
  get(key: string): Promise<string | null>;
  /**
   * Stores a string value at `key`.
   * `ifNotExists`: best-effort atomic create — should reject (return ok:false)
   * if the key already has a value. If the backend can't enforce atomicity,
   * a read-then-write fallback is acceptable but RACES are POSSIBLE.
   */
  put(
    key: string,
    value: string,
    opts?: { ttlSeconds?: number; ifNotExists?: boolean }
  ): Promise<{ ok: boolean }>;
  delete(key: string): Promise<void>;
  /**
   * Lists keys with the given prefix. `limit` is a hint; backends may cap it.
   * The `complete` flag is true when no more pages remain.
   */
  list(opts: {
    prefix: string;
    cursor?: string;
    limit?: number;
  }): Promise<{ keys: string[]; cursor?: string; complete: boolean }>;
}

export interface KVNamespaceLike {
  get(key: string, opts?: any): Promise<string | null> | string | null;
  put(key: string, value: string, opts?: any): Promise<any> | any;
  delete(key: string): Promise<any> | any;
  list(opts?: any): Promise<any> | any;
}
