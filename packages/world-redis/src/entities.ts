import type {
  Event,
  Hook,
  PaginationOptions,
  Step,
  Wait,
  WorkflowRun,
} from '@workflow/world';
import {
  EventSchema,
  HookSchema,
  StepSchema,
  WaitSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import type { RedisClient } from './client/types.js';
import { decodeBlob, encodeBlob } from './codec.js';
import type { Keys } from './keys.js';

export interface Page<T> {
  data: T[];
  cursor: string | null;
  hasMore: boolean;
}

function encodeCursor(offset: number): string {
  return Buffer.from(JSON.stringify({ o: offset })).toString('base64url');
}

function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString());
    return typeof parsed?.o === 'number' && parsed.o >= 0 ? parsed.o : 0;
  } catch {
    return 0;
  }
}

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

function clampLimit(limit: number | undefined, fallback = DEFAULT_LIMIT) {
  if (!limit || limit <= 0) return fallback;
  return Math.min(limit, MAX_LIMIT);
}

/**
 * Low-level entity persistence + secondary indexes over Redis. Stateless;
 * all mutation ordering/atomicity is handled by the caller (RunLocks) in
 * storage.ts. A single global monotonic sequence supplies stable total
 * ordering for index ZSETs.
 */
export class EntityStore {
  constructor(
    private readonly redis: RedisClient,
    private readonly keys: Keys
  ) {}

  // Index scores use the entity's immutable createdAt timestamp, so
  // re-writing an entity (status updates) keeps its position stable. ULID
  // ids break same-millisecond ties in creation order.

  // ---------------- runs ----------------

  async getRun(runId: string): Promise<WorkflowRun | null> {
    const raw = await this.redis.get(this.keys.run(runId));
    if (!raw) return null;
    return WorkflowRunSchema.parse(decodeBlob(raw));
  }

  async writeRun(run: WorkflowRun, prevStatus?: string): Promise<void> {
    const score = run.createdAt.getTime();
    await this.redis.set(this.keys.run(run.runId), encodeBlob(run));
    await this.redis.zadd(this.keys.runsIndex(), score, run.runId);
    if (prevStatus && prevStatus !== run.status) {
      await this.redis.zrem(this.keys.runsByStatus(prevStatus), run.runId);
    }
    await this.redis.zadd(this.keys.runsByStatus(run.status), score, run.runId);
  }

  async listRuns(params: {
    status?: string;
    pagination?: PaginationOptions;
  }): Promise<Page<WorkflowRun>> {
    const key = params.status
      ? this.keys.runsByStatus(params.status)
      : this.keys.runsIndex();
    const ids = await this.pageIds(key, params.pagination, 'desc');
    const runs = await Promise.all(ids.data.map((id) => this.getRun(id)));
    return { ...ids, data: runs.filter((r): r is WorkflowRun => r !== null) };
  }

  // ---------------- steps ----------------

  async getStep(runId: string, stepId: string): Promise<Step | null> {
    const raw = await this.redis.get(this.keys.step(runId, stepId));
    if (!raw) return null;
    return StepSchema.parse(decodeBlob(raw));
  }

  async writeStep(step: Step): Promise<void> {
    const score = step.createdAt.getTime();
    await this.redis.set(
      this.keys.step(step.runId, step.stepId),
      encodeBlob(step)
    );
    await this.redis.zadd(this.keys.stepsIndex(step.runId), score, step.stepId);
  }

  async listSteps(params: {
    runId: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Step>> {
    const ids = await this.pageIds(
      this.keys.stepsIndex(params.runId),
      params.pagination,
      'desc'
    );
    const steps = await Promise.all(
      ids.data.map((id) => this.getStep(params.runId, id))
    );
    return { ...ids, data: steps.filter((s): s is Step => s !== null) };
  }

  // ---------------- hooks ----------------

  async getHook(hookId: string): Promise<Hook | null> {
    const raw = await this.redis.get(this.keys.hook(hookId));
    if (!raw) return null;
    return HookSchema.parse(decodeBlob(raw)) as Hook;
  }

  async writeHook(hook: Hook): Promise<void> {
    const score = hook.createdAt.getTime();
    await this.redis.set(this.keys.hook(hook.hookId), encodeBlob(hook));
    await this.redis.zadd(this.keys.hooksIndex(), score, hook.hookId);
    await this.redis.zadd(this.keys.hooksByRun(hook.runId), score, hook.hookId);
  }

  async deleteHook(hook: Hook, tokenHash: string): Promise<void> {
    await this.redis.del(this.keys.hook(hook.hookId));
    await this.redis.zrem(this.keys.hooksIndex(), hook.hookId);
    await this.redis.zrem(this.keys.hooksByRun(hook.runId), hook.hookId);
    await this.redis.del(this.keys.hookToken(tokenHash));
  }

  async listHooks(params: {
    runId?: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Hook>> {
    const key = params.runId
      ? this.keys.hooksByRun(params.runId)
      : this.keys.hooksIndex();
    const ids = await this.pageIds(key, params.pagination, 'desc');
    const hooks = await Promise.all(ids.data.map((id) => this.getHook(id)));
    return { ...ids, data: hooks.filter((h): h is Hook => h !== null) };
  }

  // ---------------- waits ----------------

  async getWait(runId: string, correlationId: string): Promise<Wait | null> {
    const raw = await this.redis.get(this.keys.wait(runId, correlationId));
    if (!raw) return null;
    return WaitSchema.parse(decodeBlob(raw));
  }

  async writeWait(wait: Wait, correlationId: string): Promise<void> {
    const score = wait.createdAt.getTime();
    await this.redis.set(
      this.keys.wait(wait.runId, correlationId),
      encodeBlob(wait)
    );
    await this.redis.zadd(
      this.keys.waitsByRun(wait.runId),
      score,
      correlationId
    );
  }

  // ---------------- terminal cleanup ----------------

  async deleteAllHooksForRun(runId: string): Promise<void> {
    const ids = await this.redis.zrangeByRank(
      this.keys.hooksByRun(runId),
      0,
      -1
    );
    for (const id of ids) {
      const hook = await this.getHook(id);
      if (hook) {
        const { hashToken } = await import('./ids.js');
        await this.deleteHook(hook, hashToken(hook.token));
      }
    }
    await this.redis.del(this.keys.hooksByRun(runId));
  }

  async deleteAllWaitsForRun(runId: string): Promise<void> {
    const corrIds = await this.redis.zrangeByRank(
      this.keys.waitsByRun(runId),
      0,
      -1
    );
    for (const cid of corrIds) {
      await this.redis.del(this.keys.wait(runId, cid));
    }
    await this.redis.del(this.keys.waitsByRun(runId));
  }

  // ---------------- events ----------------

  async appendEvent(event: Event): Promise<void> {
    await this.redis.hset(
      this.keys.eventData(event.runId),
      event.eventId,
      encodeBlob(event)
    );
    await this.redis.rpush(this.keys.eventList(event.runId), event.eventId);
    if (event.correlationId) {
      await this.redis.rpush(
        this.keys.corrIndex(event.correlationId),
        `${event.runId}|${event.eventId}`
      );
    }
  }

  async getEvent(runId: string, eventId: string): Promise<Event | null> {
    const raw = await this.redis.hget(this.keys.eventData(runId), eventId);
    if (!raw) return null;
    return EventSchema.parse(decodeBlob(raw));
  }

  async listEvents(params: {
    runId: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Event>> {
    const listKey = this.keys.eventList(params.runId);
    const total = await this.redis.llen(listKey);
    const limit = clampLimit(params.pagination?.limit, MAX_LIMIT);
    const offset = decodeCursor(params.pagination?.cursor);
    const sortOrder = params.pagination?.sortOrder ?? 'asc';

    let ids: string[];
    if (sortOrder === 'asc') {
      ids = await this.redis.lrange(listKey, offset, offset + limit - 1);
    } else {
      // newest first: walk from the end
      const start = Math.max(0, total - offset - limit);
      const end = total - offset - 1;
      ids = end < 0 ? [] : await this.redis.lrange(listKey, start, end);
      ids.reverse();
    }

    const events = await Promise.all(
      ids.map((id) => this.getEvent(params.runId, id))
    );
    const data = events.filter((e): e is Event => e !== null);
    const consumed = offset + ids.length;
    const hasMore = consumed < total;
    // The event log always returns a resumable tail cursor when there are
    // events, so the runtime can incrementally fetch events appended later
    // (not just the currently-available pages). This differs from standard
    // entity pagination, which returns null at the end.
    return {
      data,
      cursor: total === 0 ? null : encodeCursor(consumed),
      hasMore,
    };
  }

  async listEventsByCorrelationId(params: {
    correlationId: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Event>> {
    const listKey = this.keys.corrIndex(params.correlationId);
    const refs = await this.redis.lrange(listKey, 0, -1);
    const limit = clampLimit(params.pagination?.limit, MAX_LIMIT);
    const offset = decodeCursor(params.pagination?.cursor);
    const ordered =
      (params.pagination?.sortOrder ?? 'asc') === 'asc'
        ? refs
        : [...refs].reverse();
    const slice = ordered.slice(offset, offset + limit);
    const events = await Promise.all(
      slice.map(async (ref) => {
        const [runId, eventId] = ref.split('|');
        return runId && eventId ? this.getEvent(runId, eventId) : null;
      })
    );
    const data = events.filter((e): e is Event => e !== null);
    const consumed = offset + slice.length;
    const hasMore = consumed < ordered.length;
    return {
      data,
      cursor: ordered.length === 0 ? null : encodeCursor(consumed),
      hasMore,
    };
  }

  // ---------------- helpers ----------------

  private async pageIds(
    zsetKey: string,
    pagination: PaginationOptions | undefined,
    defaultOrder: 'asc' | 'desc'
  ): Promise<Page<string>> {
    const total = await this.redis.zcard(zsetKey);
    const limit = clampLimit(pagination?.limit);
    const offset = decodeCursor(pagination?.cursor);
    const rev = (pagination?.sortOrder ?? defaultOrder) === 'desc';
    const data = await this.redis.zrangeByRank(
      zsetKey,
      offset,
      offset + limit - 1,
      rev
    );
    const consumed = offset + data.length;
    const hasMore = consumed < total;
    return { data, cursor: hasMore ? encodeCursor(consumed) : null, hasMore };
  }
}
