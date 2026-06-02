import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  Event,
  EventResult,
  Hook,
  PaginationOptions,
  Step,
  Storage,
  Wait,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import {
  SPEC_VERSION_SUPPORTS_EVENT_SOURCING,
  stripEventDataRefs,
  validateUlidTimestamp,
} from '@workflow/world';
import {
  assertSafeEntityId,
  decodeBlob,
  encodeBlob,
  hashToken,
  newEventId,
  newRunId,
} from './codec.js';
import { rowToRecord, type TabName, TAB_COLUMNS } from './schema.js';
import type { SheetsClient } from './sheets.js';

const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
const isTerminal = (s: string) => TERMINAL.has(s);

interface RowHit<T> {
  rowNumber: number; // 1-based, including header row (so first data row is 2)
  record: T;
}

/**
 * In-process per-run serialization. Sheets has no transactions, so we
 * serialize all entity writes for a given run within this process. Across
 * multiple host processes the world is still racy on the per-step terminal
 * guards — acceptable for the low-volume "operations sheet" use case;
 * document it in the README.
 */
class RunMutex {
  private inflight = new Map<string, Promise<unknown>>();
  async run<T>(runId: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.inflight.get(runId);
    const box: { task?: Promise<T> } = {};
    const task = (async () => {
      if (prev) await prev.catch(() => undefined);
      try {
        return await fn();
      } finally {
        if (this.inflight.get(runId) === box.task) this.inflight.delete(runId);
      }
    })();
    box.task = task;
    this.inflight.set(runId, task);
    return task;
  }
}

function isoOrEmpty(v: Date | undefined | null): string {
  return v ? v.toISOString() : '';
}
function parseIsoOrUndef(v: string | undefined): Date | undefined {
  if (!v) return undefined;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? undefined : d;
}

function runFromRow(record: Record<string, string>): WorkflowRun | null {
  if (!record.runId) return null;
  const base = {
    runId: record.runId,
    workflowName: record.workflowName,
    deploymentId: record.deploymentId,
    specVersion: record.specVersion ? Number(record.specVersion) : undefined,
    errorCode: record.errorCode || undefined,
    createdAt: new Date(record.createdAtIso),
    updatedAt: new Date(record.createdAtIso),
    startedAt: parseIsoOrUndef(record.startedAtIso),
    completedAt: parseIsoOrUndef(record.completedAtIso),
    expiredAt: parseIsoOrUndef(record.expiredAtIso),
    attributes: record.attributesJson ? JSON.parse(record.attributesJson) : {},
    input: decodeBlob(record.inputB64),
    output: decodeBlob(record.outputB64),
    error: decodeBlob(record.errorB64),
    executionContext: record.executionContextJson
      ? JSON.parse(record.executionContextJson)
      : undefined,
  };
  return { ...base, status: (record.status as WorkflowRun['status']) } as WorkflowRun;
}

function runToRecord(run: WorkflowRun): Record<string, string> {
  return {
    runId: run.runId,
    workflowName: run.workflowName,
    status: run.status,
    deploymentId: run.deploymentId,
    specVersion: String(run.specVersion ?? ''),
    errorCode: run.errorCode ?? '',
    createdAtIso: isoOrEmpty(run.createdAt),
    startedAtIso: isoOrEmpty(run.startedAt),
    completedAtIso: isoOrEmpty(run.completedAt),
    expiredAtIso: isoOrEmpty(run.expiredAt),
    attributesJson: JSON.stringify(run.attributes ?? {}),
    inputB64: run.input ? encodeBlob(run.input) : '',
    outputB64: run.output ? encodeBlob(run.output) : '',
    errorB64: run.error ? encodeBlob(run.error) : '',
    executionContextJson: run.executionContext
      ? JSON.stringify(run.executionContext)
      : '',
  };
}

function stepFromRow(record: Record<string, string>): Step | null {
  if (!record.stepId) return null;
  return {
    runId: record.runId,
    stepId: record.stepId,
    stepName: record.stepName,
    status: record.status as Step['status'],
    attempt: Number(record.attempt || '0'),
    specVersion: record.specVersion ? Number(record.specVersion) : undefined,
    createdAt: new Date(record.createdAtIso),
    updatedAt: new Date(record.createdAtIso),
    startedAt: parseIsoOrUndef(record.startedAtIso),
    completedAt: parseIsoOrUndef(record.completedAtIso),
    retryAfter: parseIsoOrUndef(record.retryAfterIso),
    input: decodeBlob(record.inputB64),
    output: decodeBlob(record.outputB64),
    error: decodeBlob(record.errorB64),
  };
}

function stepToRecord(step: Step): Record<string, string> {
  return {
    stepId: step.stepId,
    runId: step.runId,
    stepName: step.stepName,
    status: step.status,
    attempt: String(step.attempt),
    specVersion: String(step.specVersion ?? ''),
    createdAtIso: isoOrEmpty(step.createdAt),
    startedAtIso: isoOrEmpty(step.startedAt),
    completedAtIso: isoOrEmpty(step.completedAt),
    retryAfterIso: isoOrEmpty(step.retryAfter),
    inputB64: step.input ? encodeBlob(step.input) : '',
    outputB64: step.output ? encodeBlob(step.output) : '',
    errorB64: step.error ? encodeBlob(step.error) : '',
  };
}

function hookFromRow(record: Record<string, string>): Hook | null {
  if (!record.hookId) return null;
  return {
    hookId: record.hookId,
    runId: record.runId,
    token: record.token,
    ownerId: record.ownerId,
    projectId: record.projectId,
    environment: record.environment,
    isWebhook: record.isWebhook === 'true',
    isSystem: record.isSystem === 'true',
    specVersion: record.specVersion ? Number(record.specVersion) : undefined,
    createdAt: new Date(record.createdAtIso),
    metadata: decodeBlob(record.metadataB64),
  };
}

function waitFromRow(record: Record<string, string>): Wait | null {
  if (!record.waitId) return null;
  return {
    waitId: record.waitId,
    runId: record.runId,
    status: record.status as Wait['status'],
    specVersion: record.specVersion ? Number(record.specVersion) : undefined,
    createdAt: new Date(record.createdAtIso),
    updatedAt: new Date(record.createdAtIso),
    resumeAt: parseIsoOrUndef(record.resumeAtIso),
    completedAt: parseIsoOrUndef(record.completedAtIso),
  };
}

function eventFromRow(record: Record<string, string>): Event | null {
  if (!record.eventId) return null;
  const ev: any = {
    eventId: record.eventId,
    runId: record.runId,
    eventType: record.eventType,
    correlationId: record.correlationId || undefined,
    specVersion: record.specVersion ? Number(record.specVersion) : undefined,
    createdAt: new Date(record.createdAtIso),
    eventData: decodeBlob(record.eventDataB64),
  };
  if (ev.eventData === undefined) delete ev.eventData;
  return ev;
}

function stripRunData(run: WorkflowRun): WorkflowRunWithoutData {
  return { ...run, input: undefined, output: undefined } as WorkflowRunWithoutData;
}

function paginateBy<T>(
  items: T[],
  pagination: PaginationOptions | undefined,
  defaultOrder: 'asc' | 'desc' = 'desc'
): { data: T[]; cursor: string | null; hasMore: boolean } {
  const order = pagination?.sortOrder ?? defaultOrder;
  const ordered = order === 'desc' ? [...items].reverse() : items;
  const limit = Math.min(Math.max(1, pagination?.limit ?? 100), 1000);
  const offset = pagination?.cursor
    ? Number.parseInt(Buffer.from(pagination.cursor, 'base64url').toString(), 10) || 0
    : 0;
  const page = ordered.slice(offset, offset + limit);
  const consumed = offset + page.length;
  const hasMore = consumed < ordered.length;
  return {
    data: page,
    cursor: hasMore
      ? Buffer.from(String(consumed)).toString('base64url')
      : null,
    hasMore,
  };
}

export function createSheetsStorage(
  sheets: SheetsClient,
  owner: { ownerId: string; projectId: string; environment: string }
): Storage & {
  /** Direct row-level reader — used by the dispatcher. */
  readonly raw: {
    runsForStatus: (
      status: WorkflowRun['status']
    ) => Promise<WorkflowRun[]>;
  };
} {
  const mutex = new RunMutex();

  async function findRow<T>(
    tab: TabName,
    column: string,
    value: string,
    transform: (rec: Record<string, string>) => T | null
  ): Promise<RowHit<T> | null> {
    const rows = await sheets.getAllRows(tab);
    const idx = TAB_COLUMNS[tab].indexOf(column);
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i]!;
      if (row[idx] === value) {
        const t = transform(rowToRecord(tab, row));
        if (t) return { rowNumber: i + 2, record: t };
      }
    }
    return null;
  }

  async function findAllRows<T>(
    tab: TabName,
    predicate: (rec: Record<string, string>) => boolean,
    transform: (rec: Record<string, string>) => T | null
  ): Promise<RowHit<T>[]> {
    const rows = await sheets.getAllRows(tab);
    const hits: RowHit<T>[] = [];
    for (let i = 0; i < rows.length; i++) {
      const rec = rowToRecord(tab, rows[i]!);
      if (predicate(rec)) {
        const t = transform(rec);
        if (t) hits.push({ rowNumber: i + 2, record: t });
      }
    }
    return hits;
  }

  async function writeRun(run: WorkflowRun): Promise<void> {
    const hit = await findRow('runs', 'runId', run.runId, runFromRow);
    const row = TAB_COLUMNS.runs.map((c) => runToRecord(run)[c] ?? '');
    if (hit) {
      await sheets.updateRow('runs', hit.rowNumber, row);
    } else {
      await sheets.appendRows('runs', [row]);
    }
  }

  async function writeStep(step: Step): Promise<void> {
    const hit = await findRow('steps', 'stepId', step.stepId, stepFromRow);
    const row = TAB_COLUMNS.steps.map((c) => stepToRecord(step)[c] ?? '');
    if (hit) {
      await sheets.updateRow('steps', hit.rowNumber, row);
    } else {
      await sheets.appendRows('steps', [row]);
    }
  }

  async function appendEvent(event: Event): Promise<void> {
    const record: Record<string, string> = {
      eventId: event.eventId,
      runId: event.runId,
      eventType: event.eventType,
      correlationId: event.correlationId ?? '',
      specVersion: String(event.specVersion ?? ''),
      createdAtIso: isoOrEmpty(event.createdAt),
      eventDataB64:
        'eventData' in event && (event as any).eventData !== undefined
          ? encodeBlob((event as any).eventData)
          : '',
    };
    await sheets.appendRows(
      'events',
      [TAB_COLUMNS.events.map((c) => record[c] ?? '')]
    );
  }

  // ----- events.create — the heart -----

  async function createEvent(
    runId: string | null,
    data: any,
    params?: { resolveData?: 'none' | 'all' }
  ): Promise<EventResult> {
    if (runId != null && runId !== '') assertSafeEntityId('runId', runId);
    if (typeof data.correlationId === 'string') {
      assertSafeEntityId('correlationId', data.correlationId);
    }
    let effectiveRunId: string;
    if (data.eventType === 'run_created' && (!runId || runId === '')) {
      effectiveRunId = newRunId();
    } else if (!runId) {
      throw new Error('runId is required for non-run_created events');
    } else {
      effectiveRunId = runId;
    }
    if (data.eventType === 'run_created' && runId && runId !== '') {
      const err = validateUlidTimestamp(effectiveRunId, 'wrun_');
      if (err) throw new WorkflowWorldError(err);
    }
    return mutex.run(effectiveRunId, () =>
      createImpl(effectiveRunId, data, params)
    );
  }

  async function createImpl(
    effectiveRunId: string,
    data: any,
    params?: { resolveData?: 'none' | 'all' }
  ): Promise<EventResult> {
    const eventId = newEventId();
    const now = new Date();
    const resolveData = params?.resolveData ?? 'all';
    const effectiveSpecVersion =
      data.specVersion ?? SPEC_VERSION_SUPPORTS_EVENT_SOURCING;

    // Fetch the run for validation (unless we're creating it / on a step
    // event that's allowed to skip).
    let currentRun: WorkflowRun | null = null;
    const skipRunRead =
      data.eventType === 'step_completed' ||
      data.eventType === 'step_retrying';
    if (data.eventType !== 'run_created' && !skipRunRead) {
      const hit = await findRow('runs', 'runId', effectiveRunId, runFromRow);
      currentRun = hit?.record ?? null;
    }
    if (data.eventType === 'run_failed' && !currentRun) {
      throw new WorkflowRunNotFoundError(effectiveRunId);
    }

    if (currentRun && isTerminal(currentRun.status)) {
      if (
        data.eventType === 'run_cancelled' &&
        currentRun.status === 'cancelled'
      ) {
        const event = mkEvent(data, effectiveRunId, eventId, now, effectiveSpecVersion);
        await appendEvent(event);
        return { event: stripEventDataRefs(event, resolveData), run: currentRun };
      }
      if (data.eventType === 'run_started') {
        throw new RunExpiredError(
          `Workflow run "${effectiveRunId}" is already in terminal state "${currentRun.status}"`
        );
      }
      if (
        ['run_started', 'run_completed', 'run_failed', 'run_cancelled'].includes(
          data.eventType
        )
      ) {
        throw new EntityConflictError(
          `Cannot transition run from terminal state "${currentRun.status}"`
        );
      }
      if (
        ['step_created', 'hook_created', 'wait_created'].includes(
          data.eventType
        )
      ) {
        throw new EntityConflictError(
          `Cannot create new entities on run in terminal state "${currentRun.status}"`
        );
      }
    }

    // Step-event validation
    let validatedStep: Step | null = null;
    if (
      ['step_started', 'step_completed', 'step_failed', 'step_retrying'].includes(
        data.eventType
      ) &&
      data.correlationId
    ) {
      const hit = await findRow(
        'steps',
        'stepId',
        data.correlationId,
        stepFromRow
      );
      validatedStep = hit?.record ?? null;
      if (!validatedStep) {
        throw new WorkflowWorldError(`Step "${data.correlationId}" not found`);
      }
      if (isTerminal(validatedStep.status)) {
        throw new EntityConflictError(
          `Cannot modify step in terminal state "${validatedStep.status}"`
        );
      }
    }

    // Hook ordering
    if (
      (data.eventType === 'hook_disposed' ||
        data.eventType === 'hook_received') &&
      data.correlationId
    ) {
      const hit = await findRow(
        'hooks',
        'hookId',
        data.correlationId,
        hookFromRow
      );
      if (!hit) throw new HookNotFoundError(data.correlationId);
    }

    const event = mkEvent(data, effectiveRunId, eventId, now, effectiveSpecVersion);
    if (data.eventType === 'run_started' && 'eventData' in event) {
      delete (event as any).eventData;
    }

    let run: WorkflowRun | undefined;
    let step: Step | undefined;
    let hook: Hook | undefined;
    let wait: Wait | undefined;

    switch (data.eventType) {
      case 'run_created': {
        const rd = data.eventData;
        if (await findRow('runs', 'runId', effectiveRunId, runFromRow)) {
          throw new EntityConflictError(
            `Workflow run "${effectiveRunId}" already exists`
          );
        }
        run = {
          runId: effectiveRunId,
          deploymentId: rd.deploymentId,
          status: 'pending',
          workflowName: rd.workflowName,
          specVersion: effectiveSpecVersion,
          executionContext: rd.executionContext,
          input: rd.input,
          output: undefined,
          error: undefined,
          startedAt: undefined,
          completedAt: undefined,
          attributes: {},
          createdAt: now,
          updatedAt: now,
        };
        await writeRun(run);
        break;
      }
      case 'run_started': {
        if (currentRun) {
          if (currentRun.status === 'running') return { run: currentRun };
          run = {
            ...currentRun,
            status: 'running',
            startedAt: currentRun.startedAt ?? now,
            completedAt: undefined,
            error: undefined,
            output: undefined,
            updatedAt: now,
          };
          await writeRun(run);
        }
        break;
      }
      case 'run_completed':
      case 'run_failed':
      case 'run_cancelled': {
        if (currentRun) {
          const next: any = {
            ...currentRun,
            status:
              data.eventType === 'run_completed'
                ? 'completed'
                : data.eventType === 'run_failed'
                  ? 'failed'
                  : 'cancelled',
            completedAt: now,
            updatedAt: now,
          };
          if (data.eventType === 'run_completed') {
            next.output = data.eventData?.output;
            next.error = undefined;
          } else if (data.eventType === 'run_failed') {
            next.error = data.eventData.error;
            next.errorCode = data.eventData.errorCode;
            next.output = undefined;
          } else {
            next.output = undefined;
            next.error = undefined;
          }
          run = next as WorkflowRun;
          await writeRun(run);
          // Best-effort terminal cleanup (delete hooks/waits for this run).
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'step_created': {
        if (
          await findRow('steps', 'stepId', data.correlationId, stepFromRow)
        ) {
          throw new EntityConflictError(
            `Step "${data.correlationId}" already created`
          );
        }
        step = {
          runId: effectiveRunId,
          stepId: data.correlationId,
          stepName: data.eventData.stepName,
          status: 'pending',
          input: data.eventData.input,
          output: undefined,
          error: undefined,
          attempt: 0,
          startedAt: undefined,
          completedAt: undefined,
          createdAt: now,
          updatedAt: now,
          specVersion: effectiveSpecVersion,
        };
        await writeStep(step);
        break;
      }
      case 'step_started': {
        if (validatedStep) {
          step = {
            ...validatedStep,
            status: 'running',
            attempt: validatedStep.attempt + 1,
            startedAt: validatedStep.startedAt ?? now,
            retryAfter: undefined,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'step_completed': {
        validatedStep ??=
          (await findRow('steps', 'stepId', data.correlationId, stepFromRow))
            ?.record ?? null;
        if (validatedStep) {
          if (isTerminal(validatedStep.status)) {
            throw new EntityConflictError('Cannot modify step in terminal state');
          }
          step = {
            ...validatedStep,
            status: 'completed',
            output: data.eventData.result,
            completedAt: now,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'step_failed': {
        validatedStep ??=
          (await findRow('steps', 'stepId', data.correlationId, stepFromRow))
            ?.record ?? null;
        if (validatedStep) {
          if (isTerminal(validatedStep.status)) {
            throw new EntityConflictError('Cannot modify step in terminal state');
          }
          step = {
            ...validatedStep,
            status: 'failed',
            error: data.eventData.error,
            completedAt: now,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'step_retrying': {
        validatedStep ??=
          (await findRow('steps', 'stepId', data.correlationId, stepFromRow))
            ?.record ?? null;
        if (validatedStep) {
          step = {
            ...validatedStep,
            status: 'pending',
            error: data.eventData.error,
            retryAfter: data.eventData.retryAfter,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'hook_created': {
        const hd = data.eventData;
        const tokenHash = hashToken(hd.token);
        // NX-claim via hook-tokens tab.
        const existing = await findRow(
          'hook-tokens',
          'tokenHash',
          tokenHash,
          (r) => r
        );
        if (existing) {
          const conflictingRunId = existing.record.runId;
          const conflictEvent: Event = {
            eventType: 'hook_conflict',
            correlationId: data.correlationId,
            runId: effectiveRunId,
            eventId,
            createdAt: now,
            specVersion: effectiveSpecVersion,
            eventData: {
              token: hd.token,
              ...(conflictingRunId ? { conflictingRunId } : {}),
            },
          } as Event;
          await appendEvent(conflictEvent);
          return {
            event: stripEventDataRefs(conflictEvent, resolveData),
            hook: undefined,
          };
        }
        await sheets.appendRows('hook-tokens', [
          [tokenHash, effectiveRunId, data.correlationId, hd.token],
        ]);
        hook = {
          hookId: data.correlationId,
          runId: effectiveRunId,
          token: hd.token,
          metadata: hd.metadata,
          ownerId: owner.ownerId,
          projectId: owner.projectId,
          environment: owner.environment,
          createdAt: now,
          specVersion: effectiveSpecVersion,
          isWebhook: hd.isWebhook ?? false,
          isSystem: hd.isSystem ?? false,
        };
        const rec: Record<string, string> = {
          hookId: hook.hookId,
          runId: hook.runId,
          token: hook.token,
          tokenHash,
          ownerId: hook.ownerId,
          projectId: hook.projectId,
          environment: hook.environment,
          isWebhook: String(hook.isWebhook),
          isSystem: String(hook.isSystem),
          specVersion: String(hook.specVersion ?? ''),
          createdAtIso: isoOrEmpty(hook.createdAt),
          metadataB64: hook.metadata ? encodeBlob(hook.metadata) : '',
        };
        await sheets.appendRows(
          'hooks',
          [TAB_COLUMNS.hooks.map((c) => rec[c] ?? '')]
        );
        break;
      }
      case 'hook_disposed': {
        const hit = await findRow(
          'hooks',
          'hookId',
          data.correlationId,
          hookFromRow
        );
        if (hit) {
          await sheets.deleteRow('hooks', hit.rowNumber);
          const tHit = await findRow(
            'hook-tokens',
            'hookId',
            data.correlationId,
            (r) => r
          );
          if (tHit) await sheets.deleteRow('hook-tokens', tHit.rowNumber);
        }
        break;
      }
      case 'wait_created': {
        if (
          await findRow('waits', 'correlationId', data.correlationId, waitFromRow)
        ) {
          throw new EntityConflictError(
            `Wait "${data.correlationId}" already exists`
          );
        }
        wait = {
          waitId: `${effectiveRunId}-${data.correlationId}`,
          runId: effectiveRunId,
          status: 'waiting',
          resumeAt: data.eventData.resumeAt,
          completedAt: undefined,
          createdAt: now,
          updatedAt: now,
          specVersion: effectiveSpecVersion,
        };
        const rec: Record<string, string> = {
          waitId: wait.waitId,
          runId: wait.runId,
          correlationId: data.correlationId,
          status: wait.status,
          specVersion: String(wait.specVersion ?? ''),
          createdAtIso: isoOrEmpty(wait.createdAt),
          resumeAtIso: isoOrEmpty(wait.resumeAt),
          completedAtIso: '',
        };
        await sheets.appendRows(
          'waits',
          [TAB_COLUMNS.waits.map((c) => rec[c] ?? '')]
        );
        break;
      }
      case 'wait_completed': {
        const hit = await findRow(
          'waits',
          'correlationId',
          data.correlationId,
          waitFromRow
        );
        if (!hit) {
          throw new WorkflowWorldError(`Wait "${data.correlationId}" not found`);
        }
        if (hit.record.status === 'completed') {
          throw new EntityConflictError(
            `Wait "${data.correlationId}" already completed`
          );
        }
        wait = { ...hit.record, status: 'completed', completedAt: now, updatedAt: now };
        const rec: Record<string, string> = {
          waitId: wait.waitId,
          runId: wait.runId,
          correlationId: data.correlationId,
          status: wait.status,
          specVersion: String(wait.specVersion ?? ''),
          createdAtIso: isoOrEmpty(wait.createdAt),
          resumeAtIso: isoOrEmpty(wait.resumeAt),
          completedAtIso: isoOrEmpty(wait.completedAt),
        };
        await sheets.updateRow(
          'waits',
          hit.rowNumber,
          TAB_COLUMNS.waits.map((c) => rec[c] ?? '')
        );
        break;
      }
      default:
        break;
    }

    await appendEvent(event);
    return {
      event: stripEventDataRefs(event, resolveData),
      run,
      step,
      hook,
      wait,
    };
  }

  async function terminalCleanup(runId: string): Promise<void> {
    // Delete waits + hooks for the run. Sheets has no batch-delete-by-query;
    // walk both tabs once and delete from the bottom up (so row numbers stay
    // stable for the remaining deletes).
    const hookHits = await findAllRows(
      'hooks',
      (r) => r.runId === runId,
      hookFromRow
    );
    for (const h of hookHits.sort((a, b) => b.rowNumber - a.rowNumber)) {
      await sheets.deleteRow('hooks', h.rowNumber);
      const tHit = await findRow(
        'hook-tokens',
        'hookId',
        h.record.hookId,
        (r) => r
      );
      if (tHit) await sheets.deleteRow('hook-tokens', tHit.rowNumber);
    }
    const waitHits = await findAllRows(
      'waits',
      (r) => r.runId === runId,
      waitFromRow
    );
    for (const w of waitHits.sort((a, b) => b.rowNumber - a.rowNumber)) {
      await sheets.deleteRow('waits', w.rowNumber);
    }
  }

  // ----- public Storage interface -----

  const storage: Storage & {
    raw: {
      runsForStatus: (s: WorkflowRun['status']) => Promise<WorkflowRun[]>;
    };
  } = {
    raw: {
      async runsForStatus(status) {
        const hits = await findAllRows(
          'runs',
          (r) => r.status === status,
          runFromRow
        );
        return hits.map((h) => h.record);
      },
    },
    runs: {
      get: (async (
        id: string,
        params?: { resolveData?: 'none' | 'all' }
      ) => {
        assertSafeEntityId('runId', id);
        const hit = await findRow('runs', 'runId', id, runFromRow);
        if (!hit) throw new WorkflowRunNotFoundError(id);
        return params?.resolveData === 'none'
          ? stripRunData(hit.record)
          : hit.record;
      }) as Storage['runs']['get'],
      list: (async (params?: {
        status?: WorkflowRun['status'];
        pagination?: PaginationOptions;
        resolveData?: 'none' | 'all';
      }) => {
        const hits = await findAllRows(
          'runs',
          (r) => (params?.status ? r.status === params.status : true),
          runFromRow
        );
        // Sort by createdAt asc — the natural append order in the sheet.
        const sorted = hits.map((h) => h.record);
        const page = paginateBy(sorted, params?.pagination);
        const data =
          params?.resolveData === 'none' ? page.data.map(stripRunData) : page.data;
        return { data, cursor: page.cursor, hasMore: page.hasMore };
      }) as Storage['runs']['list'],
    },
    steps: {
      get: (async (
        runId: string,
        stepId: string,
        params?: { resolveData?: 'none' | 'all' }
      ) => {
        assertSafeEntityId('runId', runId);
        assertSafeEntityId('stepId', stepId);
        const hit = await findRow('steps', 'stepId', stepId, stepFromRow);
        if (!hit) {
          throw new WorkflowWorldError(`Step ${stepId} in run ${runId} not found`);
        }
        const s = hit.record;
        if (params?.resolveData === 'none') {
          return { ...s, input: undefined, output: undefined } as Step;
        }
        return s;
      }) as Storage['steps']['get'],
      list: (async (params: {
        runId: string;
        pagination?: PaginationOptions;
        resolveData?: 'none' | 'all';
      }) => {
        assertSafeEntityId('runId', params.runId);
        const hits = await findAllRows(
          'steps',
          (r) => r.runId === params.runId,
          stepFromRow
        );
        const page = paginateBy(hits.map((h) => h.record), params.pagination);
        const data =
          params.resolveData === 'none'
            ? page.data.map((s) => ({ ...s, input: undefined, output: undefined }))
            : page.data;
        return { data: data as any, cursor: page.cursor, hasMore: page.hasMore };
      }) as Storage['steps']['list'],
    },
    events: {
      create: createEvent as Storage['events']['create'],
      async get(runId, eventId, params) {
        assertSafeEntityId('runId', runId);
        assertSafeEntityId('eventId', eventId);
        const hit = await findRow('events', 'eventId', eventId, eventFromRow);
        if (!hit || hit.record.runId !== runId) {
          throw new WorkflowWorldError(`Event ${eventId} in run ${runId} not found`);
        }
        return stripEventDataRefs(hit.record, params?.resolveData ?? 'all');
      },
      async list(params) {
        assertSafeEntityId('runId', params.runId);
        const hits = await findAllRows(
          'events',
          (r) => r.runId === params.runId,
          eventFromRow
        );
        const items = hits.map((h) => h.record);
        const page = paginateBy(items, params.pagination, 'asc');
        const resolveData = params.resolveData ?? 'all';
        const data =
          resolveData === 'none'
            ? page.data.map((e) => stripEventDataRefs(e, resolveData))
            : page.data;
        // Events tab returns a resumable tail cursor (matches world-redirect).
        return {
          data,
          cursor: items.length === 0 ? null : page.cursor ?? Buffer.from(String(items.length)).toString('base64url'),
          hasMore: page.hasMore,
        };
      },
      async listByCorrelationId(params) {
        assertSafeEntityId('correlationId', params.correlationId);
        const hits = await findAllRows(
          'events',
          (r) => r.correlationId === params.correlationId,
          eventFromRow
        );
        const items = hits.map((h) => h.record);
        const page = paginateBy(items, params.pagination, 'asc');
        const resolveData = params.resolveData ?? 'all';
        const data =
          resolveData === 'none'
            ? page.data.map((e) => stripEventDataRefs(e, resolveData))
            : page.data;
        return {
          data,
          cursor: items.length === 0 ? null : page.cursor ?? Buffer.from(String(items.length)).toString('base64url'),
          hasMore: page.hasMore,
        };
      },
    },
    hooks: {
      async get(hookId) {
        assertSafeEntityId('hookId', hookId);
        const hit = await findRow('hooks', 'hookId', hookId, hookFromRow);
        if (!hit) throw new HookNotFoundError(hookId);
        return hit.record;
      },
      async getByToken(token) {
        const tHit = await findRow(
          'hook-tokens',
          'tokenHash',
          hashToken(token),
          (r) => r
        );
        if (!tHit) throw new HookNotFoundError(token);
        const hHit = await findRow(
          'hooks',
          'hookId',
          tHit.record.hookId,
          hookFromRow
        );
        if (!hHit) throw new HookNotFoundError(token);
        return hHit.record;
      },
      async list(params) {
        const hits = await findAllRows(
          'hooks',
          (r) => (params?.runId ? r.runId === params.runId : true),
          hookFromRow
        );
        const page = paginateBy(hits.map((h) => h.record), params?.pagination);
        return page;
      },
    },
  };
  return storage;
}

function mkEvent(
  data: any,
  runId: string,
  eventId: string,
  createdAt: Date,
  specVersion: number
): Event {
  return { ...data, runId, eventId, createdAt, specVersion } as Event;
}
