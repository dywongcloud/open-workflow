import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  RunNotSupportedError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  Event,
  EventResult,
  Hook,
  SerializedData,
  Step,
  Storage,
  Wait,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import {
  isLegacySpecVersion,
  requiresNewerWorld,
  SPEC_VERSION_SUPPORTS_EVENT_SOURCING,
  stripEventDataRefs,
  validateUlidTimestamp,
} from '@workflow/world';
import type { RedisClient } from './client/types.js';
import { EntityStore } from './entities.js';
import { assertSafeEntityId, hashToken, newEventId, newRunId } from './ids.js';
import type { Keys } from './keys.js';
import type { RunLocks } from './lock.js';

const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
const isTerminal = (status: string) => TERMINAL.has(status);

export interface HookOwnerConfig {
  ownerId: string;
  projectId: string;
  environment: string;
}

function stripRunData(run: WorkflowRun): WorkflowRunWithoutData {
  return { ...run, input: undefined, output: undefined };
}

function stripStepData(step: Step): Step {
  return { ...step, input: undefined, output: undefined };
}

export function createStorage(
  redis: RedisClient,
  keys: Keys,
  store: EntityStore,
  locks: RunLocks,
  owner: HookOwnerConfig
): Storage {
  async function createEvent(
    runId: string | null,
    data: any,
    params?: { resolveData?: 'none' | 'all' }
  ): Promise<EventResult> {
    // Path-safety: validate ids before they become Redis key components.
    if (runId != null && runId !== '') assertSafeEntityId('runId', runId);
    if (typeof data.correlationId === 'string') {
      assertSafeEntityId('correlationId', data.correlationId);
    }

    // Resolve / generate the effective run id up front so we can lock on it.
    let effectiveRunId: string;
    if (data.eventType === 'run_created' && (!runId || runId === '')) {
      effectiveRunId = newRunId();
    } else if (!runId) {
      throw new Error('runId is required for non-run_created events');
    } else {
      effectiveRunId = runId;
    }

    if (data.eventType === 'run_created' && runId && runId !== '') {
      const validationError = validateUlidTimestamp(effectiveRunId, 'wrun_');
      if (validationError) throw new WorkflowWorldError(validationError);
    }

    return locks.withLock(effectiveRunId, () =>
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

    // Load current run for validation (except run_created and the two
    // runtime events that only touch running steps).
    let currentRun: WorkflowRun | null = null;
    const skipRunValidation =
      data.eventType === 'step_completed' ||
      data.eventType === 'step_retrying';
    if (data.eventType !== 'run_created' && !skipRunValidation) {
      currentRun = await store.getRun(effectiveRunId);

      // Resilient start: run_started on a non-existent run can bootstrap it.
      if (
        data.eventType === 'run_started' &&
        !currentRun &&
        data.eventData?.deploymentId &&
        data.eventData?.workflowName &&
        data.eventData?.input !== undefined
      ) {
        const created: WorkflowRun = {
          runId: effectiveRunId,
          deploymentId: data.eventData.deploymentId,
          status: 'pending',
          workflowName: data.eventData.workflowName,
          specVersion: effectiveSpecVersion,
          executionContext: data.eventData.executionContext,
          input: data.eventData.input,
          output: undefined,
          error: undefined,
          startedAt: undefined,
          completedAt: undefined,
          attributes: {},
          createdAt: now,
          updatedAt: now,
        };
        await store.writeRun(created);
        const runCreatedEvent: Event = {
          eventType: 'run_created',
          runId: effectiveRunId,
          eventId: newEventId(),
          createdAt: now,
          specVersion: effectiveSpecVersion,
          eventData: {
            deploymentId: data.eventData.deploymentId,
            workflowName: data.eventData.workflowName,
            input: data.eventData.input,
            executionContext: data.eventData.executionContext,
          },
        } as Event;
        await store.appendEvent(runCreatedEvent);
        currentRun = created;
      }
    }

    if (data.eventType === 'run_failed' && !currentRun) {
      throw new WorkflowRunNotFoundError(effectiveRunId);
    }

    // Spec-version compatibility.
    if (currentRun) {
      if (requiresNewerWorld(currentRun.specVersion)) {
        throw new RunNotSupportedError(
          currentRun.specVersion!,
          SPEC_VERSION_SUPPORTS_EVENT_SOURCING
        );
      }
      if (isLegacySpecVersion(currentRun.specVersion)) {
        throw new WorkflowWorldError(
          `world-redirect does not support legacy (specVersion 1) run "${effectiveRunId}"`
        );
      }
    }

    // -------- run terminal-state validation --------
    if (currentRun && isTerminal(currentRun.status)) {
      if (
        data.eventType === 'run_cancelled' &&
        currentRun.status === 'cancelled'
      ) {
        const event = mkEvent(data, effectiveRunId, eventId, now, effectiveSpecVersion);
        await store.appendEvent(event);
        return {
          event: stripEventDataRefs(event, resolveData),
          run: currentRun,
        };
      }
      if (data.eventType === 'run_started') {
        throw new RunExpiredError(
          `Workflow run "${effectiveRunId}" is already in terminal state "${currentRun.status}"`
        );
      }
      if (
        data.eventType === 'run_started' ||
        data.eventType === 'run_completed' ||
        data.eventType === 'run_failed' ||
        data.eventType === 'run_cancelled'
      ) {
        throw new EntityConflictError(
          `Cannot transition run from terminal state "${currentRun.status}"`
        );
      }
      if (
        data.eventType === 'step_created' ||
        data.eventType === 'hook_created' ||
        data.eventType === 'wait_created'
      ) {
        throw new EntityConflictError(
          `Cannot create new entities on run in terminal state "${currentRun.status}"`
        );
      }
    }

    // -------- step ordering / terminal validation --------
    let validatedStep: Step | null = null;
    const stepEvents = new Set([
      'step_started',
      'step_completed',
      'step_failed',
      'step_retrying',
    ]);
    if (stepEvents.has(data.eventType) && data.correlationId) {
      validatedStep = await store.getStep(effectiveRunId, data.correlationId);
      if (!validatedStep) {
        throw new WorkflowWorldError(`Step "${data.correlationId}" not found`);
      }
      if (isTerminal(validatedStep.status)) {
        throw new EntityConflictError(
          `Cannot modify step in terminal state "${validatedStep.status}"`
        );
      }
      if (currentRun && isTerminal(currentRun.status)) {
        if (validatedStep.status !== 'running') {
          throw new RunExpiredError(
            `Cannot modify non-running step on run in terminal state "${currentRun.status}"`
          );
        }
      }
    }

    // -------- hook ordering validation --------
    if (
      (data.eventType === 'hook_disposed' ||
        data.eventType === 'hook_received') &&
      data.correlationId
    ) {
      const existingHook = await store.getHook(data.correlationId);
      if (!existingHook) throw new HookNotFoundError(data.correlationId);
    }

    const event = mkEvent(data, effectiveRunId, eventId, now, effectiveSpecVersion);
    // run_started eventData belongs on run_created only.
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
        if (await store.getRun(effectiveRunId)) {
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
        await store.writeRun(run);
        break;
      }
      case 'run_started': {
        if (currentRun) {
          if (currentRun.status === 'running') return { run: currentRun };
          run = {
            ...currentRun,
            status: 'running',
            startedAt: currentRun.startedAt ?? now,
            output: undefined,
            error: undefined,
            completedAt: undefined,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
        }
        break;
      }
      case 'run_completed': {
        if (currentRun) {
          run = {
            ...currentRun,
            status: 'completed',
            output: data.eventData?.output,
            error: undefined,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'run_failed': {
        if (currentRun) {
          run = {
            ...currentRun,
            status: 'failed',
            output: undefined,
            error: data.eventData.error as Uint8Array,
            errorCode: data.eventData.errorCode,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'run_cancelled': {
        if (currentRun) {
          run = {
            ...currentRun,
            status: 'cancelled',
            output: undefined,
            error: undefined,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'step_created': {
        // Dedup: serialized by the per-run lock; a duplicate create on an
        // existing step is a conflict (matches snapshot-replay determinism).
        if (await store.getStep(effectiveRunId, data.correlationId)) {
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
        await store.writeStep(step);
        break;
      }
      case 'step_started': {
        if (validatedStep) {
          if (
            validatedStep.retryAfter &&
            validatedStep.retryAfter.getTime() > Date.now()
          ) {
            throw new TooEarlyError(
              `Cannot start step "${data.correlationId}": retryAfter timestamp has not been reached yet`,
              {
                retryAfter: Math.ceil(
                  (validatedStep.retryAfter.getTime() - Date.now()) / 1000
                ),
              }
            );
          }
          step = {
            ...validatedStep,
            status: 'running',
            startedAt: validatedStep.startedAt ?? now,
            attempt: validatedStep.attempt + 1,
            retryAfter: undefined,
            updatedAt: now,
          };
          await store.writeStep(step);
        }
        break;
      }
      case 'step_completed': {
        validatedStep ??= await store.getStep(
          effectiveRunId,
          data.correlationId
        );
        if (validatedStep) {
          if (isTerminal(validatedStep.status)) {
            throw new EntityConflictError(
              'Cannot modify step in terminal state'
            );
          }
          step = {
            ...validatedStep,
            status: 'completed',
            output: data.eventData.result,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeStep(step);
        }
        break;
      }
      case 'step_failed': {
        validatedStep ??= await store.getStep(
          effectiveRunId,
          data.correlationId
        );
        if (validatedStep) {
          if (isTerminal(validatedStep.status)) {
            throw new EntityConflictError(
              'Cannot modify step in terminal state'
            );
          }
          step = {
            ...validatedStep,
            status: 'failed',
            error: data.eventData.error as Uint8Array,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeStep(step);
        }
        break;
      }
      case 'step_retrying': {
        validatedStep ??= await store.getStep(
          effectiveRunId,
          data.correlationId
        );
        if (validatedStep) {
          step = {
            ...validatedStep,
            status: 'pending',
            error: data.eventData.error as Uint8Array,
            retryAfter: data.eventData.retryAfter,
            updatedAt: now,
          };
          await store.writeStep(step);
        }
        break;
      }
      case 'hook_created': {
        const hd = data.eventData;
        const tokenHash = hashToken(hd.token);
        const claimed = await redis.set(
          keys.hookToken(tokenHash),
          `${effectiveRunId}|${data.correlationId}`,
          { nx: true }
        );
        if (!claimed) {
          const existing = await redis.get(keys.hookToken(tokenHash));
          const conflictingRunId = existing?.split('|')[0];
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
          await store.appendEvent(conflictEvent);
          return {
            event: stripEventDataRefs(conflictEvent, resolveData),
            run,
            step,
            hook: undefined,
          };
        }
        hook = {
          runId: effectiveRunId,
          hookId: data.correlationId,
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
        await store.writeHook(hook);
        break;
      }
      case 'hook_disposed': {
        const existing = await store.getHook(data.correlationId);
        if (existing) {
          await store.deleteHook(existing, hashToken(existing.token));
        }
        break;
      }
      case 'wait_created': {
        if (await store.getWait(effectiveRunId, data.correlationId)) {
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
        await store.writeWait(wait, data.correlationId);
        break;
      }
      case 'wait_completed': {
        const existing = await store.getWait(effectiveRunId, data.correlationId);
        if (!existing) {
          throw new WorkflowWorldError(`Wait "${data.correlationId}" not found`);
        }
        if (existing.status === 'completed') {
          throw new EntityConflictError(
            `Wait "${data.correlationId}" already completed`
          );
        }
        wait = {
          ...existing,
          status: 'completed',
          completedAt: now,
          updatedAt: now,
        };
        await store.writeWait(wait, data.correlationId);
        break;
      }
      // hook_received / hook_conflict: stored in the log, no entity mutation.
      default:
        break;
    }

    await store.appendEvent(event);

    // Preload events on run_started so the runtime can skip events.list.
    let events: Event[] | undefined;
    let cursor: string | null | undefined;
    let hasMore: boolean | undefined;
    if (data.eventType === 'run_started' && run) {
      const page = await store.listEvents({
        runId: effectiveRunId,
        pagination: { sortOrder: 'asc', limit: 1000 },
      });
      events = page.data;
      cursor = page.cursor;
      hasMore = page.hasMore;
    }

    return {
      event: stripEventDataRefs(event, resolveData),
      run,
      step,
      hook,
      wait,
      events,
      cursor,
      hasMore,
    };
  }

  async function terminalCleanup(runId: string): Promise<void> {
    await Promise.all([
      store.deleteAllHooksForRun(runId),
      store.deleteAllWaitsForRun(runId),
    ]);
  }

  return {
    runs: {
      get: (async (id: string, params?: { resolveData?: 'none' | 'all' }) => {
        assertSafeEntityId('runId', id);
        const run = await store.getRun(id);
        if (!run) throw new WorkflowRunNotFoundError(id);
        return params?.resolveData === 'none' ? stripRunData(run) : run;
      }) as Storage['runs']['get'],
      list: (async (params?: {
        status?: any;
        pagination?: any;
        resolveData?: 'none' | 'all';
      }) => {
        const page = await store.listRuns({
          status: params?.status,
          pagination: params?.pagination,
        });
        const data =
          params?.resolveData === 'none'
            ? page.data.map(stripRunData)
            : page.data;
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
        const step = await store.getStep(runId, stepId);
        if (!step) {
          throw new WorkflowWorldError(`Step ${stepId} in run ${runId} not found`);
        }
        return params?.resolveData === 'none' ? stripStepData(step) : step;
      }) as Storage['steps']['get'],
      list: (async (params: {
        runId: string;
        pagination?: any;
        resolveData?: 'none' | 'all';
      }) => {
        assertSafeEntityId('runId', params.runId);
        const page = await store.listSteps({
          runId: params.runId,
          pagination: params.pagination,
        });
        const data =
          params.resolveData === 'none'
            ? page.data.map(stripStepData)
            : page.data;
        return { data, cursor: page.cursor, hasMore: page.hasMore };
      }) as Storage['steps']['list'],
    },
    events: {
      create: createEvent as Storage['events']['create'],
      async get(runId, eventId, params) {
        assertSafeEntityId('runId', runId);
        assertSafeEntityId('eventId', eventId);
        const event = await store.getEvent(runId, eventId);
        if (!event) {
          throw new WorkflowWorldError(
            `Event ${eventId} in run ${runId} not found`
          );
        }
        return stripEventDataRefs(event, params?.resolveData ?? 'all');
      },
      async list(params) {
        assertSafeEntityId('runId', params.runId);
        const page = await store.listEvents({
          runId: params.runId,
          pagination: params.pagination,
        });
        const resolveData = params.resolveData ?? 'all';
        return {
          data:
            resolveData === 'none'
              ? page.data.map((e) => stripEventDataRefs(e, resolveData))
              : page.data,
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
      async listByCorrelationId(params) {
        assertSafeEntityId('correlationId', params.correlationId);
        const page = await store.listEventsByCorrelationId({
          correlationId: params.correlationId,
          pagination: params.pagination,
        });
        const resolveData = params.resolveData ?? 'all';
        return {
          data:
            resolveData === 'none'
              ? page.data.map((e) => stripEventDataRefs(e, resolveData))
              : page.data,
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
    },
    hooks: {
      async get(hookId, _params) {
        assertSafeEntityId('hookId', hookId);
        const hook = await store.getHook(hookId);
        if (!hook) throw new HookNotFoundError(hookId);
        return hook;
      },
      async getByToken(token, _params) {
        const ref = await redis.get(keys.hookToken(hashToken(token)));
        if (!ref) throw new HookNotFoundError(token);
        const hookId = ref.split('|')[1];
        if (!hookId) throw new HookNotFoundError(token);
        const hook = await store.getHook(hookId);
        if (!hook) throw new HookNotFoundError(token);
        return hook;
      },
      async list(params) {
        const page = await store.listHooks({
          runId: params?.runId,
          pagination: params?.pagination,
        });
        return { data: page.data, cursor: page.cursor, hasMore: page.hasMore };
      },
    },
  };
}

function mkEvent(
  data: any,
  runId: string,
  eventId: string,
  createdAt: Date,
  specVersion: number
): Event {
  return {
    ...data,
    runId,
    eventId,
    createdAt,
    specVersion,
  } as Event;
}

export type { SerializedData };
