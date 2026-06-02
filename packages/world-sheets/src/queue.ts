import {
  MessageId,
  type Queue,
  type QueuePayload,
  ValidQueueName,
} from '@workflow/world';
import { encodeBlob, decodeBlob, newMessageId } from './codec.js';
import type { ResolvedSheetsConfig } from './config.js';
import { rowToRecord, TAB_COLUMNS } from './schema.js';
import type { SheetsClient } from './sheets.js';

interface ScheduleRow {
  rowNumber: number;
  messageId: string;
  queueName: string;
  route: 'flow' | 'step';
  runId: string;
  attempt: number;
  runAtMs: number;
  claimedAt: string;
  bodyB64: string;
}

function backoffMs(attempt: number, base: number): number {
  if (attempt <= 6) return base;
  return Math.min(base * 2 ** (attempt - 6), 5 * 60_000);
}

export interface SheetsQueue extends Queue {
  startDispatcher(): void;
  stopDispatcher(): Promise<void>;
  drainOnce(): Promise<number>;
}

export function createSheetsQueue(
  sheets: SheetsClient,
  config: ResolvedSheetsConfig
): SheetsQueue {
  let running = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let inFlight = 0;

  function rowFor(record: Partial<ScheduleRow>): string[] {
    const flat: Record<string, string> = {
      messageId: record.messageId ?? '',
      queueName: record.queueName ?? '',
      route: record.route ?? 'flow',
      runId: record.runId ?? '',
      attempt: String(record.attempt ?? 1),
      runAtMs: String(record.runAtMs ?? Date.now()),
      claimedAt: record.claimedAt ?? '',
      bodyB64: record.bodyB64 ?? '',
    };
    return TAB_COLUMNS.schedule.map((c) => flat[c] ?? '');
  }

  async function readSchedule(): Promise<ScheduleRow[]> {
    const rows = await sheets.getAllRows('schedule');
    return rows.map((row, i) => {
      const r = rowToRecord('schedule', row);
      return {
        rowNumber: i + 2,
        messageId: r.messageId,
        queueName: r.queueName,
        route: (r.route as 'flow' | 'step') ?? 'flow',
        runId: r.runId,
        attempt: Number(r.attempt ?? '1'),
        runAtMs: Number(r.runAtMs ?? '0'),
        claimedAt: r.claimedAt ?? '',
        bodyB64: r.bodyB64 ?? '',
      };
    });
  }

  const queue: Queue['queue'] = async (queueName, message, opts) => {
    const messageId = newMessageId();
    const route = queueName.startsWith('__wkf_step_') ? 'step' : 'flow';
    const runAtMs =
      Date.now() + Math.max(0, (opts?.delaySeconds ?? 0) * 1000);
    const runId = (message as any)?.runId ?? (message as any)?.workflowRunId ?? '';
    await sheets.appendRows('schedule', [
      rowFor({
        messageId,
        queueName,
        route,
        runId,
        attempt: 1,
        runAtMs,
        claimedAt: '',
        bodyB64: encodeBlob(message),
      }),
    ]);
    return { messageId: MessageId.parse(messageId) };
  };

  const createQueueHandler: Queue['createQueueHandler'] = (prefix, handler) => {
    return async (req: Request): Promise<Response> => {
      const url = new URL(req.url);
      const msgId = url.searchParams.get('msg');
      if (!msgId) {
        return Response.json({ error: 'missing ?msg' }, { status: 400 });
      }

      // Look up the job by messageId in the schedule sheet.
      const all = await readSchedule();
      const job = all.find((r) => r.messageId === msgId);
      if (!job) return Response.json({ ok: true }); // idempotent no-op

      if (!job.queueName.startsWith(prefix)) {
        return Response.json({ error: 'Unhandled queue' }, { status: 400 });
      }

      const message = decodeBlob<QueuePayload>(job.bodyB64);
      if (message === undefined) {
        return Response.json({ error: 'empty body' }, { status: 400 });
      }

      let result: void | { timeoutSeconds: number };
      try {
        result = await handler(message, {
          attempt: job.attempt,
          queueName: ValidQueueName.parse(job.queueName),
          messageId: MessageId.parse(msgId),
          requestId: req.headers.get('x-request-id') ?? undefined,
        });
      } catch (err) {
        const nextAttempt = job.attempt + 1;
        if (nextAttempt <= config.maxAttempts) {
          await sheets.updateRow(
            'schedule',
            job.rowNumber,
            rowFor({
              ...job,
              attempt: nextAttempt,
              claimedAt: '',
              runAtMs:
                Date.now() + backoffMs(nextAttempt, config.retryBaseMs),
            })
          );
        } else {
          await sheets.deleteRow('schedule', job.rowNumber);
        }
        return Response.json(String(err), { status: 500 });
      }

      if (result && typeof result.timeoutSeconds === 'number') {
        const sec = Math.max(0, result.timeoutSeconds);
        await sheets.updateRow(
          'schedule',
          job.rowNumber,
          rowFor({
            ...job,
            runAtMs: Date.now() + sec * 1000,
            claimedAt: '',
          })
        );
        return Response.json({ timeoutSeconds: sec });
      }

      // Delivered — remove the row.
      await sheets.deleteRow('schedule', job.rowNumber);
      return Response.json({ ok: true });
    };
  };

  const getDeploymentId: Queue['getDeploymentId'] = async () =>
    config.deploymentId;

  async function dispatchOne(job: ScheduleRow): Promise<void> {
    const target = `${config.baseUrl}/.well-known/workflow/v1/${job.route}?msg=${encodeURIComponent(job.messageId)}`;
    try {
      const res = await fetch(target, {
        method: 'POST',
        redirect: 'follow',
        headers: {
          'content-type': 'application/json',
          'x-vqs-queue-name': job.queueName,
          'x-vqs-message-id': job.messageId,
          'x-vqs-message-attempt': String(job.attempt),
        },
        body: '',
      });
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        console.error(
          `[world-sheets] dispatch ${job.messageId} -> HTTP ${res.status}: ${text.slice(0, 200)}`
        );
      }
    } catch (err) {
      // Network error: clear claim so the next tick can retry.
      console.error(`[world-sheets] dispatch ${job.messageId} network error:`, err);
      try {
        await sheets.updateRow(
          'schedule',
          job.rowNumber,
          rowFor({
            ...job,
            attempt: job.attempt + 1,
            claimedAt: '',
            runAtMs:
              Date.now() + backoffMs(job.attempt + 1, config.retryBaseMs),
          })
        );
      } catch {
        // best effort
      }
    }
  }

  async function drainOnce(): Promise<number> {
    const all = await readSchedule();
    const now = Date.now();
    const due = all.filter((r) => !r.claimedAt && r.runAtMs <= now).slice(0, 8);
    let dispatched = 0;
    for (const job of due) {
      // Claim the row before dispatch so a parallel dispatcher in another
      // host doesn't double-process. Sheets has no atomic compare-and-swap,
      // so this is best-effort — set claimedAt to now and dispatch.
      try {
        await sheets.updateRow(
          'schedule',
          job.rowNumber,
          rowFor({ ...job, claimedAt: String(Date.now()) })
        );
      } catch {
        continue;
      }
      inFlight++;
      dispatched++;
      void dispatchOne(job).finally(() => {
        inFlight--;
      });
    }
    return dispatched;
  }

  function startDispatcher(): void {
    if (running) return;
    running = true;
    const tick = async () => {
      if (!running) return;
      try {
        await drainOnce();
      } catch (err) {
        console.error('[world-sheets] dispatcher tick error:', err);
      }
      if (!running) return;
      timer = setTimeout(tick, config.dispatcherPollMs);
    };
    void tick();
  }

  async function stopDispatcher(): Promise<void> {
    running = false;
    if (timer) clearTimeout(timer);
    const deadline = Date.now() + 5000;
    while (inFlight > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  return {
    queue,
    createQueueHandler,
    getDeploymentId,
    startDispatcher,
    stopDispatcher,
    drainOnce,
  };
}
