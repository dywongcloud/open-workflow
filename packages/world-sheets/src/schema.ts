/**
 * Schema mapping the Workflow World's entities onto Google Sheets tabs.
 *
 * Each tab is row-per-record with a header row in row 1. Columns are
 * positional — see TAB_COLUMNS for the ordering. Binary payloads
 * (SerializedData / event eventData / job bodies) are stored as base64 of
 * CBOR so a human reading the sheet still sees one cell per field.
 *
 * The schema is deliberately wide (split across tabs by entity type) instead
 * of normalised: people are going to FILTER and SORT in the spreadsheet UI,
 * and one-tab-per-type makes that natural.
 */

export const TABS = {
  runs: 'runs',
  events: 'events',
  steps: 'steps',
  hooks: 'hooks',
  hookTokens: 'hook-tokens',
  waits: 'waits',
  schedule: 'schedule',
  streams: 'streams',
} as const;

export type TabName = (typeof TABS)[keyof typeof TABS];

export const TAB_COLUMNS: Record<TabName, readonly string[]> = {
  runs: [
    'runId',
    'workflowName',
    'status',
    'deploymentId',
    'specVersion',
    'errorCode',
    'createdAtIso',
    'startedAtIso',
    'completedAtIso',
    'expiredAtIso',
    'attributesJson',
    'inputB64',
    'outputB64',
    'errorB64',
    'executionContextJson',
  ],
  events: [
    'eventId',
    'runId',
    'eventType',
    'correlationId',
    'specVersion',
    'createdAtIso',
    'eventDataB64',
  ],
  steps: [
    'stepId',
    'runId',
    'stepName',
    'status',
    'attempt',
    'specVersion',
    'createdAtIso',
    'startedAtIso',
    'completedAtIso',
    'retryAfterIso',
    'inputB64',
    'outputB64',
    'errorB64',
  ],
  hooks: [
    'hookId',
    'runId',
    'token',
    'tokenHash',
    'ownerId',
    'projectId',
    'environment',
    'isWebhook',
    'isSystem',
    'specVersion',
    'createdAtIso',
    'metadataB64',
  ],
  'hook-tokens': ['tokenHash', 'runId', 'hookId', 'token'],
  waits: [
    'waitId',
    'runId',
    'correlationId',
    'status',
    'specVersion',
    'createdAtIso',
    'resumeAtIso',
    'completedAtIso',
  ],
  schedule: [
    'messageId',
    'queueName',
    'route',
    'runId',
    'attempt',
    'runAtMs',
    'claimedAt',
    'bodyB64',
  ],
  streams: [
    'runId',
    'name',
    'chunkIdx',
    'done',
    'createdAtIso',
    'dataB64',
  ],
};

export function tabName(prefix: string, name: TabName): string {
  return prefix ? `${prefix}-${name}` : name;
}

/** Make a row from a record, preserving column order. Missing values -> ''. */
export function recordToRow(
  tab: TabName,
  record: Record<string, unknown>
): string[] {
  return TAB_COLUMNS[tab].map((col) => {
    const v = record[col];
    if (v == null) return '';
    if (typeof v === 'string') return v;
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    return JSON.stringify(v);
  });
}

/** Parse a row (1D array) back into a record using the tab's column names. */
export function rowToRecord(
  tab: TabName,
  row: string[]
): Record<string, string> {
  const cols = TAB_COLUMNS[tab];
  const out: Record<string, string> = {};
  for (let i = 0; i < cols.length; i++) {
    out[cols[i]!] = row[i] ?? '';
  }
  return out;
}
