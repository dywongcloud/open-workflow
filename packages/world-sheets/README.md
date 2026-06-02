# @open-workflow/world-sheets

A Workflow World where **every run, event, step, and queued job is a row
in a Google Sheet**. Storage, queue, and stream chunks all live in tabs
your operations team can read, filter, sort, and comment on directly —
no separate dashboard, no SQL, no `SELECT * FROM workflow_events`.

```
runs       events     steps     hooks     hook-tokens    waits     schedule    streams
─────      ──────     ─────     ─────     ───────────    ─────     ────────    ───────
runId      eventId    stepId    hookId    tokenHash      waitId    messageId   runId
…          runId      runId     runId     runId          runId     runAtMs     name
                                                                   bodyB64     chunkIdx
                                                                               dataB64
```

## When to use this

- **Ops / approvals / admin workflows** where a non-engineer team owns
  the process and lives in spreadsheets.
- **Low-volume orchestration** (a few hundred runs/day, maybe a few
  thousand events). Google Sheets API allows ~60 reads + 60 writes per
  minute per user.
- **Demos / education** — the whole durable runtime is just a sheet
  you can scroll through.

## When NOT to use this

- High-throughput workloads. Use [`world-redirect`](../world-redirect).
- Multi-host concurrency. There are no transactions; the in-process
  mutex doesn't extend across host instances.
- Sub-second dispatch latency. The dispatcher polls the schedule tab
  (default every 2 s) and each operation is a network round-trip to the
  Sheets API.

## Install

```bash
npm install @open-workflow/world-sheets workflow
```

## Setup

### 1. Create a Google Cloud service account

In the Google Cloud console:

1. Create a project (or reuse one).
2. Enable the Google Sheets API.
3. IAM & Admin → Service Accounts → create one (no extra roles needed
   for the project).
4. Open the service account → Keys → Add Key → JSON. Save the file.

### 2. Create the spreadsheet and share it

1. Make a new Google Sheet.
2. Copy the spreadsheet ID out of the URL
   (`https://docs.google.com/spreadsheets/d/<THIS_PART>/edit`).
3. Share the sheet with your service account's email
   (`xxx-xxx@your-project.iam.gserviceaccount.com`) with **Editor**
   permission. This is the most-forgotten step — without it every
   request 403s.

### 3. Set env vars

```
WORKFLOW_TARGET_WORLD       = @open-workflow/world-sheets
GOOGLE_SHEETS_SPREADSHEET_ID = <the spreadsheet id from step 2>
GOOGLE_SERVICE_ACCOUNT_JSON  = <the entire JSON file's contents, or base64 of it>
WORKFLOW_BASE_URL            = https://<where-your-flow-endpoint-lives>
```

`GOOGLE_SERVICE_ACCOUNT_JSON` accepts either the raw JSON or
base64-encoded JSON — base64 is easier in env-var fields that mangle
newlines.

### 4. (Optional) Pin the dispatcher poll interval

Sheets is rate-limited. Defaults to a 2-second poll. Tune via:

```
WORKFLOW_SHEETS_DISPATCHER_POLL_MS = 5000
WORKFLOW_SHEETS_MAX_ATTEMPTS       = 10
WORKFLOW_SHEETS_RETRY_BASE_MS      = 10000
```

Set `WORKFLOW_SHEETS_DISABLE_DISPATCHER=1` on dashboard / read-only
hosts so they don't compete with your real dispatcher for API quota.

## How it works

- `world.queue(queueName, message, opts)` appends one row to the
  `schedule` tab with `runAtMs = now + delay` and the message as
  base64(CBOR).
- The in-process dispatcher polls `schedule`, picks rows where
  `claimedAt` is empty and `runAtMs <= now`, stamps `claimedAt` to mark
  the row taken, then POSTs to `{WORKFLOW_BASE_URL}/.well-known/workflow/v1/{flow|step}?msg=<id>`.
- Your `createQueueHandler` wrapper looks up the row by `?msg=`, runs
  the user handler, and either deletes the row (success), updates
  `runAtMs` (timeoutSeconds re-delivery), or increments `attempt` +
  reschedules with backoff (handler threw).
- `events.create` appends one row to the `events` tab and updates the
  relevant entity tab in the same logical operation (serialised by an
  in-process per-run mutex).
- Streams: `streamer.write` appends a chunk row; `get()` returns a
  ReadableStream that polls the `streams` tab.

## Programmatic use

```ts
import { createSheetsWorld } from '@open-workflow/world-sheets';

const world = createSheetsWorld({
  spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID!,
  serviceAccountKey: process.env.GOOGLE_SERVICE_ACCOUNT_JSON!,
  baseUrl: 'https://my-app.example.com',
  tabPrefix: 'prod', // optional — namespace tabs ("prod-runs", "prod-events", …)
  dispatcherPollMs: 5_000,
  maxAttempts: 10,
});

await world.start();   // bootstrap missing tabs + start dispatcher
```

## Custom auth providers

If you don't want to use a service-account key file (e.g. you're on
Workload Identity Federation, or you have ADC set up), pass your own
access-token provider:

```ts
import { createSheetsWorld } from '@open-workflow/world-sheets';
import { GoogleAuth } from 'google-auth-library';

const auth = new GoogleAuth({
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

createSheetsWorld({
  accessTokenProvider: async () => {
    const client = await auth.getClient();
    const tok = await client.getAccessToken();
    return tok.token!;
  },
});
```

## Caveats and known limits

- **Rate limits.** The Sheets API allows roughly 60 read + 60 write
  requests / minute / user (and 300 / minute / project, shared across
  all users). Each `events.create` is at minimum 2 writes (entity +
  event row) plus 1 read (entity lookup). A workflow with three steps
  produces ~15 events ⇒ ~45 API operations per run. Plan accordingly.
- **No cross-process concurrency.** Multiple hosts running the same
  world will race on terminal-state transitions and may double-claim
  schedule rows (Sheets has no atomic update). Run one dispatcher,
  document the choice, or partition by `tabPrefix`.
- **Row scans on every read.** Entity lookup is O(rows-in-tab). Fine up
  to a few thousand rows per tab; archive old runs periodically by
  filtering and copying to another spreadsheet.
- **Cells max 50,000 chars.** Large workflow inputs/outputs that
  serialise above that limit will error on write. Store the big blob
  elsewhere (S3, GCS) and pass a URL through the workflow.
- **No live pub/sub for streams.** `get()` polls. Acceptable for tail-
  log-style use cases at ~2s update granularity; not for high-rate
  binary streaming.

## License

Apache-2.0.
