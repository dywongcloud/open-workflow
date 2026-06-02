# @open-workflow/world-sheets

## 0.1.0 — 2026-06-02

Initial release. A Workflow World where every entity lives as a row in a
Google Sheet — runs, events, steps, hooks, waits, queued jobs, and
stream chunks all stored in dedicated tabs the operations team can
read, filter, and comment on directly.

Implements the full event-sourced `Storage` contract (runs / events /
steps / hooks lifecycle, hook token NX-claim with `hook_conflict` event
fallback, wait dedup, terminal-state guards, per-run in-process mutex),
plus a `schedule`-tab based queue with an in-process polling dispatcher
that POSTs to `{WORKFLOW_BASE_URL}/.well-known/workflow/v1/...` and a
polling streamer.

Auth: built-in service-account flow (JWT RS256 signed in pure
`node:crypto`, exchanged at `oauth2.googleapis.com/token`, cached
50 min). Custom `accessTokenProvider` accepted for callers that already
use `google-auth-library` / ADC / Workload Identity Federation.

Caveats documented in README: Sheets API ~60 reads + 60 writes per
minute per user, no transactions (single-host deployment only), row
scans on every entity lookup. Built for low-volume ops / approval /
admin workflows, not high-throughput orchestration.

Selected by the runtime via
`WORKFLOW_TARGET_WORLD=@open-workflow/world-sheets`. Config from env:
`GOOGLE_SHEETS_SPREADSHEET_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON`,
`WORKFLOW_BASE_URL`, optional `WORKFLOW_SHEETS_*` tuning knobs.
