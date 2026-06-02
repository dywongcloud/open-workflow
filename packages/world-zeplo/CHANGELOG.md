# @open-workflow/world-zeplo

## 0.1.0 — 2026-06-02

Initial release.

A Workflow World that uses Zeplo's hosted HTTP queue for dispatch and
reuses `@open-workflow/world-redirect`'s Redis storage + streamer. Built
for serverless platforms where you can't keep a long-running dispatcher
alive — `world-redirect`'s 307 trampoline + sorted-set scheduler are
replaced by Zeplo posting back to your public URL with each queued
message.

- `createZeploWorld(config)` — programmatic constructor; accepts the
  same Redis options as `createRedisWorld` plus a `zeplo` block.
- `createWorld()` / default export — singleton entry for the runtime to
  load via `WORKFLOW_TARGET_WORLD=@open-workflow/world-zeplo`.
- `createZeploQueue(config)` — the Queue implementation in isolation,
  in case you want to compose it with a different storage backend.
- `resolveZeploConfig(partial)` — env-aware config resolver
  (`ZEPLO_TOKEN`, `ZEPLO_ENDPOINT`, `WORKFLOW_BASE_URL`,
  `ZEPLO_WEBHOOK_SECRET`).

Storage / streamer behaviour is unchanged from `world-redirect@0.2.0`.
