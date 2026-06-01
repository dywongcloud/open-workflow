/**
 * Backend construction helpers — re-export of `@open-workflow/world-redis`.
 *
 * Use `createRedisWorld(config)` to build a World programmatically, or rely on
 * `WORKFLOW_TARGET_WORLD=@open-workflow/world-redis` + env vars for the
 * zero-code path.
 */
export * from '@open-workflow/world-redis';
