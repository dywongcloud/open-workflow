/**
 * open-workflow — vendor-agnostic durable workflows.
 *
 * This is a 1:1 re-export of the Workflow SDK developer API (`workflow`).
 * Everything you can import from `workflow` you can import from
 * `open-workflow`: `sleep`, `step` helpers, `getWritable`, error classes,
 * hooks, metadata accessors, etc.
 *
 * The only thing that changes versus the proprietary setup is the backend:
 * set `WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect` and provide a Redis
 * connection. See `open-workflow/redis` for programmatic world construction
 * and `@open-workflow/host` for self-hosting.
 *
 * NOTE: files containing `"use workflow"` / `"use step"` should import the
 * runtime directly from `workflow` so the compiler's workflow-context build
 * is selected. Use `open-workflow` freely everywhere else (route handlers,
 * clients, server actions).
 */
export * from 'workflow';
