// Shim for @workflow/world-vercel. The Workflow Next.js integration emits a
// bundle that imports this package by name even when WORKFLOW_TARGET_WORLD
// points elsewhere. On Vercel the import is satisfied by the real package; on
// EdgeOne / any non-Vercel platform it's missing and the function fails to
// load at module init.
//
// Redirect everything to @open-workflow/world-redirect so the import resolves
// and the world that actually gets used (via getWorld() reading
// WORKFLOW_TARGET_WORLD) is ours.

import {
  createRedisWorld,
  createWorld,
  NodeRedisClient,
  UpstashRedisClient,
} from "@open-workflow/world-redirect";

// Upstream world-vercel exports createVercelWorld(); alias to our createWorld
// so any code path that names it directly still resolves.
export const createVercelWorld = createWorld;

export { createRedisWorld, createWorld, NodeRedisClient, UpstashRedisClient };

export default createWorld;
