import type { NextConfig } from 'next';
import { withWorkflow } from 'workflow/next';

// Point the Workflow runtime at the vendor-agnostic Redis world BEFORE
// withWorkflow runs (it only defaults to `local` when this is unset).
process.env.WORKFLOW_TARGET_WORLD ||= '@open-workflow/world-redis';
// In dev, the 307 dispatcher posts back to this same server.
process.env.WORKFLOW_BASE_URL ||= `http://localhost:${process.env.PORT ?? 3000}`;

const nextConfig: NextConfig = {
  // world-redis (and its node-redis dep) run on the server only.
  serverExternalPackages: ['@open-workflow/world-redis'],
};

export default withWorkflow(nextConfig);
