// Mirror of the eager-generated flow route at a non-dot-prefix path.
// EdgeOne / OpenNext strip `.well-known/` directories from the deployed
// function bundle, so the original `app/.well-known/workflow/v1/flow/route.js`
// is missing at runtime. Re-exporting from this file causes webpack to inline
// the workflow runtime + step bundles into this chunk, which lives under
// `/api/wf/flow` and survives the deploy.
//
// The `beforeFiles` rewrite installed by `withEdgeOneWorkflow` translates
// `/.well-known/workflow/v1/flow` → `/api/wf/flow` ahead of route matching,
// so the dispatcher's hardcoded URL keeps working.

export { POST } from "../../../.well-known/workflow/v1/flow/route";
