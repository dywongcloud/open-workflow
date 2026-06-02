// Mirror of the eager-generated webhook route. See ../flow/route.ts for the
// rationale. The `[token]` dynamic segment matches the original webhook URL
// pattern, and the rewrite installed by withEdgeOneWorkflow translates
// `/.well-known/workflow/v1/webhook/:token` → `/api/wf/webhook/:token` ahead
// of route matching.

export {
  GET,
  POST,
  PUT,
  PATCH,
  DELETE,
  HEAD,
  OPTIONS,
} from "../../../../.well-known/workflow/v1/webhook/[token]/route";
