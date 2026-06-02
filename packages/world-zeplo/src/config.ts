import type { RedisWorldConfig } from '@open-workflow/world-redirect';

export interface ZeploConfig {
  /**
   * Zeplo API token. Required. Resolved from `ZEPLO_TOKEN` env var if not
   * passed explicitly.
   */
  token?: string;

  /**
   * Zeplo endpoint. Default `https://zeplo.to`. Override if you run a
   * self-hosted Zeplo or use a region-specific endpoint.
   */
  endpoint?: string;

  /**
   * Public URL where this app's `.well-known/workflow/v1/{flow,step,webhook}`
   * routes are reachable from the public internet. Zeplo POSTs back to that
   * URL with each queued message — it must be HTTPS and externally reachable
   * (no localhost). Resolved from `WORKFLOW_BASE_URL` env var if not passed.
   */
  targetBaseUrl?: string;

  /**
   * Shared secret sent as `x-zeplo-secret` on every enqueue and verified by
   * the queue handler before invoking your handler. Resolved from
   * `ZEPLO_WEBHOOK_SECRET` env var if not passed. Optional but strongly
   * recommended — without it any caller that knows your URL can replay
   * messages.
   */
  webhookSecret?: string;

  /**
   * Default Zeplo retry count applied to every enqueue. Default 25 (matches
   * the default `maxAttempts` of the in-process dispatcher in world-redirect
   * so failure semantics are consistent across worlds).
   */
  defaultRetry?: number;
}

export interface ZeploWorldConfig extends RedisWorldConfig {
  zeplo?: ZeploConfig;
}

export interface ResolvedZeploConfig {
  token: string;
  endpoint: string;
  targetBaseUrl: string;
  webhookSecret?: string;
  defaultRetry: number;
}

export function resolveZeploConfig(
  partial: ZeploConfig = {}
): ResolvedZeploConfig {
  const env = process.env;
  const token = partial.token ?? env.ZEPLO_TOKEN;
  if (!token) {
    throw new Error(
      '[world-zeplo] No Zeplo token configured. Set ZEPLO_TOKEN env var or pass { zeplo: { token } } to createWorld().'
    );
  }
  const targetBaseUrl =
    partial.targetBaseUrl ?? env.WORKFLOW_BASE_URL ?? env.APP_BASE_URL;
  if (!targetBaseUrl) {
    throw new Error(
      '[world-zeplo] No targetBaseUrl configured. Set WORKFLOW_BASE_URL env var (the public URL where your flow endpoints live) or pass { zeplo: { targetBaseUrl } } to createWorld(). Zeplo must be able to POST back to it from the public internet.'
    );
  }
  return {
    token,
    endpoint: (partial.endpoint ?? env.ZEPLO_ENDPOINT ?? 'https://zeplo.to').replace(/\/$/, ''),
    targetBaseUrl: targetBaseUrl.replace(/\/$/, ''),
    webhookSecret: partial.webhookSecret ?? env.ZEPLO_WEBHOOK_SECRET,
    defaultRetry: partial.defaultRetry ?? 25,
  };
}
