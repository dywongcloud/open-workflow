# @open-workflow/world-zeplo

A Workflow World that uses [Zeplo](https://zeplo.io) for queue dispatch
and Redis (via [`@open-workflow/world-redirect`](../world-redirect)) for
storage.

```
   start()  ──>  Zeplo  ──delay/retry──>  POST /.well-known/workflow/v1/flow?msg=…
                                                   │
                                                   ▼
                                            flow handler runs
                                                   │
                                                   ▼
                                       Redis: events / runs / steps
                                                   │
                                                   ▼
                                  re-enqueue?  ──>  Zeplo  ──> next hop
```

## When you want this

You don't want to keep a dispatcher process alive. You're on:

- Vercel functions / Cloudflare Workers / AWS Lambda
- EdgeOne Pages / any OpenNext target
- Any "function-per-invocation" platform

…and you'd rather pay a small managed-queue service to handle scheduling,
retries, and dead-lettering than reproduce that yourself.

If you can run a long-lived Node process, prefer
[`@open-workflow/world-redirect`](../world-redirect) — same DX, no
external queue dependency.

## What changes vs. `world-redirect`

| Concern | `world-redirect` | `world-zeplo` |
| --- | --- | --- |
| Event log + materialized entities | Redis | Redis (unchanged) |
| Stream chunks | Redis + pub/sub | Redis + pub/sub (unchanged) |
| Queue / scheduler | Redis sorted set + 307 trampoline + in-process pump | **Zeplo** (hosted HTTP queue; no pump) |
| Public URL needed at runtime | No (loopback fine) | **Yes** (Zeplo posts back from the internet) |
| Retries | dispatcher backoff, `maxAttempts` env | Zeplo `_retry` query param |
| Delayed delivery (sleep, retry-after) | sorted set with `runAt` | Zeplo `_delay` query param |

The Workflow developer API is identical. Workflows you wrote against
`world-redirect` run unchanged on `world-zeplo`.

## Install

```bash
npm install @open-workflow/world-zeplo @open-workflow/world-redirect workflow
```

You also need:

- A Zeplo account + API token.
- A publicly-reachable HTTPS URL for your app (Zeplo posts back to it).
- Redis credentials for storage — `WORKFLOW_REDIS_REST_URL` /
  `WORKFLOW_REDIS_REST_TOKEN` (Upstash REST), or `WORKFLOW_REDIS_URL`
  (RESP).

## Configuration

Selected by the Workflow runtime when `WORKFLOW_TARGET_WORLD` points at
this package. All config from env, mirroring the other worlds:

| Env var | Purpose |
| --- | --- |
| `WORKFLOW_TARGET_WORLD` | Set to `@open-workflow/world-zeplo` |
| `ZEPLO_TOKEN` | Your Zeplo API token (required) |
| `ZEPLO_ENDPOINT` | Override Zeplo's base URL (default `https://zeplo.to`) |
| `WORKFLOW_BASE_URL` | Public URL where your flow/step/webhook routes live; Zeplo posts here |
| `ZEPLO_WEBHOOK_SECRET` | Shared secret sent with every enqueue and verified on dispatch (recommended) |
| `WORKFLOW_REDIS_REST_URL` / `WORKFLOW_REDIS_REST_TOKEN` | Upstash REST credentials for storage |
| `WORKFLOW_REDIS_URL` | RESP URL (alternative to Upstash) |
| `WORKFLOW_REDIS_KEY_PREFIX` | Key namespace (default `owf`) |
| `WORKFLOW_DEPLOYMENT_ID` | Reported by `getDeploymentId()` (default `dpl_zeplo`) |

Or construct programmatically:

```ts
import { createZeploWorld } from '@open-workflow/world-zeplo';

const world = createZeploWorld({
  // Redis (storage) — anything @open-workflow/world-redirect accepts
  upstashUrl: process.env.UPSTASH_REDIS_REST_URL,
  upstashToken: process.env.UPSTASH_REDIS_REST_TOKEN,

  // Zeplo (queue)
  zeplo: {
    token: process.env.ZEPLO_TOKEN!,
    targetBaseUrl: 'https://your-app.example.com',
    webhookSecret: process.env.ZEPLO_WEBHOOK_SECRET,
    defaultRetry: 25,
  },
});
```

## How dispatch works

Every `world.queue(queueName, message, opts)` call:

1. Generates a `messageId` (ULID).
2. Builds the target URL the runtime expects:
   `{targetBaseUrl}/.well-known/workflow/v1/{flow|step}?msg={messageId}`.
3. POSTs to `{ZEPLO_ENDPOINT}/{target}?_token=…&_delay=…&_retry=…` with
   the workflow payload as the JSON body and `x-vqs-*` plus
   `x-zeplo-secret` headers.
4. Zeplo persists the request and, after any delay, POSTs the body +
   headers back to your target URL.

Your `createQueueHandler(prefix, handler)`:

1. Optionally verifies the `x-zeplo-secret` header.
2. Parses the body, extracts the message metadata.
3. Runs your handler.
4. Maps the result back to HTTP:
   - `void` → `200 {ok:true}`. Delivered.
   - `{ timeoutSeconds: N }` → `200 {timeoutSeconds:N}` and re-enqueues
     a fresh copy through Zeplo with `_delay=N`. (Zeplo doesn't expose
     a "delay this retry" semantic natively, so we re-enqueue
     ourselves; it's the same model `world-redirect` uses.)
   - Thrown error → `500`. Zeplo retries per `_retry`.

There is **no in-process dispatcher**. The world's `start()` is a no-op
by default — there's nothing to pump, and Zeplo runs in their cloud.

## Caveats

- Zeplo must reach your URL from the public internet. Localhost won't
  work. For local development, expose your dev server via a tunnel
  (ngrok / cloudflared / Tailscale Funnel / etc.) and set
  `WORKFLOW_BASE_URL` to the tunnel URL.
- Zeplo is an external dependency; if it's down, no workflow progresses.
  The event log in Redis is the source of truth, so when Zeplo recovers
  the runtime can resume.
- Webhook verification: set `ZEPLO_WEBHOOK_SECRET`. Without it, anyone
  who knows your flow URL can POST a forged message — your handler will
  execute whatever it's given.
- Streams (live `getReadable`) still use Redis pub/sub through
  `world-redirect`. If you run on a platform that can't hold an open
  Redis subscriber connection (e.g. short-lived serverless functions),
  the dashboard's live stream view will fall back to polling.

## License

Apache-2.0.
