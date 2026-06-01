# @open-workflow/world-redis

A vendor-agnostic Redis [`World`](https://github.com/vercel/workflow) backend
for the Workflow SDK — a drop-in replacement for `@workflow/world-vercel`.

- **Durable state in Redis**: the append-only event log plus materialized
  run / step / hook / wait entities and stream chunks. Entities are CBOR-encoded
  and base64-stored so the RESP and REST transports behave identically.
- **Execution via 307 redirects**: instead of a queue broker, the flow handler
  continues a run by returning a `307` to itself with the next durable job
  referenced in the query string. A small Redis sorted-set scheduler handles
  delayed work (sleeps, retry backoff).
- **Any Redis**: node-redis (RESP/TCP), Upstash REST, or a zero-setup in-memory
  client for tests and local dev.

## Usage

The Workflow runtime loads this world when `WORKFLOW_TARGET_WORLD` points at it:

```bash
WORKFLOW_TARGET_WORLD=@open-workflow/world-redis
WORKFLOW_REDIS_URL=redis://localhost:6379
```

Or construct one programmatically:

```ts
import { createRedisWorld } from '@open-workflow/world-redis';

const world = createRedisWorld({ redisUrl: 'redis://localhost:6379' });
await world.start();           // launches the 307 dispatch pump + recovers runs
```

Upstash REST:

```ts
createRedisWorld({
  upstashUrl: process.env.WORKFLOW_REDIS_REST_URL,
  upstashToken: process.env.WORKFLOW_REDIS_REST_TOKEN,
});
```

In-memory (dev/tests, non-durable):

```ts
import { MemoryRedisClient } from '@open-workflow/world-redis';
createRedisWorld({ client: new MemoryRedisClient() });
// or: WORKFLOW_REDIS_URL=memory
```

## Configuration

| Option / env | Default | Purpose |
| --- | --- | --- |
| `redisUrl` / `WORKFLOW_REDIS_URL` | — | RESP URL, or `memory` |
| `upstashUrl` / `WORKFLOW_REDIS_REST_URL` | — | Upstash REST endpoint |
| `upstashToken` / `WORKFLOW_REDIS_REST_TOKEN` | — | Upstash REST token |
| `keyPrefix` / `WORKFLOW_REDIS_KEY_PREFIX` | `owf` | Key namespace |
| `baseUrl` / `WORKFLOW_BASE_URL` | `http://localhost:{PORT}` | Dispatcher target |
| `deploymentId` / `WORKFLOW_DEPLOYMENT_ID` | `dpl_redis_local` | `getDeploymentId()` |
| `startDispatcher` | `true` | Run the in-process 307 pump (auto-starts on first use) |
| `dispatcherPollMs` / `WORKFLOW_REDIS_DISPATCHER_POLL_MS` | `50` | Idle poll interval |
| `maxAttempts` / `WORKFLOW_REDIS_MAX_ATTEMPTS` | `25` | Max delivery attempts |
| `retryBaseMs` / `WORKFLOW_REDIS_RETRY_BASE_MS` | `5000` | Backoff base on failure |

Set `WORKFLOW_REDIS_DISABLE_DISPATCHER=1` for read-only consumers (e.g. the
dashboard) that should not drive execution.

## Design notes

- **Atomicity**: `events.create` serializes per-run via an in-process mutex plus
  a Redis lock (TTL + atomic compare-and-delete release), so the
  validate-then-write transitions are consistent even across multiple host
  instances. Cross-run hook-token uniqueness uses `SET NX`.
- **The 307 trampoline** keeps each invocation short and broker-free on the hot
  path; immediate self-continuations are claimed by the redirect chain, while a
  grace window prevents the poller from racing them. The chain is capped well
  below the HTTP redirect limit, after which the poller resumes the run.
- **At-least-once + idempotent**: every continuation is a durable Redis job;
  duplicate delivery is safe because the runtime replays the event log.

## License

Apache-2.0.
