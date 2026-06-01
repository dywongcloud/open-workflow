import { createWebhook } from 'workflow';

// Suspends until an external HTTP request hits the webhook URL, then resumes.
// Demonstrates durable waits resumed via the public webhook endpoint
// (POST /.well-known/workflow/v1/webhook/{token}).
export async function approval() {
  'use workflow';
  const webhook = createWebhook();
  const request = await webhook;
  const body = await request.text();
  return { approved: true, payload: body };
}
