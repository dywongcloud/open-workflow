import { getStepMetadata, RetryableError } from 'workflow';

// A plain step: full Node.js runtime, automatic retries.
async function greet(name: string): Promise<string> {
  'use step';
  return `Hello, ${name}!`;
}

// A step that fails once to demonstrate durable retries.
async function flaky(value: string): Promise<string> {
  'use step';
  const { attempt } = getStepMetadata();
  if (attempt === 1) {
    throw new RetryableError('transient failure, will retry', {
      retryAfter: '1s',
    });
  }
  return `${value} (succeeded on attempt ${attempt})`;
}

// The orchestrator: deterministic, runs in the workflow sandbox.
export async function hello(name: string) {
  'use workflow';
  const greeting = await greet(name);
  const finalized = await flaky(greeting);
  return { message: finalized, backend: 'open-workflow / redis + 307' };
}
