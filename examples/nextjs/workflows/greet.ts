import { sleep } from 'workflow';

async function buildGreeting(name: string): Promise<string> {
  'use step';
  return `Hello, ${name}!`;
}

async function stamp(message: string): Promise<string> {
  'use step';
  return `${message} — served by open-workflow on Redis + 307`;
}

export async function greet(name: string) {
  'use workflow';
  const greeting = await buildGreeting(name);
  await sleep('500ms');
  return { message: await stamp(greeting) };
}
