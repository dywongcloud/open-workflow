import { sleep } from 'workflow';

async function double(n: number): Promise<number> {
  'use step';
  return n * 2;
}

// Exercises parallel step fan-out (Promise.all) and a durable sleep, both of
// which drive the Redis scheduler + 307 trampoline.
export async function controlFlow() {
  'use workflow';
  const [a, b, c] = await Promise.all([double(1), double(2), double(3)]);
  await sleep('500ms');
  return { values: [a, b, c], sum: a + b + c };
}
