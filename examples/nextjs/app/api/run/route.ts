import { start } from 'workflow/api';
import { greet } from '../../../workflows/greet';

export async function POST(request: Request): Promise<Response> {
  const { name = 'World' } = (await request.json().catch(() => ({}))) as {
    name?: string;
  };
  const run = await start(greet, [name]);
  return Response.json({ runId: run.runId });
}
