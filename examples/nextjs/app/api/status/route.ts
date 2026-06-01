import { getRun } from 'workflow/api';

export async function GET(request: Request): Promise<Response> {
  const runId = new URL(request.url).searchParams.get('runId');
  if (!runId) {
    return Response.json({ error: 'missing runId' }, { status: 400 });
  }
  const run = getRun(runId);
  const status = await run.status;
  const body: Record<string, unknown> = { runId, status };
  if (status === 'completed') body.returnValue = await run.returnValue;
  return Response.json(body);
}
