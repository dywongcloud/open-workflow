import type { IncomingMessage, ServerResponse } from 'node:http';

/** Convert a Node IncomingMessage into a WHATWG Request. */
export async function toWebRequest(
  req: IncomingMessage,
  origin: string
): Promise<Request> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const body = Buffer.concat(chunks);

  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const v of value) headers.append(key, v);
    } else if (value != null) {
      headers.set(key, value);
    }
  }

  const method = req.method ?? 'GET';
  const hasBody = method !== 'GET' && method !== 'HEAD' && body.length > 0;

  return new Request(`${origin}${req.url ?? '/'}`, {
    method,
    headers,
    body: hasBody ? body : undefined,
  });
}

/** Write a WHATWG Response back to a Node ServerResponse. */
export async function writeWebResponse(
  res: ServerResponse,
  response: Response
): Promise<void> {
  res.statusCode = response.status;
  response.headers.forEach((value, key) => {
    res.setHeader(key, value);
  });
  if (response.body) {
    const buf = Buffer.from(await response.arrayBuffer());
    res.end(buf);
  } else {
    res.end();
  }
}
