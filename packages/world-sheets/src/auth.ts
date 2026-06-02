import { createSign } from 'node:crypto';

/**
 * Minimal Google service-account auth. Signs a JWT with RS256 against the
 * key in the service-account JSON, exchanges it for an access token at
 * oauth2.googleapis.com/token, and caches the token in-process for ~50 min
 * (Google issues 1h tokens).
 *
 * No external dependency — keeps the package light. If you need OAuth user
 * flow, ADC, or workload-identity-federation, drop in google-auth-library
 * yourself and pass an access-token provider via createWorld config.
 */

export interface ServiceAccountKey {
  client_email: string;
  private_key: string;
  token_uri?: string;
  [extra: string]: unknown;
}

export type AccessTokenProvider = () => Promise<string>;

const TOKEN_SCOPE = 'https://www.googleapis.com/auth/spreadsheets';
const TOKEN_TTL_BUFFER_MS = 60_000;
const TOKEN_LIFETIME_MS = 50 * 60_000;

function b64url(input: string | Buffer): string {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

function signJwt(header: object, payload: object, privateKey: string): string {
  const h = b64url(JSON.stringify(header));
  const p = b64url(JSON.stringify(payload));
  const data = `${h}.${p}`;
  const sig = createSign('RSA-SHA256').update(data).sign(privateKey);
  return `${data}.${b64url(sig)}`;
}

export function parseServiceAccountKey(
  source: string | ServiceAccountKey
): ServiceAccountKey {
  if (typeof source === 'object') return source;
  // Allow a base64-encoded JSON blob (handy for env vars).
  let raw = source.trim();
  if (!raw.startsWith('{')) {
    try {
      raw = Buffer.from(raw, 'base64').toString('utf8');
    } catch {
      throw new Error('[world-sheets] service-account key is neither JSON nor base64(JSON)');
    }
  }
  const parsed = JSON.parse(raw) as ServiceAccountKey;
  if (!parsed.client_email || !parsed.private_key) {
    throw new Error('[world-sheets] service-account key missing client_email or private_key');
  }
  return parsed;
}

/**
 * Build an access-token provider that signs JWTs, exchanges them, and caches
 * the result.
 */
export function makeServiceAccountTokenProvider(
  key: ServiceAccountKey
): AccessTokenProvider {
  let cachedToken: string | null = null;
  let cachedUntilMs = 0;

  return async () => {
    const now = Date.now();
    if (cachedToken && cachedUntilMs - TOKEN_TTL_BUFFER_MS > now) {
      return cachedToken;
    }

    const iat = Math.floor(now / 1000);
    const jwt = signJwt(
      { alg: 'RS256', typ: 'JWT' },
      {
        iss: key.client_email,
        scope: TOKEN_SCOPE,
        aud: key.token_uri ?? 'https://oauth2.googleapis.com/token',
        iat,
        exp: iat + 3600,
      },
      key.private_key
    );

    const res = await fetch(key.token_uri ?? 'https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        assertion: jwt,
      }).toString(),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(
        `[world-sheets] token exchange failed: HTTP ${res.status} ${text.slice(0, 300)}`
      );
    }
    const body = (await res.json()) as { access_token?: string };
    if (!body.access_token) {
      throw new Error('[world-sheets] token response missing access_token');
    }
    cachedToken = body.access_token;
    cachedUntilMs = now + TOKEN_LIFETIME_MS;
    return body.access_token;
  };
}
