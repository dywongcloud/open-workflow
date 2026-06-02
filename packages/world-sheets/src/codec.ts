import { decode as cborDecode, encode as cborEncode } from 'cbor-x';
import { createHash } from 'node:crypto';
import { monotonicFactory } from 'ulid';

const ulid = monotonicFactory();

export const newEventId = () => `evnt_${ulid()}`;
export const newRunId = () => `wrun_${ulid()}`;
export const newMessageId = () => `msg_${ulid()}`;

export function encodeBlob(value: unknown): string {
  return Buffer.from(cborEncode(value)).toString('base64');
}

export function decodeBlob<T = unknown>(b64: string): T | undefined {
  if (!b64) return undefined;
  return cborDecode(Buffer.from(b64, 'base64')) as T;
}

export function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

const SAFE_ID = /^[A-Za-z0-9_-]+$/;
export function assertSafeEntityId(name: string, value: string): void {
  if (!SAFE_ID.test(value)) {
    throw new Error(
      `[world-sheets] Invalid ${name} "${value}": must match ${SAFE_ID}`
    );
  }
}
