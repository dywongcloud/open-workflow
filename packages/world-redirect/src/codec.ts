import { decode as cborDecode, encode as cborEncode } from 'cbor-x';

/**
 * Entities and events are stored as CBOR (compact, binary-safe — natively
 * round-trips Date and Uint8Array, which is exactly what SerializedData and
 * the entity timestamps need) and then base64-encoded so the value is a plain
 * string for both the RESP and Upstash REST transports.
 */
export function encodeBlob(value: unknown): string {
  return Buffer.from(cborEncode(value)).toString('base64');
}

export function decodeBlob<T = unknown>(b64: string): T {
  return cborDecode(Buffer.from(b64, 'base64')) as T;
}

/** Encode raw bytes (e.g. a stream chunk) to base64 for storage. */
export function bytesToBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('base64');
}

/** Decode base64 back to a Uint8Array. */
export function base64ToBytes(b64: string): Uint8Array {
  return new Uint8Array(Buffer.from(b64, 'base64'));
}
