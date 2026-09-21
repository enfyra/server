import type {
  ClientIpRequest,
  ForwardedClientContext,
} from '../types/client-ip.types';
import { normalizeIpAddress } from './ip-address.util';

const CONTEXT_HEADERS = new Set([
  'x-forwarded-for',
  'forwarded',
  'x-real-ip',
  'cf-connecting-ip',
  'cf-connecting-ipv6',
  'host',
  'x-forwarded-host',
  'x-forwarded-proto',
  'x-forwarded-port',
  'origin',
  'referer',
  'user-agent',
]);

export function readForwardedClientContext(
  req: ClientIpRequest,
): ForwardedClientContext | null {
  const encoded = req.headers?.['x-enfyra-client-context'];
  if (
    typeof encoded !== 'string' ||
    encoded.length > 12288 ||
    !/^[A-Za-z0-9_-]+$/.test(encoded)
  )
    return null;
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const data = value as Record<string, unknown>;
  if (data.version !== 1 || !normalizeIpAddress(data.peerIp)) return null;
  if (
    !data.headers ||
    typeof data.headers !== 'object' ||
    Array.isArray(data.headers)
  )
    return null;
  const headers: Record<string, string | string[]> = {};
  for (const [name, header] of Object.entries(data.headers)) {
    if (!CONTEXT_HEADERS.has(name)) continue;
    if (
      typeof header !== 'string' &&
      !(
        Array.isArray(header) &&
        header.every((item) => typeof item === 'string')
      )
    )
      return null;
    headers[name] = header;
  }
  return { version: 1, peerIp: normalizeIpAddress(data.peerIp)!, headers };
}
