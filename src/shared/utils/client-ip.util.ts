import type { ClientIpRequest } from '../types/client-ip.types';
import { readForwardedClientContext } from './client-context.util';
import {
  isIpInRange,
  normalizeForwardedIp,
  normalizeIpAddress,
} from './ip-address.util';

const CLOUDFLARE_RANGES = [
  '173.245.48.0/20',
  '103.21.244.0/22',
  '103.22.200.0/22',
  '103.31.4.0/22',
  '141.101.64.0/18',
  '108.162.192.0/18',
  '190.93.240.0/20',
  '188.114.96.0/20',
  '197.234.240.0/22',
  '198.41.128.0/17',
  '162.158.0.0/15',
  '104.16.0.0/13',
  '104.24.0.0/14',
  '172.64.0.0/13',
  '131.0.72.0/22',
  '2400:cb00::/32',
  '2606:4700::/32',
  '2803:f800::/32',
  '2405:b500::/32',
  '2405:8100::/32',
  '2a06:98c0::/29',
  '2c0f:f248::/32',
];

const INTERNAL_PROXY_RANGES = [
  '127.0.0.0/8',
  '10.0.0.0/8',
  '172.16.0.0/12',
  '192.168.0.0/16',
  '::1/128',
  'fc00::/7',
  'fe80::/10',
];

function forwardedEntries(headers: Record<string, unknown>): unknown[] {
  if (headers['x-forwarded-for'] !== undefined) {
    const value = headers['x-forwarded-for'];
    return typeof value === 'string' ? value.split(',').slice(-64) : [null];
  }
  if (headers.forwarded !== undefined) {
    if (typeof headers.forwarded !== 'string') return [null];
    return headers.forwarded
      .split(',')
      .slice(-64)
      .map((entry) => {
        const matches = entry
          .split(';')
          .map((part) => part.trim())
          .filter((part) => /^for=/i.test(part));
        if (matches.length !== 1) return null;
        const value = matches[0].slice(4).trim();
        return value.startsWith('"') && value.endsWith('"')
          ? value.slice(1, -1)
          : value;
      });
  }
  return headers['x-real-ip'] !== undefined ? [headers['x-real-ip']] : [];
}

export function resolveClientIpFromRequest(req: ClientIpRequest): string {
  const forwardedContext = readForwardedClientContext(req);
  const headers = forwardedContext?.headers ?? req.headers ?? {};
  const peer = normalizeIpAddress(
    forwardedContext?.peerIp ??
      req.socket?.remoteAddress ??
      req.connection?.remoteAddress,
  );
  if (!peer) return 'unknown';
  let current = peer;
  const entries = forwardedEntries(headers);

  for (let index = entries.length - 1; ; index--) {
    if (CLOUDFLARE_RANGES.some((range) => isIpInRange(current, range))) {
      const connectingIp = normalizeIpAddress(headers['cf-connecting-ip']);
      if (connectingIp) {
        const ipv6 = normalizeIpAddress(headers['cf-connecting-ipv6']);
        return isIpInRange(connectingIp, '240.0.0.0/4') && ipv6?.includes(':')
          ? ipv6
          : connectingIp;
      }
      if (headers['cf-connecting-ip'] !== undefined) return current;
    } else if (
      !INTERNAL_PROXY_RANGES.some((range) => isIpInRange(current, range))
    ) {
      return current;
    }
    if (index < 0) return current;
    const next = normalizeForwardedIp(entries[index]);
    if (!next) return current;
    current = next;
  }
}
