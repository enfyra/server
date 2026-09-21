import { isIP } from 'node:net';
import ipaddr from 'ipaddr.js';

export function normalizeIpAddress(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const ip = value.trim();
  if (!isIP(ip) || ip.includes('%')) return null;
  return ipaddr.process(ip).toString();
}

export function normalizeForwardedIp(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const token = value.trim();
  const plain = normalizeIpAddress(token);
  if (plain) return plain;
  const endpoint =
    token.match(/^\[([^\]]+)\](?::(\d{1,5}))?$/) ??
    token.match(/^([^:]+):(\d{1,5})$/);
  if (!endpoint || (endpoint[2] && Number(endpoint[2]) > 65535)) return null;
  return normalizeIpAddress(endpoint[1]);
}

export function isIpInRange(ip: string, pattern: string): boolean {
  const normalized = normalizeIpAddress(ip);
  if (!normalized) return false;
  const parts = pattern.trim().split('/');
  const range = normalizeIpAddress(parts[0]);
  if (!range || parts.length > 2) return false;
  if (parts.length === 1) return normalized === range;
  if (!/^\d{1,3}$/.test(parts[1])) return false;
  let bits = Number(parts[1]);
  const address = ipaddr.process(normalized);
  const network = ipaddr.process(range);
  if (address.kind() !== network.kind()) return false;
  if (parts[0].includes(':') && network.kind() === 'ipv4' && bits >= 96)
    bits -= 96;
  if (bits > (network.kind() === 'ipv4' ? 32 : 128)) return false;
  return address.match(network, bits);
}
