import { describe, expect, it } from 'vitest';
import { resolveClientIpFromRequest } from '../../src/shared/utils/client-ip.util';

describe('Client IP resolution', () => {
  it('trusts cf-connecting-ip only when peer is Cloudflare', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'cf-connecting-ip': '203.0.113.50' },
      connection: { remoteAddress: '173.245.48.1' },
    });
    expect(result).toBe('203.0.113.50');
  });

  it('ignores cf-connecting-ip when peer is not Cloudflare', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'cf-connecting-ip': '10.0.0.99' },
      connection: { remoteAddress: '203.0.113.1' },
    });
    expect(result).toBe('203.0.113.1');
  });

  it('attacker cannot spoof cf-connecting-ip from public IP', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'cf-connecting-ip': '1.1.1.1' },
      connection: { remoteAddress: '45.33.32.156' },
    });
    expect(result).toBe('45.33.32.156');
  });

  it('reads X-Forwarded-For when peer is private (Docker/nginx)', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'x-forwarded-for': '203.0.113.77, 172.16.0.1' },
      connection: { remoteAddress: '172.16.0.1' },
    });
    expect(result).toBe('203.0.113.77');
  });

  it('reads X-Forwarded-For when peer is loopback', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'x-forwarded-for': '198.51.100.20' },
      connection: { remoteAddress: '127.0.0.1' },
    });
    expect(result).toBe('198.51.100.20');
  });

  it('uses peer IP directly when public and no proxy headers', () => {
    const result = resolveClientIpFromRequest({
      headers: {},
      connection: { remoteAddress: '198.51.100.5' },
    });
    expect(result).toBe('198.51.100.5');
  });

  it('normalizes ::ffff: prefix', () => {
    const result = resolveClientIpFromRequest({
      headers: {},
      connection: { remoteAddress: '::ffff:192.168.1.100' },
    });
    expect(result).toBe('192.168.1.100');
  });

  it('handles Cloudflare IPv6 peer', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'cf-connecting-ip': '2001:db8::1' },
      connection: { remoteAddress: '2400:cb00::1' },
    });
    expect(result).toBe('2001:db8::1');
  });

  it('ignores X-Forwarded-For from public peer', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'x-forwarded-for': '10.0.0.1' },
      connection: { remoteAddress: '203.0.113.99' },
    });
    expect(result).toBe('203.0.113.99');
  });

  it('picks rightmost public IP from X-Forwarded-For chain', () => {
    const result = resolveClientIpFromRequest({
      headers: { 'x-forwarded-for': '203.0.113.1, 198.51.100.2, 172.16.0.1' },
      connection: { remoteAddress: '192.168.1.1' },
    });
    expect(result).toBe('198.51.100.2');
  });
});
