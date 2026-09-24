import { describe, expect, it } from 'vitest';
import { resolveClientIpFromRequest } from '../../src/shared/utils/client-ip.util';

const visitor = '198.51.100.20';
const edge = '173.245.48.1';
const resolve = (peer: string, headers: Record<string, unknown>) =>
  resolveClientIpFromRequest({ socket: { remoteAddress: peer }, headers });

describe('Client IP topology boundaries', () => {
  it.each(['127.0.0.1', '172.18.0.2', 'fd00::2'])(
    'resolves Cloudflare behind internal proxy %s',
    (peer) => {
      expect(
        resolve(peer, {
          'x-forwarded-for': `${visitor}, ${edge}`,
          'cf-connecting-ip': visitor,
        }),
      ).toBe(visitor);
    },
  );

  it('walks internal proxy chains without selecting a Cloudflare edge as the visitor', () => {
    expect(
      resolve('10.0.0.2', {
        'x-forwarded-for': `${visitor}, ${edge}, 192.168.1.20`,
        'cf-connecting-ip': visitor,
      }),
    ).toBe(visitor);
  });

  it('does not infer trust for arbitrary public proxies', () => {
    expect(
      resolve('203.0.113.2', {
        'x-forwarded-for': visitor,
        'cf-connecting-ip': visitor,
      }),
    ).toBe('203.0.113.2');
  });

  it('stops at the first public non-Cloudflare hop rather than accepting spoofed headers', () => {
    expect(
      resolve('127.0.0.1', {
        'x-forwarded-for': `1.1.1.1, ${edge}, ${visitor}`,
        'cf-connecting-ip': '1.1.1.1',
      }),
    ).toBe(visitor);
  });

  it('does not use req.ip as transport provenance', () => {
    expect(
      resolveClientIpFromRequest({
        ip: edge,
        headers: { 'cf-connecting-ip': visitor },
      }),
    ).toBe('unknown');
  });

  it('recognizes complete loopback and IPv6 internal CIDRs', () => {
    for (const peer of ['127.0.0.2', 'fc00::1', 'fdff::1', 'fe90::1'])
      expect(resolve(peer, { 'x-forwarded-for': visitor })).toBe(visitor);
  });

  it('recognizes the complete Cloudflare IPv6 CIDR', () => {
    expect(resolve('2a06:98c1::1', { 'cf-connecting-ip': visitor })).toBe(
      visitor,
    );
    expect(resolve('2a06:98c7:ffff::1', { 'cf-connecting-ip': visitor })).toBe(
      visitor,
    );
    expect(resolve('2a06:98c8::1', { 'cf-connecting-ip': visitor })).toBe(
      '2a06:98c8::1',
    );
  });

  it('uses Tunnel XFF without trusting a CF header from an unverified local hop', () => {
    expect(
      resolve('127.0.0.1', {
        'cf-connecting-ip': visitor,
        'x-forwarded-for': visitor,
      }),
    ).toBe(visitor);
    expect(resolve('127.0.0.1', { 'cf-connecting-ip': visitor })).toBe(
      '127.0.0.1',
    );
  });

  it('preserves actual IPv6 when Cloudflare uses pseudo IPv4', () => {
    expect(
      resolve(edge, {
        'cf-connecting-ip': '240.1.2.3',
        'cf-connecting-ipv6': '2001:0DB8::5',
      }),
    ).toBe('2001:db8::5');
  });

  it('normalizes IPv6 and mapped IPv4 without conflating IPv6 loopback', () => {
    expect(resolve('2001:0DB8:0:0:0:0:0:5', {})).toBe('2001:db8::5');
    expect(resolve('::ffff:c000:0201', {})).toBe('192.0.2.1');
    expect(resolve('::1', {})).toBe('::1');
  });

  it.each(['not-an-ip', '1.2.3.4junk', '999.0.0.1', '', '198.51.100.20, bad'])(
    'stops at malformed XFF %s',
    (value) => {
      expect(resolve('127.0.0.1', { 'x-forwarded-for': value })).toBe(
        '127.0.0.1',
      );
    },
  );

  it('does not skip an invalid intermediate hop', () => {
    expect(
      resolve('127.0.0.1', { 'x-forwarded-for': `${visitor}, bad, 10.0.0.2` }),
    ).toBe('10.0.0.2');
  });

  it('ignores invalid and duplicate Cloudflare identity headers', () => {
    for (const value of ['bad', `${visitor}, 1.1.1.1`, [visitor, '1.1.1.1']])
      expect(resolve(edge, { 'cf-connecting-ip': value })).toBe(edge);
  });

  it('supports internal RFC 7239 and X-Real-IP proxies', () => {
    expect(
      resolve('127.0.0.1', {
        forwarded: 'for="[2001:db8::5]:443";proto=https',
      }),
    ).toBe('2001:db8::5');
    expect(resolve('127.0.0.1', { 'x-real-ip': visitor })).toBe(visitor);
    expect(
      resolve('127.0.0.1', { 'x-forwarded-for': `${visitor}:12345` }),
    ).toBe(visitor);
  });

  it('does not fall back to another header after malformed XFF', () => {
    expect(
      resolve('127.0.0.1', { 'x-forwarded-for': 'bad', 'x-real-ip': visitor }),
    ).toBe('127.0.0.1');
  });

  it('bounds long chains and never trusts the leftmost spoofed prefix', () => {
    expect(
      resolve('127.0.0.1', {
        'x-forwarded-for': `${'1.1.1.1, '.repeat(1000)}${visitor}`,
      }),
    ).toBe(visitor);
  });
});
