import { describe, expect, it } from 'vitest';
import { resolveClientIpFromRequest } from '../../src/shared/utils/client-ip.util';

const visitor = '198.51.100.20';
const bridge = '203.0.113.90';
const encode = (peerIp: string, headers: Record<string, unknown> = {}) =>
  Buffer.from(JSON.stringify({ version: 1, peerIp, headers })).toString(
    'base64url',
  );
const request = (
  peer: string,
  context: unknown,
  headers: Record<string, unknown> = {},
) => ({
  socket: { remoteAddress: peer },
  headers: { ...headers, 'x-enfyra-client-context': context },
});

describe('Original client context forwarded by Nuxt', () => {
  it('uses the original public client through a public bridge', () => {
    expect(resolveClientIpFromRequest(request(bridge, encode(visitor)))).toBe(
      visitor,
    );
  });

  it('does not let second-hop Cloudflare replace the original client', () => {
    expect(
      resolveClientIpFromRequest(
        request('173.245.48.1', encode(visitor), {
          'cf-connecting-ip': bridge,
          'x-forwarded-for': `${visitor}, ${bridge}`,
        }),
      ),
    ).toBe(visitor);
  });

  it('resolves the original Cloudflare and private proxy chain independently', () => {
    const context = encode('172.18.0.2', {
      'cf-connecting-ip': visitor,
      'x-forwarded-for': `${visitor}, 173.245.48.1`,
    });
    expect(resolveClientIpFromRequest(request(bridge, context))).toBe(visitor);
  });

  it('normalizes original IPv6 with pseudo IPv4 without reading second-hop headers', () => {
    expect(
      resolveClientIpFromRequest(
        request(
          bridge,
          encode('2a06:98c1::1', {
            'cf-connecting-ip': '240.1.2.3',
            'cf-connecting-ipv6': '2001:0DB8::5',
          }),
        ),
      ),
    ).toBe('2001:db8::5');
  });

  it('still ignores a forged XFF supplied to a public Nuxt ingress', () => {
    expect(
      resolveClientIpFromRequest(
        request(
          bridge,
          encode(visitor, {
            'x-forwarded-for': '1.1.1.1',
            'cf-connecting-ip': '1.1.1.1',
          }),
        ),
      ),
    ).toBe(visitor);
  });

  it('does not recursively consume a nested client context', () => {
    expect(
      resolveClientIpFromRequest(
        request(
          bridge,
          encode(visitor, {
            'x-enfyra-client-context': encode('1.1.1.1'),
          }),
        ),
      ),
    ).toBe(visitor);
  });

  it.each([
    undefined,
    '',
    'not-json',
    [],
    ['abc', 'def'],
    'a'.repeat(12289),
    Buffer.from(
      JSON.stringify({ version: 2, peerIp: visitor, headers: {} }),
    ).toString('base64url'),
    encode('not-an-ip'),
    Buffer.from(
      JSON.stringify({ version: 1, peerIp: visitor, headers: [] }),
    ).toString('base64url'),
    encode(visitor, { 'x-forwarded-for': { invalid: true } }),
  ])('falls back to actual transport for malformed context %#', (context) => {
    expect(resolveClientIpFromRequest(request(bridge, context))).toBe(bridge);
  });

  it('keeps direct requests on their actual peer when no context exists', () => {
    expect(
      resolveClientIpFromRequest({
        socket: { remoteAddress: visitor },
        headers: { 'x-forwarded-for': '1.1.1.1' },
      }),
    ).toBe(visitor);
  });

  it('treats the unsigned context as forwarded metadata, not bridge authentication', () => {
    expect(
      resolveClientIpFromRequest(request('203.0.113.10', encode(visitor))),
    ).toBe(visitor);
  });
});
