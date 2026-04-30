import { resolveClientIpFromRequest } from '../../src/shared/utils/client-ip.util';

describe('resolveClientIpFromRequest', () => {
  it('prioritizes CF-Connecting-IP over req.ip', () => {
    const ip = resolveClientIpFromRequest({
      headers: { 'cf-connecting-ip': '1.2.3.4', 'x-forwarded-for': '5.6.7.8' },
      ip: '192.168.1.1',
    });
    expect(ip).toBe('1.2.3.4');
  });

  it('ignores X-Forwarded-For', () => {
    const ip = resolveClientIpFromRequest({
      headers: { 'x-forwarded-for': '5.6.7.8' },
      ip: '192.168.1.1',
    });
    expect(ip).toBe('192.168.1.1');
  });

  it('falls back to req.ip when no CF header', () => {
    const ip = resolveClientIpFromRequest({
      headers: {},
      ip: '203.0.113.1',
    });
    expect(ip).toBe('203.0.113.1');
  });

  it('falls back to connection remoteAddress when req.ip is loopback', () => {
    const ip = resolveClientIpFromRequest({
      headers: {},
      ip: '127.0.0.1',
      connection: { remoteAddress: '10.0.0.1' },
    });
    expect(ip).toBe('10.0.0.1');
  });

  it('normalizes ::ffff: IPv4-mapped addresses', () => {
    const ip = resolveClientIpFromRequest({
      headers: {},
      ip: '::ffff:192.0.2.1',
    });
    expect(ip).toBe('192.0.2.1');
  });

  it('handles array cf-connecting-ip header: picks first value', () => {
    const ip = resolveClientIpFromRequest({
      headers: { 'cf-connecting-ip': ['7.7.7.7', '8.8.8.8'] },
      ip: '203.0.113.1',
    });
    expect(ip).toBe('7.7.7.7');
  });

  it('normalizes ::1 to 127.0.0.1', () => {
    const ip = resolveClientIpFromRequest({
      headers: {},
      ip: '::1',
    });
    expect(ip).toBe('127.0.0.1');
  });
});
