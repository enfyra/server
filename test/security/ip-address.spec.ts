import { describe, expect, it } from 'vitest';
import {
  isIpInRange,
  normalizeIpAddress,
} from '../../src/shared/utils/ip-address.util';

describe('IP parser', () => {
  it.each(['127.1', '0x7f000001', '010.0.0.1', '1.2.3.4junk', 'fe80::1%eth0'])(
    'rejects noncanonical or scoped input %s',
    (value) => {
      expect(normalizeIpAddress(value)).toBeNull();
    },
  );
  it.each(['bad', '10.0.0.0/33', '::/129', '10.0.0.1/8oops', '10.0.0.1/8/8'])(
    'rejects invalid CIDR %s',
    (value) => {
      expect(isIpInRange('10.0.0.1', value)).toBe(false);
    },
  );
  it('matches IPv4-mapped IPv6 CIDRs and legacy dotted mapped patterns', () => {
    expect(isIpInRange('192.0.2.1', '::ffff:192.0.2.0/120')).toBe(true);
    expect(isIpInRange('192.0.2.1', '::ffff:192.0.2.0/24')).toBe(true);
    expect(isIpInRange('192.0.3.1', '::ffff:192.0.2.0/120')).toBe(false);
  });
});
