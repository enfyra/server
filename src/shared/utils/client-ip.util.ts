const CF_IPV4_RANGES = [
  [0xadf53000, 20], [0x6715f400, 22], [0x6716c800, 22], [0x671f0400, 22],
  [0x8d654000, 18], [0x6ca2c000, 18], [0xbe5df000, 20], [0xbc726000, 20],
  [0xc5eaf000, 22], [0xc6298000, 17], [0xa29e0000, 15], [0x68100000, 13],
  [0x68180000, 14], [0xac400000, 13], [0x83004800, 22],
];

const CF_IPV6_PREFIXES = [
  '2400:cb00:', '2606:4700:', '2803:f800:', '2405:b500:',
  '2405:8100:', '2a06:98c0:', '2c0f:f248:',
];

function normalizeIp(ip: string): string {
  let out = ip;
  if (out === '::1') out = '127.0.0.1';
  if (out.startsWith('::ffff:')) out = out.substring(7);
  return out;
}

function isPrivateIp(ip: string): boolean {
  if (ip === '127.0.0.1' || ip === '::1') return true;
  if (ip.startsWith('10.')) return true;
  if (ip.startsWith('192.168.')) return true;
  if (/^172\.(1[6-9]|2\d|3[01])\./.test(ip)) return true;
  if (ip.startsWith('fc') || ip.startsWith('fd') || ip.startsWith('fe80:')) return true;
  return false;
}

function isCloudflareIp(ip: string): boolean {
  if (ip.includes(':')) {
    const lower = ip.toLowerCase();
    return CF_IPV6_PREFIXES.some((p) => lower.startsWith(p));
  }
  const parts = ip.split('.');
  if (parts.length !== 4) return false;
  const num = (+parts[0] << 24) | (+parts[1] << 16) | (+parts[2] << 8) | +parts[3];
  return CF_IPV4_RANGES.some(([base, bits]) => {
    const mask = ~0 << (32 - bits);
    return (num & mask) === (base | 0);
  });
}

function parseXForwardedFor(header: unknown): string | null {
  if (typeof header !== 'string' || !header) return null;
  const entries = header.split(',').map((s) => s.trim()).filter(Boolean);
  for (let i = entries.length - 1; i >= 0; i--) {
    const ip = normalizeIp(entries[i]);
    if (!isPrivateIp(ip)) return ip;
  }
  return entries.length > 0 ? normalizeIp(entries[0]) : null;
}

export function resolveClientIpFromRequest(req: {
  headers?: Record<string, unknown>;
  ip?: string;
  connection?: { remoteAddress?: string };
  socket?: { remoteAddress?: string };
}): string {
  const headers = req.headers || {};
  const peerIp = normalizeIp(
    req.connection?.remoteAddress || req.socket?.remoteAddress || req.ip || 'unknown',
  );

  if (isCloudflareIp(peerIp)) {
    const cfIp = headers['cf-connecting-ip'];
    if (cfIp) {
      return normalizeIp(Array.isArray(cfIp) ? cfIp[0] : String(cfIp));
    }
  }

  if (isPrivateIp(peerIp)) {
    const forwarded = parseXForwardedFor(headers['x-forwarded-for']);
    if (forwarded) return forwarded;
  }

  return peerIp;
}
