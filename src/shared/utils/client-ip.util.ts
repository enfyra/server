export function resolveClientIpFromRequest(req: {
  headers?: Record<string, unknown>;
  ip?: string;
  connection?: { remoteAddress?: string };
  socket?: { remoteAddress?: string };
}): string {
  const headers = req.headers || {};
  const cfConnectingIP = headers['cf-connecting-ip'];
  const remoteAddress =
    req.connection?.remoteAddress || req.socket?.remoteAddress;
  const reqIP = req.ip;
  let clientIP: string;

  if (cfConnectingIP) {
    clientIP = Array.isArray(cfConnectingIP)
      ? cfConnectingIP[0]
      : String(cfConnectingIP);
  } else if (reqIP && reqIP !== '::1' && reqIP !== '127.0.0.1') {
    clientIP = reqIP;
  } else if (
    remoteAddress &&
    remoteAddress !== '::1' &&
    remoteAddress !== '127.0.0.1'
  ) {
    clientIP = remoteAddress;
  } else {
    clientIP = reqIP || remoteAddress || 'unknown';
  }

  if (clientIP === '::1') {
    clientIP = '127.0.0.1';
  }
  if (clientIP?.startsWith('::ffff:')) {
    clientIP = clientIP.substring(7);
  }

  return clientIP;
}
