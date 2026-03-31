import { lookup } from 'dns/promises';
import { isIP } from 'net';

export type FetchHelper = (
  url: string,
  options?: {
    method?: string;
    headers?: Record<string, string>;
    query?: Record<string, any>;
    body?: any;
    timeoutMs?: number;
    maxBytes?: number;
    responseType?: 'json' | 'text' | 'arrayBuffer';
    allowPrivateIp?: boolean;
  },
) => Promise<any>;

function isPrivateIp(ip: string): boolean {
  if (!ip) return true;
  const v = isIP(ip);
  if (v === 4) {
    const parts = ip.split('.').map((x) => Number(x));
    if (parts.length !== 4 || parts.some((n) => Number.isNaN(n) || n < 0 || n > 255)) return true;
    const [a, b] = parts;
    if (a === 10) return true;
    if (a === 127) return true;
    if (a === 0) return true;
    if (a === 169 && b === 254) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
    if (a >= 224) return true;
    return false;
  }
  if (v === 6) {
    const normalized = ip.toLowerCase();
    if (normalized === '::1') return true;
    if (normalized.startsWith('fc') || normalized.startsWith('fd')) return true;
    if (normalized.startsWith('fe80:')) return true;
    return false;
  }
  return true;
}

function buildUrl(input: string, query?: Record<string, any>): string {
  const u = new URL(input);
  if (query && typeof query === 'object') {
    for (const [k, v] of Object.entries(query)) {
      if (v === undefined || v === null) continue;
      u.searchParams.set(k, typeof v === 'string' ? v : JSON.stringify(v));
    }
  }
  return u.toString();
}

async function assertNetworkAllowed(url: URL, allowPrivateIp?: boolean) {
  const protocol = url.protocol.toLowerCase();
  if (protocol !== 'http:' && protocol !== 'https:') {
    throw new Error(`Protocol "${protocol}" is not allowed`);
  }

  const hostname = url.hostname;
  if (!hostname) throw new Error('Invalid URL hostname');

  if (hostname === 'localhost' || hostname.endsWith('.localhost')) {
    if (!allowPrivateIp) throw new Error('Private/localhost targets are not allowed');
    return;
  }

  const ipLiteral = isIP(hostname) ? hostname : null;
  if (ipLiteral) {
    if (!allowPrivateIp && isPrivateIp(ipLiteral)) {
      throw new Error('Private IP targets are not allowed');
    }
    return;
  }

  if (allowPrivateIp) return;

  const results = await lookup(hostname, { all: true, verbatim: true });
  for (const r of results) {
    if (isPrivateIp(r.address)) {
      throw new Error('Private IP targets are not allowed');
    }
  }
}

export function createFetchHelper(defaults?: {
  timeoutMs?: number;
  maxBytes?: number;
  allowPrivateIp?: boolean;
}): FetchHelper {
  const defaultTimeoutMs = defaults?.timeoutMs ?? 8000;
  const defaultMaxBytes = defaults?.maxBytes ?? 1024 * 1024;
  const defaultAllowPrivateIp = defaults?.allowPrivateIp ?? false;

  return async (url: string, options) => {
    const finalUrl = buildUrl(url, options?.query);
    const u = new URL(finalUrl);
    await assertNetworkAllowed(u, options?.allowPrivateIp ?? defaultAllowPrivateIp);

    const timeoutMs = options?.timeoutMs ?? defaultTimeoutMs;
    const maxBytes = options?.maxBytes ?? defaultMaxBytes;
    const responseType = options?.responseType ?? 'json';

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    let body: any = options?.body;
    const headers: Record<string, string> = { ...(options?.headers || {}) };
    if (body !== undefined && body !== null) {
      if (typeof body === 'string' || body instanceof ArrayBuffer || ArrayBuffer.isView(body)) {
      } else {
        headers['content-type'] = headers['content-type'] || 'application/json';
        body = JSON.stringify(body);
      }
    }

    try {
      const res = await fetch(finalUrl, {
        method: options?.method || (body !== undefined ? 'POST' : 'GET'),
        headers,
        body,
        signal: controller.signal,
        redirect: 'follow',
      } as any);

      const contentLength = Number(res.headers?.get?.('content-length') || 0);
      if (contentLength && contentLength > maxBytes) {
        throw new Error(`Response too large (${contentLength} bytes)`);
      }

      const ab = await res.arrayBuffer();
      if (ab.byteLength > maxBytes) {
        throw new Error(`Response too large (${ab.byteLength} bytes)`);
      }

      if (!res.ok) {
        const text = new TextDecoder().decode(new Uint8Array(ab));
        throw new Error(`Fetch failed with ${res.status}: ${text.substring(0, 500)}`);
      }

      if (responseType === 'arrayBuffer') return ab;
      const text = new TextDecoder().decode(new Uint8Array(ab));
      if (responseType === 'text') return text;
      return text ? JSON.parse(text) : null;
    } finally {
      clearTimeout(timer);
    }
  };
}

