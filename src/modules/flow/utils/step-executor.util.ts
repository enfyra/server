import * as dns from 'dns';
import * as net from 'net';
import { TDynamicContext } from '../../../shared/types';
import { ExecutorEngineService } from '../../../infrastructure/executor-engine/services/executor-engine.service';
import { transformCode } from '../../../infrastructure/executor-engine/code-transformer';

const DEFAULT_HTTP_TIMEOUT = 30000;
const MAX_HTTP_TIMEOUT = 60000;

const BLOCKED_HOSTNAMES = new Set([
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '[::1]',
  '::1',
]);

function clampTimeout(
  value: unknown,
  defaultMs: number,
  maxMs: number,
): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return defaultMs;
  return Math.min(n, maxMs);
}

function isPrivateIp(ip: string): boolean {
  if (net.isIPv4(ip)) {
    const parts = ip.split('.').map(Number);
    if (parts[0] === 127) return true;
    if (parts[0] === 10) return true;
    if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
    if (parts[0] === 192 && parts[1] === 168) return true;
    if (parts[0] === 169 && parts[1] === 254) return true;
    if (parts[0] === 0) return true;
    return false;
  }
  if (net.isIPv6(ip)) {
    const normalized = ip.toLowerCase();
    if (normalized === '::1') return true;
    if (normalized.startsWith('fc') || normalized.startsWith('fd')) return true;
    if (normalized.startsWith('fe80')) return true;
    return false;
  }
  return false;
}

async function validateHttpUrl(rawUrl: string): Promise<URL> {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error('Invalid URL provided for HTTP step');
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(
      `URL protocol must be http or https, got: ${parsed.protocol}`,
    );
  }
  const hostname = parsed.hostname.replace(/^\[|\]$/g, '');
  if (BLOCKED_HOSTNAMES.has(hostname.toLowerCase())) {
    throw new Error('HTTP requests to this host are not allowed');
  }
  if (!hostname.includes('.') || hostname.endsWith('.local')) {
    throw new Error('HTTP requests to internal hosts are not allowed');
  }
  if (net.isIP(hostname)) {
    if (isPrivateIp(hostname)) {
      throw new Error('HTTP requests to private IP addresses are not allowed');
    }
    return parsed;
  }
  try {
    const addresses = await dns.promises
      .resolve4(hostname)
      .catch(() => [] as string[]);
    const addresses6 = await dns.promises
      .resolve6(hostname)
      .catch(() => [] as string[]);
    for (const addr of [...addresses, ...addresses6]) {
      if (isPrivateIp(addr)) {
        throw new Error(
          'HTTP requests to hosts resolving to private IPs are not allowed',
        );
      }
    }
  } catch (err) {
    if (err.message?.includes('not allowed')) throw err;
  }
  return parsed;
}

export interface StepExecOptions {
  type: string;
  config: Record<string, any>;
  timeout: number;
  ctx: TDynamicContext;
  executorEngineService: ExecutorEngineService;
  shouldTransformCode?: boolean;
}

export async function executeStepCore(opts: StepExecOptions): Promise<any> {
  const {
    type,
    config,
    timeout,
    ctx,
    executorEngineService,
    shouldTransformCode,
  } = opts;

  switch (type) {
    case 'script': {
      const code = shouldTransformCode
        ? transformCode(config.code || '')
        : config.code || '';
      return executorEngineService.run(code, ctx, timeout);
    }

    case 'condition': {
      const code = shouldTransformCode
        ? transformCode(config.code || 'return false;')
        : config.code || 'return false;';
      return executorEngineService.run(code, ctx, timeout);
    }

    case 'query':
      if (!config.table)
        throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].find({
        filter: config.filter,
        fields: config.fields,
        limit: config.limit,
        sort: config.sort,
      });

    case 'create':
      if (!config.table)
        throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].create({ data: config.data });

    case 'update':
      if (!config.table)
        throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].update({
        id: config.id,
        data: config.data,
      });

    case 'delete':
      if (!config.table)
        throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].delete({ id: config.id });

    case 'http': {
      if (!config.url)
        throw new Error('Step config missing required field: url');
      const safeUrl = await validateHttpUrl(config.url);
      const httpTimeoutMs = clampTimeout(
        config.timeout || timeout,
        DEFAULT_HTTP_TIMEOUT,
        MAX_HTTP_TIMEOUT,
      );
      const controller = new AbortController();
      const httpTimer = setTimeout(() => controller.abort(), httpTimeoutMs);
      try {
        const method = config.method || 'GET';
        const hasBody =
          !['GET', 'DELETE'].includes(method) && config.body !== undefined;
        const headers = { ...(config.headers || {}) };
        if (
          hasBody &&
          !Object.keys(headers).some((k) => k.toLowerCase() === 'content-type')
        ) {
          headers['Content-Type'] = 'application/json';
        }
        const response = await fetch(safeUrl.href, {
          method,
          headers,
          body: hasBody ? JSON.stringify(config.body) : undefined,
          signal: controller.signal,
        });
        const contentType = response.headers.get('content-type') || '';
        const data = contentType.includes('json')
          ? await response.json()
          : await response.text();
        return { status: response.status, data };
      } catch (err) {
        if (err.name === 'AbortError')
          throw new Error(`HTTP request timed out after ${httpTimeoutMs}ms`);
        throw err;
      } finally {
        clearTimeout(httpTimer);
      }
    }

    case 'sleep': {
      const rawMs = Number(config.ms);
      const sleepMs =
        Number.isFinite(rawMs) && rawMs > 0 ? Math.min(rawMs, 60000) : 1000;
      await new Promise<void>((resolve) => {
        setTimeout(resolve, sleepMs);
      });
      return { slept: sleepMs };
    }

    case 'log': {
      const msg = config.message || JSON.stringify((ctx as any).$flow?.$last);
      if (ctx.$logs) ctx.$logs(msg);
      return { logged: true, message: msg };
    }

    default:
      throw new Error(`Unknown step type: ${type}`);
  }
}
