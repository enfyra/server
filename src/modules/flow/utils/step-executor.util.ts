import { TDynamicContext } from '../../../shared/types';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { transformCode } from '../../../infrastructure/handler-executor/code-transformer';

const DEFAULT_HTTP_TIMEOUT = 30000;
const MAX_HTTP_TIMEOUT = 60000;
const MAX_SLEEP_MS = 60000;
const DEFAULT_SLEEP_MS = 1000;

const BLOCKED_HOSTNAMES = new Set([
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '[::1]',
  '::1',
]);

function clampTimeout(value: unknown, defaultMs: number, maxMs: number): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return defaultMs;
  return Math.min(n, maxMs);
}

function validateHttpUrl(url: string): void {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error(`Invalid URL: ${url}`);
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`URL protocol must be http or https, got: ${parsed.protocol}`);
  }
  const hostname = parsed.hostname.replace(/^\[|\]$/g, '');
  if (BLOCKED_HOSTNAMES.has(hostname)) {
    throw new Error(`HTTP requests to ${hostname} are not allowed`);
  }
  if (!hostname.includes('.') || hostname.endsWith('.local')) {
    throw new Error(`HTTP requests to internal hosts are not allowed: ${hostname}`);
  }
  if (/^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)/.test(hostname)) {
    throw new Error(`HTTP requests to private IP ranges are not allowed: ${hostname}`);
  }
}

export interface StepExecOptions {
  type: string;
  config: Record<string, any>;
  timeout: number;
  ctx: TDynamicContext;
  handlerExecutor: HandlerExecutorService;
  shouldTransformCode?: boolean;
}

export async function executeStepCore(opts: StepExecOptions): Promise<any> {
  const { type, config, timeout, ctx, handlerExecutor, shouldTransformCode } = opts;

  switch (type) {
    case 'script': {
      const code = shouldTransformCode ? transformCode(config.code || '') : (config.code || '');
      return handlerExecutor.run(code, ctx, timeout);
    }

    case 'condition': {
      const code = shouldTransformCode ? transformCode(config.code || 'return false;') : (config.code || 'return false;');
      return handlerExecutor.run(code, ctx, timeout);
    }

    case 'query':
      if (!config.table) throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].find({
        filter: config.filter,
        fields: config.fields,
        limit: config.limit,
        sort: config.sort,
      });

    case 'create':
      if (!config.table) throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].create({ data: config.data });

    case 'update':
      if (!config.table) throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].update({ id: config.id, data: config.data });

    case 'delete':
      if (!config.table) throw new Error('Step config missing required field: table');
      return ctx.$repos[config.table].delete({ id: config.id });

    case 'http': {
      if (!config.url) throw new Error('Step config missing required field: url');
      validateHttpUrl(config.url);
      const httpTimeoutMs = clampTimeout(config.timeout || timeout, DEFAULT_HTTP_TIMEOUT, MAX_HTTP_TIMEOUT);
      const controller = new AbortController();
      const httpTimeout = setTimeout(() => controller.abort(), httpTimeoutMs);
      try {
        const method = config.method || 'GET';
        const hasBody = !['GET', 'DELETE'].includes(method) && config.body !== undefined;
        const headers = { ...(config.headers || {}) };
        if (hasBody && !Object.keys(headers).some(k => k.toLowerCase() === 'content-type')) {
          headers['Content-Type'] = 'application/json';
        }
        const response = await fetch(config.url, {
          method,
          headers,
          body: hasBody ? JSON.stringify(config.body) : undefined,
          signal: controller.signal,
        });
        const contentType = response.headers.get('content-type') || '';
        const data = contentType.includes('json') ? await response.json() : await response.text();
        return { status: response.status, data };
      } catch (err) {
        if (err.name === 'AbortError') throw new Error(`HTTP request to ${config.url} timed out after ${httpTimeoutMs}ms`);
        throw err;
      } finally {
        clearTimeout(httpTimeout);
      }
    }

    case 'sleep': {
      const sleepMs = clampTimeout(config.ms, DEFAULT_SLEEP_MS, MAX_SLEEP_MS);
      await new Promise((r) => setTimeout(r, sleepMs));
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
