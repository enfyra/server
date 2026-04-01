import { Injectable, Logger } from '@nestjs/common';
import * as vm from 'vm';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { PackageCdnLoaderService } from '../../cache/services/package-cdn-loader.service';
import { ErrorHandler } from '../utils/error-handler';
import { ScriptTimeoutException, ScriptExecutionException } from '../../../core/exceptions/custom-exceptions';

const DEFAULT_LOG_LIMIT_ENTRIES = 200;
const DEFAULT_LOG_LIMIT_BYTES = 64 * 1024;
const DEFAULT_RESULT_LIMIT_BYTES = 256 * 1024;

const BLOCKED_MODULES = new Set([
  'assert',
  'assert/strict',
  'async_hooks',
  'child_process',
  'cluster',
  'console',
  'dgram',
  'diagnostics_channel',
  'dns',
  'domain',
  'events',
  'fs',
  'fs/promises',
  'http',
  'http2',
  'https',
  'inspector',
  'module',
  'net',
  'os',
  'path',
  'perf_hooks',
  'process',
  'punycode',
  'querystring',
  'readline',
  'repl',
  'stream',
  'stream/promises',
  'stream/web',
  'string_decoder',
  'sys',
  'timers',
  'timers/promises',
  'tls',
  'trace_events',
  'tty',
  'url',
  'util',
  'v8',
  'vm',
  'wasi',
  'worker_threads',
  'zlib',
]);

const NETWORK_DENYLIST_PACKAGES = new Set([
  'axios',
  'node-fetch',
  'undici',
  'got',
  'superagent',
  'request',
  'needle',
  'node-libcurl',
  'ws',
  'socket.io-client',
]);

@Injectable()
export class VmExecutorService {
  private readonly logger = new Logger(VmExecutorService.name);

  constructor(
    private packageCacheService: PackageCacheService,
    private cdnLoader: PackageCdnLoaderService,
  ) {}

  private safeJsonSize(value: any, maxBytes: number): { ok: true; bytes: number } | { ok: false; bytes: number } {
    const seen = new WeakSet<object>();
    const replacer = (_k: string, v: any) => {
      if (typeof v === 'object' && v !== null) {
        if (seen.has(v)) return '[Circular]';
        seen.add(v);
      }
      if (typeof v === 'bigint') return v.toString();
      if (typeof v === 'function') return '[Function]';
      return v;
    };
    try {
      const s = JSON.stringify(value, replacer);
      const bytes = Buffer.byteLength(s || '', 'utf8');
      if (bytes > maxBytes) return { ok: false, bytes };
      return { ok: true, bytes };
    } catch {
      return { ok: true, bytes: 0 };
    }
  }

  private createCappedLogger(ctx: TDynamicContext, limits: { maxEntries: number; maxBytes: number }) {
    const share = (ctx.$share = ctx.$share || ({} as any));
    const logs = (share.$logs = Array.isArray(share.$logs) ? share.$logs : []);
    let totalBytes = 0;
    try {
      totalBytes = Buffer.byteLength(JSON.stringify(logs).slice(0, limits.maxBytes), 'utf8');
    } catch {
      totalBytes = 0;
    }

    return (...args: any[]) => {
      if (logs.length >= limits.maxEntries) {
        throw new Error(`Log limit exceeded (${limits.maxEntries} entries)`);
      }
      const size = this.safeJsonSize(args, limits.maxBytes);
      const addBytes = size.ok ? size.bytes : limits.maxBytes + 1;
      if (totalBytes + addBytes > limits.maxBytes) {
        throw new Error(`Log limit exceeded (${limits.maxBytes} bytes)`);
      }
      logs.push(...args);
      totalBytes += addBytes;
    };
  }

  private freezeShallow<T extends object>(obj: T): T {
    try {
      return Object.freeze(obj);
    } catch {
      return obj;
    }
  }

  async run(code: string, ctx: TDynamicContext, timeoutMs: number): Promise<any> {
    const packages = await this.packageCacheService.getPackages();
    const loadedModules = this.cdnLoader.getLoadedPackages();

    const pkgs: Record<string, any> = {};
    for (const packageName of packages) {
      const mod = loadedModules.get(packageName);
      if (mod) pkgs[packageName] = mod;
    }
    (ctx as any).$pkgs = pkgs;

    const $throw = this.buildThrowProxy(code);

    const normalizeModuleName = (raw: string) => {
      const name = String(raw || '').trim();
      return name.startsWith('node:') ? name.slice('node:'.length) : name;
    };

    const safeRequire = (rawModuleName: string) => {
      const moduleName = normalizeModuleName(rawModuleName);

      if (!moduleName) {
        throw new Error('Module name is required');
      }

      if (BLOCKED_MODULES.has(moduleName)) {
        throw new Error(`Module "${moduleName}" is not allowed in handlers`);
      }

      for (const denied of NETWORK_DENYLIST_PACKAGES) {
        if (moduleName === denied || moduleName.startsWith(`${denied}/`)) {
          throw new Error(`Module "${moduleName}" is not allowed in handlers. Use $fetch instead`);
        }
      }

      const mod = loadedModules.get(moduleName);
      if (!mod) {
        throw new Error(
          `Module "${moduleName}" is not available. Install/enable it via Settings → Packages`,
        );
      }
      return mod;
    };

    const safeProcess = {
      env: {},
      version: process.version,
      platform: process.platform,
      nextTick: process.nextTick.bind(process),
    };

    const safeConsole = {
      log: (...args: any[]) => console.log(...args),
      info: (...args: any[]) => console.info(...args),
      warn: (...args: any[]) => console.warn(...args),
      error: (...args: any[]) => console.error(...args),
    };

    const safeSetInterval = () => {
      throw new Error('setInterval is not allowed in handlers');
    };

    const cappedLogs = this.createCappedLogger(ctx, {
      maxEntries: DEFAULT_LOG_LIMIT_ENTRIES,
      maxBytes: DEFAULT_LOG_LIMIT_BYTES,
    });

    const frozenHelpers = this.freezeShallow({ ...(ctx.$helpers || {}) });
    const frozenReq = this.freezeShallow({ ...(ctx.$req as any) });
    const frozenApi = this.freezeShallow({
      ...(ctx.$api as any),
      request: this.freezeShallow({ ...((ctx.$api as any)?.request || {}) }),
    } as any);
    const frozenSocket = this.freezeShallow({ ...(ctx.$socket as any) } as any);
    const frozenThrow = this.freezeShallow($throw as any);

    const sandbox: any = {
      $ctx: {
        ...ctx,
        $throw: frozenThrow,
        $helpers: frozenHelpers,
        $req: frozenReq as any,
        $api: frozenApi,
        $socket: frozenSocket,
        $logs: cappedLogs,
      },
      console: safeConsole,
      Buffer,
      setTimeout,
      setInterval: safeSetInterval,
      clearTimeout,
      clearInterval,
      Date,
      JSON,
      Math,
      RegExp,
      Array,
      Object,
      Map,
      Set,
      WeakMap,
      WeakSet,
      Promise,
      Error,
      TypeError,
      RangeError,
      SyntaxError,
      URIError,
      Symbol,
      Proxy,
      Reflect,
      URL,
      URLSearchParams,
      TextEncoder,
      TextDecoder,
      parseInt,
      parseFloat,
      isNaN,
      isFinite,
      encodeURIComponent,
      decodeURIComponent,
      encodeURI,
      decodeURI,
      queueMicrotask,
      structuredClone,
      require: safeRequire,
      process: safeProcess,
      fetch: undefined,
      WebSocket: undefined,
    };
    sandbox.$fetch = (...args: any[]) => sandbox.$ctx?.$helpers?.$fetch?.(...args);

    const vmContext = vm.createContext(sandbox, {
      codeGeneration: { strings: false, wasm: false },
    });

    const wrappedCode = `
      (async () => {
        "use strict";
        ${code}
      })();
    `;

    try {
      const script = new vm.Script(wrappedCode, {
        filename: 'handler.js',
      });

      const resultPromise = script.runInContext(vmContext, {
        timeout: Number(timeoutMs) || 30000,
      });

      const result = await this.withAsyncTimeout(resultPromise, Number(timeoutMs) || 30000, code);

      const resultSize = this.safeJsonSize(result, DEFAULT_RESULT_LIMIT_BYTES);
      if (!resultSize.ok) {
        throw new Error(`Result too large (${resultSize.bytes} bytes)`);
      }

      const outCtx = sandbox.$ctx as TDynamicContext;
      if (outCtx && typeof outCtx === 'object') {
        if (outCtx.$body !== undefined) ctx.$body = outCtx.$body;
        if (outCtx.$query !== undefined) ctx.$query = outCtx.$query;
        if (outCtx.$params !== undefined) ctx.$params = outCtx.$params;
        if (outCtx.$data !== undefined) ctx.$data = outCtx.$data;
        if (outCtx.$statusCode !== undefined) (ctx as any).$statusCode = (outCtx as any).$statusCode;
        if (outCtx.$share) ctx.$share = outCtx.$share;
      }

      delete (ctx as any).$throw;
      delete (ctx as any).$pkgs;

      return result;
    } catch (error) {
      if (error.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT') {
        throw new ScriptTimeoutException(timeoutMs, code);
      }

      if (error.constructor?.name?.includes('Exception')) {
        throw error;
      }

      let errorLine = null;
      let codeContextArray: string[] = [];
      try {
        const stackMatch = error.stack?.match(/handler\.js:(\d+)/);
        if (stackMatch) {
          const rawLine = parseInt(stackMatch[1]);
          errorLine = rawLine - 2;
          if (errorLine > 0) {
            const lines = code.split('\n');
            const start = Math.max(0, errorLine - 2);
            const end = Math.min(lines.length, errorLine + 3);
            codeContextArray = lines.slice(start, end).map((line, idx) => {
              const num = start + idx + 1;
              return `${num === errorLine ? '>' : ' '} ${num}. ${line}`;
            });
          }
        }
      } catch {
      }

      const details: any = {};
      if (errorLine) {
        details.location = { line: errorLine };
        details.code = codeContextArray;
      }

      throw ErrorHandler.createException(
        undefined,
        error.statusCode || error.status,
        error.message || 'Unknown error',
        code,
        details,
      );
    } finally {
      try {
        if (ctx?.$share?.$logs && Array.isArray(ctx.$share.$logs)) {
          if (ctx.$share.$logs.length > DEFAULT_LOG_LIMIT_ENTRIES) {
            ctx.$share.$logs = ctx.$share.$logs.slice(0, DEFAULT_LOG_LIMIT_ENTRIES);
          }
        }
      } catch {
      }
    }
  }

  private withAsyncTimeout<T>(promise: Promise<T>, timeoutMs: number, code: string): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new ScriptTimeoutException(timeoutMs, code));
      }, timeoutMs);

      promise.then(
        (result) => { clearTimeout(timer); resolve(result); },
        (error) => { clearTimeout(timer); reject(error); },
      );
    });
  }

  private buildThrowProxy(code: string): any {
    return new Proxy({}, {
      get(_, prop: string) {
        return (message: string, details?: any) => {
          throw ErrorHandler.createException(`$throw.${prop}`, undefined, message, code, details);
        };
      },
    });
  }
}
