import { Injectable, Logger } from '@nestjs/common';
import * as vm from 'vm';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { ErrorHandler } from '../utils/error-handler';
import { ScriptTimeoutException, ScriptExecutionException } from '../../../core/exceptions/custom-exceptions';

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

  constructor(private packageCacheService: PackageCacheService) {}

  async run(code: string, ctx: TDynamicContext, timeoutMs: number): Promise<any> {
    const packages = await this.packageCacheService.getPackages();
    const allowedPackages = new Set(packages);

    const pkgs: Record<string, any> = {};
    for (const packageName of packages) {
      try {
        pkgs[packageName] = require(packageName);
      } catch {
      }
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

      if (!allowedPackages.has(moduleName)) {
        throw new Error(
          `Module "${moduleName}" is not allowed in handlers. Install/enable it via Settings → Packages`,
        );
      }

      for (const denied of NETWORK_DENYLIST_PACKAGES) {
        if (moduleName === denied || moduleName.startsWith(`${denied}/`)) {
          throw new Error(`Module "${moduleName}" is not allowed in handlers. Use $fetch instead`);
        }
      }

      try {
        return require(moduleName);
      } catch (e: any) {
        throw new Error(`Module "${moduleName}" failed to load: ${e?.message || 'unknown error'}`);
      }
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

    const sandbox: any = {
      $ctx: { ...ctx, $throw },
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
      parseInt,
      parseFloat,
      isNaN,
      isFinite,
      encodeURIComponent,
      decodeURIComponent,
      encodeURI,
      decodeURI,
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

      Object.assign(ctx, sandbox.$ctx);
      delete (ctx as any).$throw;

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
