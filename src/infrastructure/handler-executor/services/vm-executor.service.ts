import { Injectable, Logger } from '@nestjs/common';
import * as vm from 'vm';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { ErrorHandler } from '../utils/error-handler';
import { ScriptTimeoutException, ScriptExecutionException } from '../../../core/exceptions/custom-exceptions';

@Injectable()
export class VmExecutorService {
  private readonly logger = new Logger(VmExecutorService.name);

  constructor(private packageCacheService: PackageCacheService) {}

  async run(code: string, ctx: TDynamicContext, timeoutMs: number): Promise<any> {
    const packages = await this.packageCacheService.getPackages();

    const pkgs: Record<string, any> = {};
    for (const packageName of packages) {
      try {
        pkgs[packageName] = require(packageName);
      } catch {
      }
    }
    (ctx as any).$pkgs = pkgs;

    const $throw = this.buildThrowProxy(code);

    const sandbox = {
      $ctx: { ...ctx, $throw },
      console,
      Buffer,
      setTimeout,
      setInterval,
      clearTimeout,
      clearInterval,
      Date,
      JSON,
      Math,
      RegExp,
      Array,
      Object,
      String,
      Number,
      Boolean,
      Error,
      TypeError,
      RangeError,
      Promise,
      Map,
      Set,
      WeakMap,
      WeakSet,
      Symbol,
      parseInt,
      parseFloat,
      isNaN,
      isFinite,
      encodeURIComponent,
      decodeURIComponent,
      encodeURI,
      decodeURI,
      require,
    };

    const vmContext = vm.createContext(sandbox);

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
