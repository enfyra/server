import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { resolvePath } from './resolve-path';
import { ErrorHandler } from './error-handler';
import { ScriptTimeoutException } from '../../../core/exceptions/custom-exceptions';
import { smartMergeContext } from './smart-merge';

export class ChildProcessManager {
  private static readonly logger = new Logger(ChildProcessManager.name);

  static setupTimeout(
    child: any,
    timeoutMs: number,
    code: string,
    isDone: { value: boolean },
    reject: (error: any) => void,
  ): NodeJS.Timeout {
    return setTimeout(async () => {
      if (isDone.value) return;
      isDone.value = true;
      child.removeAllListeners();
      try {
        await child.kill();
      } catch (e) {
        this.logger.warn('Failed to kill child on timeout', e);
      }
      reject(new ScriptTimeoutException(timeoutMs, code));
    }, timeoutMs);
  }

  static setupChildProcessListeners(
    child: any,
    ctx: TDynamicContext,
    timeout: NodeJS.Timeout,
    pool: any,
    isDone: { value: boolean },
    resolve: (value: any) => void,
    reject: (error: any) => void,
    code: string,
  ): void {
    child.on('message', async (msg: any) => {
      if (isDone.value) return;

      if (msg.type === 'call') {
        if (msg.path.includes('$throw')) {
          const error = ErrorHandler.createException(
            msg.path,
            undefined,
            msg.args[0],
            code,
          );
          reject(error);
        }
        try {
          const { parent, method } = resolvePath(ctx, msg.path);

          if (typeof parent[method] !== 'function') {
            return;
          }

          const reconstructedArgs = msg.args.map((arg: any) => {
            if (arg && typeof arg === 'object' && arg.type === 'Buffer' && Array.isArray(arg.data)) {
              return Buffer.from(arg.data);
            }
            return arg;
          });

          const result = await parent[method](...reconstructedArgs);

          let safeResult = result;
          if (msg.path.startsWith('$res.')) {
            safeResult = undefined;
          }

          if (!child.killed && child.connected) {
            child.send({
              type: 'call_result',
              callId: msg.callId,
              result: safeResult,
            });
          }
        } catch (err) {
          if (!child.killed && child.connected) {
            child.send({
              type: 'call_result',
              callId: msg.callId,
              error: true,
              errorResponse: err.response,
            });
          }
        }
      }

      if (msg.type === 'done') {
        isDone.value = true;
        child.removeAllListeners();

        if (msg.ctx) {
          const mergedCtx = smartMergeContext(ctx, msg.ctx);
          Object.assign(ctx, mergedCtx);
        }

        clearTimeout(timeout);
        await pool.release(child);
        resolve(msg.data);
      }

      if (msg.type === 'error') {
        const error = ErrorHandler.createException(
          undefined,
          msg.error.statusCode,
          msg.error.message,
          code,
          {
            statusCode: msg.error.statusCode,
            stack: msg.error.stack,
          },
        );

        ErrorHandler.handleChildError(
          isDone.value,
          child,
          timeout,
          pool,
          error,
          'Child Process Error',
          msg.error.message,
          code,
          reject,
          {
            statusCode: msg.error.statusCode,
            stack: msg.error.stack,
          },
        );
      }
    });

    child.once('exit', async (exitCode: number, signal: string) => {
      const error = ErrorHandler.createException(
        undefined,
        undefined,
        `Child process exited with code ${exitCode}, signal ${signal}`,
        code,
        { exitCode, signal },
      );

      ErrorHandler.handleChildError(
        isDone.value,
        child,
        timeout,
        pool,
        error,
        'Child Process Exit',
        `Child process exited with code ${exitCode}, signal ${signal}`,
        code,
        reject,
        { exitCode, signal },
      );
    });

    child.once('error', async (err: any) => {
      const error = ErrorHandler.createException(
        undefined,
        undefined,
        `Child process error: ${err?.message || err}`,
        code,
        { originalError: err },
      );

      ErrorHandler.handleChildError(
        isDone.value,
        child,
        timeout,
        pool,
        error,
        'Child Process Error',
        err?.message || err,
        code,
        reject,
        { originalError: err },
      );
    });
  }

  static sendExecuteMessage(
    child: any,
    ctx: TDynamicContext,
    code: string,
    packages: string[],
  ): void {
    child.send({
      type: 'execute',
      ctx: ctx,
      code,
      packages,
    });
  }
}
