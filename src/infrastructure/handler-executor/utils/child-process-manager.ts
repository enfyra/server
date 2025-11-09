import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { resolvePath } from './resolve-path';
import { ErrorHandler } from './error-handler';
import { ScriptTimeoutException } from '../../../core/exceptions/custom-exceptions';
import { smartMergeContext } from './smart-merge';

export class ChildProcessManager {
  private static readonly logger = new Logger(ChildProcessManager.name);

  private static deserializeBuffers(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }

    // Handle serialized Buffer: { type: 'Buffer', data: [...] }
    if (obj && typeof obj === 'object' && obj.type === 'Buffer' && Array.isArray(obj.data)) {
      return Buffer.from(obj.data);
    }

    if (Array.isArray(obj)) {
      return obj.map(item => this.deserializeBuffers(item));
    }

    if (obj && typeof obj === 'object' && obj.constructor === Object) {
      const deserialized: any = {};
      for (const key in obj) {
        deserialized[key] = this.deserializeBuffers(obj[key]);
      }
      return deserialized;
    }

    return obj;
  }

  static setupTimeout(
    child: any,
    timeoutMs: number,
    code: string,
    isDone: { value: boolean },
    reject: (error: any) => void,
    pool: any,
  ): NodeJS.Timeout {
    return setTimeout(async () => {
      if (isDone.value) return;
      isDone.value = true;
      child.removeAllListeners();

      child.kill('SIGKILL');

      try {
        await pool.destroy(child);
      } catch (e) {
        this.logger.warn('Failed to destroy child on timeout', e);
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
    const activeStreams = new Map<string, { started: boolean }>();

    let stderrOutput = '';
    if (child.stderr) {
      child.stderr.on('data', (data: Buffer) => {
        stderrOutput += data.toString();
      });
    }

    child.on('message', async (msg: any) => {
      if (isDone.value) return;

      if (msg.type === 'stream_start') {
        const { callId, options } = msg;
        activeStreams.set(callId, { started: true });

        const res = ctx.$res;
        if (!res) {
          child.send({
            type: 'call_result',
            callId,
            error: true,
            errorResponse: { message: 'Response object not available' },
          });
          return;
        }

        if (options.mimetype) {
          res.setHeader('Content-Type', options.mimetype);
        }
        if (options.filename) {
          res.setHeader('Content-Disposition', `attachment; filename="${options.filename}"`);
        }

        return;
      }

      if (msg.type === 'stream_chunk') {
        const { callId, chunk } = msg;
        const streamInfo = activeStreams.get(callId);
        if (!streamInfo) return;

        const res = ctx.$res;
        if (!res) return;

        let buffer: Buffer;
        if (chunk && chunk.type === 'Buffer' && Array.isArray(chunk.data)) {
          buffer = Buffer.from(chunk.data);
        } else {
          buffer = Buffer.from(chunk);
        }

        res.write(buffer);
        return;
      }

      if (msg.type === 'stream_end') {
        const { callId } = msg;
        const streamInfo = activeStreams.get(callId);
        if (!streamInfo) return;

        const res = ctx.$res;
        if (res) {
          res.end();
        }

        activeStreams.delete(callId);

        if (!child.killed && child.connected) {
          child.send({
            type: 'call_result',
            callId,
            result: undefined,
          });
        }
        return;
      }

      if (msg.type === 'stream_error') {
        const { callId, error } = msg;
        activeStreams.delete(callId);

        const res = ctx.$res;
        if (res && !res.headersSent) {
          res.status(500).json({ error: error.message });
        }

        if (!child.killed && child.connected) {
          child.send({
            type: 'call_result',
            callId,
            error: true,
            errorResponse: error,
          });
        }
        return;
      }

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
            if (!child.killed && child.connected) {
              child.send({
                type: 'call_result',
                callId: msg.callId,
                error: true,
                errorResponse: {
                  message: `Helper function not found: ${msg.path}. Parent: ${JSON.stringify(Object.keys(parent || {}))}, Method: ${method}, Type: ${typeof parent?.[method]}`,
                  name: 'HelperNotFoundError',
                },
              });
            }
            return;
          }

          const reconstructedArgs = msg.args.map((arg: any) => {
            if (arg && typeof arg === 'object' && arg.type === 'Buffer' && Array.isArray(arg.data)) {
              return Buffer.from(arg.data);
            } else if (arg && typeof arg === 'object' && !Buffer.isBuffer(arg)) {
              const keys = Object.keys(arg);
              const numericKeys = keys.filter(k => /^\d+$/.test(k));
              if (numericKeys.length > 0) {
                const sortedKeys = numericKeys.map(k => parseInt(k, 10)).sort((a, b) => a - b);
                const arr = new Array(sortedKeys.length);
                for (let i = 0; i < sortedKeys.length; i++) {
                  arr[i] = arg[sortedKeys[i].toString()];
                }
                return Buffer.from(arr);
              }
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
        
        const deserializedData = this.deserializeBuffers(msg.data);
        resolve(deserializedData);
      }

      if (msg.type === 'error') {
        const simpleMessage = msg.error.message;

        const errorDetails: any = {
          type: msg.error.name || 'Error',
          message: msg.error.message,
          statusCode: msg.error.statusCode,
        };

        if (msg.error.errorLine && msg.error.codeContextArray) {
          errorDetails.location = {
            line: msg.error.errorLine,
          };
          errorDetails.code = msg.error.codeContextArray;
        }

        const error = ErrorHandler.createException(
          undefined,
          msg.error.statusCode,
          simpleMessage,
          code,
          errorDetails,
        );

        ErrorHandler.handleChildError(
          isDone,
          child,
          timeout,
          pool,
          error,
          'Handler Execution Error',
          simpleMessage,
          code,
          reject,
          errorDetails,
        );
      }
    });

    child.once('exit', async (exitCode: number, signal: string) => {
      if (isDone.value) return;

      let errorMessage = `Child process exited with code ${exitCode}, signal ${signal}`;
      const errorDetails: any = { exitCode, signal };

      if (stderrOutput) {
        const syntaxErrorMatch = stderrOutput.match(/SyntaxError: (.+)/);
        const referenceErrorMatch = stderrOutput.match(/ReferenceError: (.+)/);
        const typeErrorMatch = stderrOutput.match(/TypeError: (.+)/);

        if (syntaxErrorMatch) {
          errorMessage = syntaxErrorMatch[1];
          errorDetails.type = 'SyntaxError';
        } else if (referenceErrorMatch) {
          errorMessage = referenceErrorMatch[1];
          errorDetails.type = 'ReferenceError';
        } else if (typeErrorMatch) {
          errorMessage = typeErrorMatch[1];
          errorDetails.type = 'TypeError';
        }

        errorDetails.stderr = stderrOutput;
      }

      const error = ErrorHandler.createException(
        undefined,
        undefined,
        errorMessage,
        code,
        errorDetails,
      );

      ErrorHandler.handleChildError(
        isDone,
        child,
        timeout,
        pool,
        error,
        'Child Process Error',
        errorMessage,
        code,
        reject,
        errorDetails,
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
        isDone,
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
