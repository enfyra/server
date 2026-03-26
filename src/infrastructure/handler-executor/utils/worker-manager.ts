import { Worker } from 'worker_threads';
import { TDynamicContext } from '../../../shared/types';
import { ErrorHandler } from './error-handler';
import { ScriptTimeoutException } from '../../../core/exceptions/custom-exceptions';

const BLOCKED_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

function safePost(worker: Worker, data: any): void {
  worker.postMessage(JSON.parse(JSON.stringify(data, (_, v) => (typeof v === 'function' ? undefined : v))));
}

function deserializeBuffers(obj: any): any {
  if (obj === null || obj === undefined) return obj;
  if (obj && typeof obj === 'object' && obj.type === 'Buffer' && Array.isArray(obj.data)) {
    return Buffer.from(obj.data);
  }
  if (Array.isArray(obj)) return obj.map(item => deserializeBuffers(item));
  if (obj && typeof obj === 'object' && obj.constructor === Object) {
    const out: any = {};
    for (const key in obj) out[key] = deserializeBuffers(obj[key]);
    return out;
  }
  return obj;
}

function resolvePath(obj: any, path: string): { parent: any; method: string } {
  const parts = path.split('.');
  const method = parts.pop();
  if (BLOCKED_KEYS.has(method)) throw new Error(`Invalid path: ${path}`);
  let parent = obj;
  for (const part of parts) {
    if (BLOCKED_KEYS.has(part)) throw new Error(`Invalid path: ${path}`);
    if (parent && Object.prototype.hasOwnProperty.call(parent, part)) {
      parent = parent[part];
    } else {
      throw new Error(`Invalid path: ${path}`);
    }
  }
  return { parent, method };
}

function isPrimitive(value: any): boolean {
  return (
    value === null ||
    value === undefined ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'symbol'
  );
}

function containsFunctions(obj: any): boolean {
  if (typeof obj !== 'object' || obj === null) return false;
  for (const key in obj) {
    const value = obj[key];
    if (typeof value === 'function') return true;
    if (typeof value === 'object' && value !== null && containsFunctions(value)) return true;
  }
  return false;
}

function containsArrays(obj: any): boolean {
  if (typeof obj !== 'object' || obj === null) return false;
  for (const key in obj) {
    const value = obj[key];
    if (Array.isArray(value)) return true;
    if (typeof value === 'object' && value !== null && containsArrays(value)) return true;
  }
  return false;
}

function containsDates(obj: any): boolean {
  if (typeof obj !== 'object' || obj === null) return false;
  for (const key in obj) {
    const value = obj[key];
    if (value instanceof Date) return true;
    if (typeof value === 'object' && value !== null && containsDates(value)) return true;
  }
  return false;
}

function isMergeableProperty(value: any): boolean {
  return (
    value !== null &&
    value !== undefined &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    !(value instanceof Date) &&
    !(value instanceof Function) &&
    !containsFunctions(value) &&
    !containsArrays(value) &&
    !containsDates(value)
  );
}

function safeMerge(target: any, source: any): any {
  if (!source || typeof source !== 'object' || Array.isArray(source)) return target;
  const result: any = { ...target };
  for (const key of Object.keys(source)) {
    if (BLOCKED_KEYS.has(key)) continue;
    const srcVal = source[key];
    const tgtVal = result[key];
    if (
      srcVal && typeof srcVal === 'object' && !Array.isArray(srcVal) && !(srcVal instanceof Date) &&
      tgtVal && typeof tgtVal === 'object' && !Array.isArray(tgtVal) && !(tgtVal instanceof Date)
    ) {
      result[key] = safeMerge(tgtVal, srcVal);
    } else {
      result[key] = srcVal;
    }
  }
  return result;
}

function mergeContext(originalCtx: TDynamicContext, workerCtx: any): TDynamicContext {
  const merged = { ...originalCtx };
  const nonMergeable = ['$repos', '$logs', '$helpers', '$req', '$throw'];

  for (const key in workerCtx) {
    if (nonMergeable.includes(key)) continue;
    const value = workerCtx[key];
    if (key === '$body' || key === '$data') {
      merged[key] = safeMerge(merged[key] || {}, value);
    } else if (isPrimitive(value) && value !== null && value !== undefined) {
      merged[key] = value;
    } else if (isMergeableProperty(value)) {
      merged[key] = safeMerge(merged[key] || {}, value);
    }
  }

  return merged;
}

export class WorkerManager {
  static setupTimeout(
    worker: Worker,
    timeoutMs: number,
    code: string,
    isDone: { value: boolean },
    reject: (error: any) => void,
    terminateWorker: (worker: Worker) => void,
  ): NodeJS.Timeout {
    return setTimeout(() => {
      if (isDone.value) return;
      isDone.value = true;
      worker.removeAllListeners('message');
      worker.removeAllListeners('exit');
      worker.removeAllListeners('error');
      worker.stderr?.removeAllListeners?.('data');
      terminateWorker(worker);
      reject(new ScriptTimeoutException(timeoutMs, code));
    }, timeoutMs);
  }

  static setupListeners(
    worker: Worker,
    ctx: TDynamicContext,
    timeout: NodeJS.Timeout,
    isDone: { value: boolean },
    resolve: (value: any) => void,
    reject: (error: any) => void,
    code: string,
    returnWorker: (worker: Worker) => void,
    terminateWorker: (worker: Worker) => void,
  ): void {
    const activeStreams = new Map<string, { started: boolean }>();
    let stderrOutput = '';

    const releaseOnSuccess = () => {
      worker.removeAllListeners('message');
      worker.removeAllListeners('exit');
      worker.removeAllListeners('error');
      worker.stderr?.removeAllListeners?.('data');
      returnWorker(worker);
    };

    const releaseOnError = () => {
      worker.removeAllListeners('message');
      worker.removeAllListeners('exit');
      worker.removeAllListeners('error');
      worker.stderr?.removeAllListeners?.('data');
      terminateWorker(worker);
    };

    if (worker.stderr) {
      worker.stderr.removeAllListeners('data');
      worker.stderr.on('data', (data: Buffer) => {
        stderrOutput += data.toString();
      });
    }

    worker.on('message', async (msg: any) => {
      if (isDone.value) return;

      if (msg.type === 'stream_start') {
        const { callId, options } = msg;
        activeStreams.set(callId, { started: true });
        const res = ctx.$res;
        if (!res) {
          safePost(worker, { type: 'call_result', callId, error: true, errorResponse: { message: 'Response object not available' } });
          return;
        }
        if (options.mimetype) res.setHeader('Content-Type', options.mimetype);
        if (options.filename) res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(options.filename)}`);
        return;
      }

      if (msg.type === 'stream_chunk') {
        const { callId, chunk } = msg;
        if (!activeStreams.get(callId)) return;
        const res = ctx.$res;
        if (!res) return;
        res.write(chunk?.type === 'Buffer' && Array.isArray(chunk.data) ? Buffer.from(chunk.data) : Buffer.from(chunk));
        return;
      }

      if (msg.type === 'stream_end') {
        const { callId } = msg;
        if (!activeStreams.get(callId)) return;
        ctx.$res?.end();
        activeStreams.delete(callId);
        safePost(worker, { type: 'call_result', callId, result: undefined });
        return;
      }

      if (msg.type === 'stream_error') {
        const { callId, error } = msg;
        activeStreams.delete(callId);
        const res = ctx.$res;
        if (res && !res.headersSent) res.status(500).json({ error: error.message });
        safePost(worker, { type: 'call_result', callId, error: true, errorResponse: error });
        return;
      }

      if (msg.type === 'call') {
        if (msg.path.includes('$throw')) {
          reject(ErrorHandler.createException(msg.path, undefined, msg.args[0], code));
          return;
        }

        if (msg.path.startsWith('$socket.')) {
          const ALLOWED_SOCKET_METHODS = new Set(['emit', 'join', 'leave', 'to', 'close']);
          const method = msg.path.substring(8);
          if (!ALLOWED_SOCKET_METHODS.has(method)) {
            safePost(worker, { type: 'call_result', callId: msg.callId, error: true, errorResponse: { message: `Socket method not allowed: ${method}` } });
            return;
          }
          try {
            const socketProxy = ctx.$socket as any;
            if (socketProxy && typeof socketProxy[method] === 'function') {
              await socketProxy[method](...msg.args);
            }
            safePost(worker, { type: 'call_result', callId: msg.callId, result: undefined });
          } catch (err) {
            safePost(worker, { type: 'call_result', callId: msg.callId, error: true, errorResponse: { message: err.message } });
          }
          return;
        }

        try {
          const { parent, method } = resolvePath(ctx, msg.path);
          if (typeof parent[method] !== 'function') {
            safePost(worker, {
              type: 'call_result',
              callId: msg.callId,
              error: true,
              errorResponse: {
                message: `Helper function not found: ${msg.path}. Parent: ${JSON.stringify(Object.keys(parent || {}))}, Method: ${method}, Type: ${typeof parent?.[method]}`,
                name: 'HelperNotFoundError',
              },
            });
            return;
          }

          const reconstructedArgs = msg.args.map((arg: any) => {
            if (arg && typeof arg === 'object' && arg.type === 'Buffer' && Array.isArray(arg.data)) {
              return Buffer.from(arg.data);
            }
            if (arg && typeof arg === 'object' && !Buffer.isBuffer(arg)) {
              const keys = Object.keys(arg);
              const numericKeys = keys.filter(k => /^\d+$/.test(k));
              if (numericKeys.length > 0) {
                const sortedKeys = numericKeys.map(k => parseInt(k, 10)).sort((a, b) => a - b);
                const arr = new Array(sortedKeys.length);
                for (let i = 0; i < sortedKeys.length; i++) arr[i] = arg[sortedKeys[i].toString()];
                return Buffer.from(arr);
              }
            }
            return arg;
          });

          const result = await parent[method](...reconstructedArgs);
          safePost(worker, { type: 'call_result', callId: msg.callId, result: msg.path.startsWith('$res.') ? undefined : result });
        } catch (err) {
          safePost(worker, {
            type: 'call_result',
            callId: msg.callId,
            error: true,
            errorResponse: { message: err.message || 'Unknown error', name: err.name || 'Error', statusCode: err.statusCode, response: err.response },
          });
        }
        return;
      }

      if (msg.type === 'done') {
        isDone.value = true;
        if (msg.ctx) Object.assign(ctx, mergeContext(ctx, msg.ctx));
        clearTimeout(timeout);
        releaseOnSuccess();
        resolve(deserializeBuffers(msg.data));
        return;
      }

      if (msg.type === 'error') {
        const errorDetails: any = { type: msg.error.name || 'Error', message: msg.error.message, statusCode: msg.error.statusCode };
        if (msg.error.errorLine && msg.error.codeContextArray) {
          errorDetails.location = { line: msg.error.errorLine };
          errorDetails.code = msg.error.codeContextArray;
        }
        isDone.value = true;
        clearTimeout(timeout);
        ErrorHandler.logError('Handler Execution Error', msg.error.message, code, errorDetails);
        releaseOnError();
        reject(ErrorHandler.createException(undefined, msg.error.statusCode, msg.error.message, code, errorDetails));
      }
    });

    worker.once('exit', (exitCode: number) => {
      if (isDone.value) return;
      let errorMessage = `Worker exited with code ${exitCode}`;
      const errorDetails: any = { exitCode };

      if (stderrOutput) {
        const syntaxMatch = stderrOutput.match(/SyntaxError: (.+)/);
        const referenceMatch = stderrOutput.match(/ReferenceError: (.+)/);
        const typeMatch = stderrOutput.match(/TypeError: (.+)/);
        if (syntaxMatch) { errorMessage = syntaxMatch[1]; errorDetails.type = 'SyntaxError'; }
        else if (referenceMatch) { errorMessage = referenceMatch[1]; errorDetails.type = 'ReferenceError'; }
        else if (typeMatch) { errorMessage = typeMatch[1]; errorDetails.type = 'TypeError'; }
        errorDetails.stderr = stderrOutput;
      }

      isDone.value = true;
      clearTimeout(timeout);
      ErrorHandler.logError('Worker Exit Error', errorMessage, code, errorDetails);
      releaseOnError();
      reject(ErrorHandler.createException(undefined, undefined, errorMessage, code, errorDetails));
    });

    worker.once('error', (err: any) => {
      if (isDone.value) return;
      const errorMessage = `Worker error: ${err?.message || err}`;
      isDone.value = true;
      clearTimeout(timeout);
      ErrorHandler.logError('Worker Error', errorMessage, code, { originalError: err });
      releaseOnError();
      reject(ErrorHandler.createException(undefined, undefined, errorMessage, code, { originalError: err }));
    });
  }

  static sendExecute(worker: Worker, ctx: TDynamicContext, code: string, packages: string[]): void {
    safePost(worker, { type: 'execute', ctx, code, packages });
  }
}
