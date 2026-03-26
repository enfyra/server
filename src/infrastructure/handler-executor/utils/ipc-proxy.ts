import { randomUUID } from 'crypto';
import { pendingCalls, addPendingCall } from '../worker';

function serializeBuffers(obj: any): any {
  if (Buffer.isBuffer(obj)) {
    return { type: 'Buffer', data: Array.from(obj) };
  }
  if (Array.isArray(obj)) {
    return obj.map(serializeBuffers);
  }
  if (obj && typeof obj === 'object' && obj.type === 'Buffer' && Array.isArray(obj.data)) {
    return obj;
  }
  if (obj && typeof obj === 'object' && obj.constructor === Object) {
    const keys = Object.keys(obj);
    const isNumericKeys = keys.length > 0 && keys.every(k => /^\d+$/.test(k));
    if (isNumericKeys) {
      const sortedKeys = keys.map(Number).sort((a, b) => a - b);
      return { type: 'Buffer', data: sortedKeys.map(k => obj[k]) };
    }
    const serialized: any = {};
    for (const key in obj) serialized[key] = serializeBuffers(obj[key]);
    return serialized;
  }
  return obj;
}

function waitForParentResponse(callId: string, path: string): Promise<any> {
  return new Promise((resolve, reject) => {
    addPendingCall(callId, resolve, reject, path);
  });
}

export function buildFunctionProxy(prefixPath: string): any {
  return new Proxy(async function () {}, {
    get(_, prop: string | symbol) {
      if (
        prop === 'toJSON' ||
        prop === 'inspect' ||
        prop === Symbol.toPrimitive ||
        prop === Symbol.toStringTag
      ) {
        return () => `[FunctionProxy: ${prefixPath}]`;
      }
      return buildFunctionProxy(`${prefixPath}.${String(prop)}`);
    },
    apply(_, __, args: any[]) {
      return (async () => {
        const callId = randomUUID();
        process.send?.({ type: 'call', callId, path: prefixPath, args: args.map(serializeBuffers) });
        return waitForParentResponse(callId, prefixPath);
      })();
    },
  });
}

export function buildCallableFunctionProxy(path: string) {
  return async (...args: any[]) => {
    const callId = randomUUID();
    process.send?.({ type: 'call', callId, path, args: args.map(serializeBuffers) });
    return waitForParentResponse(callId, path);
  };
}

export function buildResponseProxy(): any {
  const baseProxy = buildFunctionProxy('$res');
  return new Proxy(baseProxy, {
    get(target, prop: string | symbol) {
      if (prop === 'stream') {
        return async (streamOrIterable: any, options?: { mimetype?: string; filename?: string }) => {
          const callId = randomUUID();
          process.send?.({ type: 'stream_start', callId, options: options || {} });
          try {
            const { Readable } = require('stream');
            let readable: any;
            if (streamOrIterable?.pipe && typeof streamOrIterable.pipe === 'function') {
              readable = streamOrIterable;
            } else if (streamOrIterable?.[Symbol.asyncIterator]) {
              readable = Readable.from(streamOrIterable);
            } else {
              throw new Error('stream() requires a Readable stream or async iterable');
            }
            for await (const chunk of readable) {
              const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
              process.send?.({
                type: 'stream_chunk',
                callId,
                chunk: { type: 'Buffer', data: Array.from(buffer) },
              });
            }
            process.send?.({ type: 'stream_end', callId });
            await waitForParentResponse(callId, '$res.stream');
          } catch (error) {
            process.send?.({ type: 'stream_error', callId, error: { message: error.message, stack: error.stack } });
            throw error;
          }
        };
      }
      return target[prop];
    },
  });
}
