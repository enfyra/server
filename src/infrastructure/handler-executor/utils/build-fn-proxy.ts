import { pendingCalls } from '../runner';

function serializeBuffers(obj: any): any {
  if (Buffer.isBuffer(obj)) {
    return {
      type: 'Buffer',
      data: Array.from(obj),
    };
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
      const data = sortedKeys.map(k => obj[k]);
      return {
        type: 'Buffer',
        data,
      };
    }

    const serialized: any = {};
    for (const key in obj) {
      serialized[key] = serializeBuffers(obj[key]);
    }
    return serialized;
  }

  return obj;
}

let callCounter = 1;
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

      const newPath = `${prefixPath}.${String(prop)}`;
      return buildFunctionProxy(newPath);
    },

    apply(_, __, args: any[]) {
      return (async () => {
        const callId = `call_${++callCounter}`;
        const serializedArgs = args.map(serializeBuffers);

        process.send?.({
          type: 'call',
          callId,
          path: prefixPath,
          args: serializedArgs,
        });
        const result = await waitForParentResponse(callId, prefixPath);
        return result;
      })();
    },
  });
}

export function buildCallableFunctionProxy(path: string) {
  return async (...args: any[]) => {
    const callId = `call_${++callCounter}`;
    const serializedArgs = args.map(serializeBuffers);

    process.send?.({
      type: 'call',
      callId,
      path,
      args: serializedArgs,
    });
    const result = await waitForParentResponse(callId, path);
    return result;
  };
}

function waitForParentResponse(callId: string, path: string): Promise<any> {
  return new Promise((resolve, reject) => {
    pendingCalls.set(callId, {
      resolve: (value: any) => {
        pendingCalls.delete(callId);
        resolve(value);
      },
      reject: (error: any) => {
        pendingCalls.delete(callId);
        reject(error);
      },
    });
  });
}

export function buildResponseProxy(): any {
  const baseProxy = buildFunctionProxy('$res');

  return new Proxy(baseProxy, {
    get(target, prop: string | symbol) {
      if (prop === 'stream') {
        return async (streamOrIterable: any, options?: { mimetype?: string; filename?: string }) => {
          const callId = `stream_${++callCounter}`;

          // Send stream start message with options
          process.send?.({
            type: 'stream_start',
            callId,
            options: options || {},
          });

          try {
            // Handle different stream types
            const { Readable } = require('stream');

            let readable: any;
            if (streamOrIterable?.pipe && typeof streamOrIterable.pipe === 'function') {
              // Already a readable stream
              readable = streamOrIterable;
            } else if (streamOrIterable?.[Symbol.asyncIterator]) {
              // Async iterable - convert to stream
              readable = Readable.from(streamOrIterable);
            } else {
              throw new Error('stream() requires a Readable stream or async iterable');
            }

            // Read and send chunks
            for await (const chunk of readable) {
              const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
              const serializedChunk = {
                type: 'Buffer',
                data: Array.from(buffer),
              };

              process.send?.({
                type: 'stream_chunk',
                callId,
                chunk: serializedChunk,
              });
            }

            // Send stream end
            process.send?.({
              type: 'stream_end',
              callId,
            });

            // Wait for confirmation
            await waitForParentResponse(callId, '$res.stream');
          } catch (error) {
            // Send stream error
            process.send?.({
              type: 'stream_error',
              callId,
              error: {
                message: error.message,
                stack: error.stack,
              },
            });
            throw error;
          }
        };
      }

      // Delegate other properties to base proxy
      return target[prop];
    },
  });
}
