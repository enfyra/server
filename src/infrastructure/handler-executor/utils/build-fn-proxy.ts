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

function waitForParentResponse(callId: string, path: string, timeoutMs: number = 10000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      pendingCalls.delete(callId);
      reject(new Error(`IPC call timeout: ${path} did not respond within ${timeoutMs}ms`));
    }, timeoutMs);

    pendingCalls.set(callId, {
      resolve: (value: any) => {
        clearTimeout(timeout);
        resolve(value);
      },
      reject: (error: any) => {
        clearTimeout(timeout);
        reject(error);
      },
    });
  });
}
