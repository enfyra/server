import { pendingCalls } from '../runner';

let callCounter = 1;
export function buildFunctionProxy(prefixPath: string): any {
  return new Proxy(async function () {}, {
    get(_, prop: string | symbol) {
      // Skip special properties during debug/log
      if (
        prop === 'toJSON' ||
        prop === 'inspect' ||
        prop === Symbol.toPrimitive ||
        prop === Symbol.toStringTag
      ) {
        return () => `[FunctionProxy: ${prefixPath}]`;
      }

      // Allow nested calls like $helpers.$bcrypt.hash
      const newPath = `${prefixPath}.${String(prop)}`;
      return buildFunctionProxy(newPath);
    },

    apply(_, __, args: any[]) {
      return (async () => {
        const callId = `call_${++callCounter}`;
        process.send?.({
          type: 'call',
          callId,
          path: prefixPath,
          args,
        });
        return await waitForParentResponse(callId);
      })();
    },
  });
}

export function buildCallableFunctionProxy(path: string) {
  return async (...args: any[]) => {
    const callId = `call_${++callCounter}`;
    process.send?.({
      type: 'call',
      callId,
      path,
      args,
    });
    return await waitForParentResponse(callId);
  };
}

/**
 * Wait for response from parent process
 */
function waitForParentResponse(callId: string): Promise<any> {
  return new Promise((resolve, reject) => {
    pendingCalls.set(callId, { resolve, reject });
  });
}
