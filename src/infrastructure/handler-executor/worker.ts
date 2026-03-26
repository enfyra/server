import { parentPort, isMainThread } from 'worker_threads';
import {
  buildCallableFunctionProxy,
  buildFunctionProxy,
  buildResponseProxy,
} from './utils/ipc-proxy';

if (isMainThread) {
  throw new Error('Must be run as a Worker thread');
}

(process as any).send = (data: any): void => {
  const msg = JSON.parse(JSON.stringify(data, (_, v) => (typeof v === 'function' ? undefined : v)));
  parentPort!.postMessage(msg);
};


interface PendingCall {
  resolve: (value: any) => void;
  reject: (error: any) => void;
  timeoutId: NodeJS.Timeout;
  createdAt: number;
  methodName: string;
}

export const pendingCalls = new Map<string, PendingCall>();

const PENDING_CALL_TIMEOUT = 30000;
const PENDING_CALL_MAX_AGE = 60000;

export function addPendingCall(callId: string, resolve: (value: any) => void, reject: (error: any) => void, methodName: string): void {
  const timeoutId = setTimeout(() => {
    if (pendingCalls.has(callId)) {
      pendingCalls.delete(callId);
      reject(new Error(`Call ${methodName} (${callId}) timed out after ${PENDING_CALL_TIMEOUT}ms`));
    }
  }, PENDING_CALL_TIMEOUT);

  pendingCalls.set(callId, {
    resolve,
    reject,
    timeoutId,
    createdAt: Date.now(),
    methodName,
  });
}

export function resolvePendingCall(callId: string, result: any, error?: any): void {
  const pending = pendingCalls.get(callId);
  if (pending) {
    clearTimeout(pending.timeoutId);
    pendingCalls.delete(callId);
    if (error) {
      pending.reject(error);
    } else {
      pending.resolve(result);
    }
  }
}

function cleanupStalePendingCalls(): void {
  const now = Date.now();
  const staleCallIds: string[] = [];

  for (const [callId, pending] of pendingCalls) {
    if (now - pending.createdAt > PENDING_CALL_MAX_AGE) {
      staleCallIds.push(callId);
    }
  }

  for (const callId of staleCallIds) {
    const pending = pendingCalls.get(callId);
    if (pending) {
      clearTimeout(pending.timeoutId);
      pending.reject(new Error(`Call ${pending.methodName} expired after ${PENDING_CALL_MAX_AGE}ms`));
      pendingCalls.delete(callId);
    }
  }
}

setInterval(cleanupStalePendingCalls, 10000);

function clearAllPendingCalls(reason: string): void {
  for (const [, pending] of pendingCalls) {
    clearTimeout(pending.timeoutId);
    pending.reject(new Error(`Process exiting: ${reason}`));
  }
  pendingCalls.clear();
}

process.on('exit', () => {
  clearAllPendingCalls('process_exit');
});

process.on('unhandledRejection', (reason: any) => {
  console.warn('[Fire-and-forget] Unhandled proxy call rejection:', reason?.message || reason);
});

process.on('uncaughtException', (error: any) => {
  try {
    let errorMessage = error.message ?? String(error);
    let errorName = error.name ?? 'UncaughtException';
    let statusCode = undefined;

    if (error.errorResponse?.message) {
      errorMessage = error.errorResponse.message;
    } else if (error.response?.message) {
      errorMessage = error.response.message;
    } else if (typeof error.response === 'string') {
      errorMessage = error.response;
    }

    if (error.errorResponse?.name) {
      errorName = error.errorResponse.name;
    } else if (error.response?.name) {
      errorName = error.response.name;
    }

    if (error.errorResponse?.statusCode) {
      statusCode = error.errorResponse.statusCode;
    } else if (error.response?.statusCode) {
      statusCode = error.response.statusCode;
    } else if (error.statusCode) {
      statusCode = error.statusCode;
    }

    process.send?.({
      type: 'error',
      error: {
        message: errorMessage,
        stack: error.stack,
        name: errorName,
        statusCode: statusCode,
      },
    });
  } catch (sendError) {
    console.error('Failed to send error:', sendError);
  }

  setTimeout(() => process.exit(1), 100);
});

parentPort!.on('message', async (msg: any) => {
  if (msg.type === 'get_memory_usage') {
    const memory = process.memoryUsage();
    const memoryMB = Math.round(memory.heapUsed / 1024 / 1024);
    process.send?.({
      type: 'memory_usage_response',
      memoryMB,
      memoryDetails: {
        heapUsed: memory.heapUsed,
        heapTotal: memory.heapTotal,
        external: memory.external,
        rss: memory.rss,
      },
    });
    return;
  }

  if (msg.type === 'call_result') {
    const { callId, result, error, ...others } = msg;
    if (error) {
      resolvePendingCall(callId, null, { ...error, ...others });
    } else {
      resolvePendingCall(callId, result);
    }
  }

  if (msg.type === 'execute') {
    const originalRepos = msg.ctx.$repos || {};
    const packages = msg.packages;

    const ctx = msg.ctx;
    ctx.$repos = {};

    ctx.$pkgs = {};
    for (const packageName of packages) {
      try {
        ctx.$pkgs[packageName] = require(packageName);
      } catch (error) {
        console.warn(`Failed to require package "${packageName}":`, error.message);
      }
    }

    for (const serviceName of Object.keys(originalRepos)) {
      ctx.$repos[serviceName] = buildFunctionProxy(`$repos.${serviceName}`);
    }
    ctx.$throw = buildFunctionProxy('$throw');
    ctx.$helpers = buildFunctionProxy('$helpers');
    ctx.$logs = buildCallableFunctionProxy('$logs');
    ctx.$cache = buildFunctionProxy('$cache');
    ctx.$socket = buildFunctionProxy('$socket');

    if (ctx.$res) {
      ctx.$res = buildResponseProxy();
    }

    if (ctx.$uploadedFile?.buffer) {
      const bufData = ctx.$uploadedFile.buffer;
      let bufferArray: number[] = null;

      if (Buffer.isBuffer(bufData)) {
        bufferArray = Array.from(bufData);
      } else if (bufData && typeof bufData === 'object') {
        if (bufData.type === 'Buffer' && Array.isArray(bufData.data)) {
          bufferArray = bufData.data;
        } else {
          const keys = Object.keys(bufData);
          const numericKeys = keys.filter(k => /^\d+$/.test(k));
          if (numericKeys.length > 0) {
            const sortedKeys = numericKeys.map(k => parseInt(k, 10)).sort((a, b) => a - b);
            bufferArray = new Array(sortedKeys.length);
            for (let i = 0; i < sortedKeys.length; i++) {
              bufferArray[i] = bufData[sortedKeys[i].toString()];
            }
          }
        }
      }

      if (bufferArray) {
        ctx.$uploadedFile.buffer = {
          type: 'Buffer',
          data: bufferArray,
          toBuffer: () => Buffer.from(bufferArray),
          valueOf: () => Buffer.from(bufferArray),
        };
      }
    }

    const processedCode = msg.code;

    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
    const wrappedCode = `"use strict";\nreturn (async () => {\nprocess.env = {};\n${processedCode}\n})();`;

    try {
      const asyncFn = new AsyncFunction('$ctx', wrappedCode);
      const result = await asyncFn(ctx);

      process.send?.({
        type: 'done',
        data: result,
        ctx,
      });
    } catch (error) {
      let errorLine = null;
      let codeContext = '';
      let codeContextArray: string[] = [];

      try {
        const stackMatch = error.stack?.match(/<anonymous>:(\d+)/);
        if (stackMatch) {
          const transformedLine = parseInt(stackMatch[1]);
          const wrapperLinesBefore = 4;
          errorLine = transformedLine - wrapperLinesBefore;

          if (errorLine > 0) {
            const originalLines = msg.code.split('\n');
            const startLine = Math.max(0, errorLine - 2);
            const endLine = Math.min(originalLines.length, errorLine + 3);

            codeContextArray = originalLines
              .slice(startLine, endLine)
              .map((line: string, idx: number) => {
                const lineNum = startLine + idx + 1;
                const marker = lineNum === errorLine ? '>' : ' ';
                return `${marker} ${lineNum}. ${line}`;
              });

            codeContext = originalLines
              .slice(startLine, endLine)
              .map((line: string, idx: number) => {
                const lineNum = startLine + idx + 1;
                const marker = lineNum === errorLine ? '❯' : ' ';
                const padding = String(lineNum).padStart(4);
                return `${marker} ${padding} | ${line}`;
              })
              .join('\n');
          }
        }
      } catch (parseError) {
      }

      console.error('\n╭─────────────────────────────────────────╮');
      console.error('│  ❌ Handler Execution Error             │');
      console.error('╰─────────────────────────────────────────╯');
      console.error('');
      console.error(`💥 ${error.name || 'Error'}: ${error.message}`);

      if (errorLine) {
        console.error('');
        console.error(`📍 Error at line ${errorLine}`);
        console.error('');
        console.error(codeContext);
      } else {
        console.error('');
        console.error('📝 Code snippet:');
        console.error(msg.code.split('\n').slice(0, 10).map((line: string, idx: number) =>
          `    ${String(idx + 1).padStart(4)} | ${line}`
        ).join('\n'));
      }

      console.error('');
      console.error('📚 Stack trace:');
      console.error(error.stack);
      console.error('');

      const errorMessage = error.errorResponse?.message ?? error.message ?? 'Unknown error';
      const errorName = error.errorResponse?.name ?? error.name;
      const statusCode = error.errorResponse?.statusCode ?? error.statusCode;

      process.send?.({
        type: 'error',
        error: {
          message: errorMessage,
          name: errorName,
          statusCode: statusCode,
          errorLine: errorLine,
          codeContextArray: codeContextArray,
          codeContext: codeContext,
          stack: error.stack,
        },
      });
    }
  }
});
