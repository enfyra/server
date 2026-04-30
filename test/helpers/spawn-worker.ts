import { Worker } from 'worker_threads';

const WORKER_SCRIPT = require.resolve('@enfyra/kernel/execution/worker.js');

export interface CodeBlock {
  code: string;
  type: 'preHook' | 'handler' | 'postHook';
}

function encodeMainThreadToIsolate(value: unknown): string {
  if (value === undefined) return JSON.stringify({ __e: 'u' });
  try {
    return JSON.stringify({ __e: 'v', d: value }, (_k, v) =>
      typeof v === 'bigint' ? v.toString() : v,
    );
  } catch {
    return JSON.stringify({ __e: 'v', d: { __serializationError: true } });
  }
}

export function executeBatch(opts: {
  codeBlocks: CodeBlock[];
  pkgSources?: any[];
  snapshot: Record<string, any>;
  timeoutMs?: number;
  memoryLimitMb?: number;
  isolatePoolSize?: number;
  ctx?: Record<string, any>;
}): Promise<any> {
  return spawnWorker(
    'executeBatch',
    {
      codeBlocks: opts.codeBlocks,
      pkgSources: opts.pkgSources ?? [],
      snapshot: opts.snapshot,
      timeoutMs: opts.timeoutMs ?? 10000,
      memoryLimitMb: opts.memoryLimitMb ?? 128,
      isolatePoolSize: opts.isolatePoolSize,
    },
    opts.ctx ?? {},
    opts.timeoutMs ?? 10000,
  );
}

export function executeSingle(opts: {
  code: string;
  pkgSources?: any[];
  snapshot: Record<string, any>;
  timeoutMs?: number;
  memoryLimitMb?: number;
  isolatePoolSize?: number;
  ctx?: Record<string, any>;
}): Promise<any> {
  return spawnWorker(
    'execute',
    {
      code: opts.code,
      pkgSources: opts.pkgSources ?? [],
      snapshot: opts.snapshot,
      timeoutMs: opts.timeoutMs ?? 10000,
      memoryLimitMb: opts.memoryLimitMb ?? 128,
      isolatePoolSize: opts.isolatePoolSize,
    },
    opts.ctx ?? {},
    opts.timeoutMs ?? 10000,
  );
}

export function executeBatchSequence(
  requests: Array<{
    codeBlocks: CodeBlock[];
    snapshot: Record<string, any>;
    pkgSources?: any[];
    timeoutMs?: number;
    memoryLimitMb?: number;
    isolatePoolSize?: number;
  }>,
): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT);
    const results: any[] = [];
    let index = 0;
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const cleanup = () => {
      if (timer) clearTimeout(timer);
      worker.terminate();
    };

    const rejectOnce = (error: Error) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    };

    const sendNext = () => {
      if (index >= requests.length) {
        settled = true;
        cleanup();
        resolve(results);
        return;
      }
      if (timer) clearTimeout(timer);
      const req = requests[index];
      const timeoutMs = req.timeoutMs ?? 10000;
      timer = setTimeout(() => {
        rejectOnce(new Error(`Script execution timed out after ${timeoutMs}ms`));
      }, timeoutMs + 5000);
      worker.postMessage({
        type: 'executeBatch',
        id: `seq_${index}`,
        codeBlocks: req.codeBlocks,
        pkgSources: req.pkgSources ?? [],
        snapshot: req.snapshot,
        timeoutMs,
        memoryLimitMb: req.memoryLimitMb ?? 128,
        isolatePoolSize: req.isolatePoolSize ?? 1,
      });
    };

    worker.on('message', (msg) => {
      if (msg.type !== 'result') return;
      if (timer) clearTimeout(timer);
      if (msg.success) {
        const res: any = {
          value: msg.value,
          valueAbsent: msg.valueAbsent === true,
          ctxChanges: msg.ctxChanges,
        };
        if (msg.shortCircuit) res.shortCircuit = true;
        results.push(res);
        index++;
        sendNext();
      } else {
        const err: any = new Error(
          msg.error?.message || 'Handler execution failed',
        );
        err.statusCode = msg.error?.statusCode;
        err.code = msg.error?.code;
        err.details = msg.error?.details;
        if (msg.error?.stack) err.stack = msg.error.stack;
        rejectOnce(err);
      }
    });

    worker.on('error', rejectOnce);
    worker.on('exit', (code) => {
      if (settled) return;
      rejectOnce(new Error(`Worker exited unexpectedly with code ${code}`));
    });

    sendNext();
  });
}

function spawnWorker(
  messageType: string,
  payload: Record<string, any>,
  ctx: Record<string, any>,
  timeoutMs: number,
): Promise<any> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT);
    const id = `test_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      worker.terminate();
      const err: any = new Error(
        `Script execution timed out after ${timeoutMs}ms`,
      );
      err.code = 'ERR_SCRIPT_EXECUTION_TIMEOUT';
      err.isTimeout = true;
      reject(err);
    }, timeoutMs + 5000);

    const cleanup = () => {
      clearTimeout(timer);
      worker.terminate();
    };

    worker.on('message', async (msg) => {
      if (msg.type === 'result') {
        if (settled) return;
        settled = true;
        cleanup();
        if (msg.success) {
          const res: any = {
            value: msg.value,
            valueAbsent: msg.valueAbsent === true,
            ctxChanges: msg.ctxChanges,
          };
          if (msg.shortCircuit) res.shortCircuit = true;
          resolve(res);
        } else {
          const err: any = new Error(
            msg.error?.message || 'Handler execution failed',
          );
          err.statusCode = msg.error?.statusCode;
          err.code = msg.error?.code;
          err.details = msg.error?.details;
          if (msg.error?.stack) err.stack = msg.error.stack;
          if (msg.ctxChanges) err.ctxChanges = msg.ctxChanges;
          reject(err);
        }
      } else if (msg.type === 'repoCall') {
        try {
          const args = JSON.parse(msg.argsJson);
          const repo = ctx?.$repos?.[msg.table];
          if (!repo || typeof repo[msg.method] !== 'function')
            throw new Error(`Repo not found: ${msg.table}.${msg.method}`);
          const result = await repo[msg.method](...args);
          worker.postMessage({
            type: 'callResult',
            callId: msg.callId,
            result: encodeMainThreadToIsolate(result),
          });
        } catch (e: any) {
          worker.postMessage({
            type: 'callError',
            callId: msg.callId,
            error: e.message,
          });
        }
      } else if (msg.type === 'helpersCall') {
        try {
          const args = JSON.parse(msg.argsJson);
          const parts = msg.name.split('.');
          let fn: any = ctx?.$helpers;
          for (const key of parts) fn = fn?.[key];
          if (typeof fn !== 'function')
            throw new Error(`Helper not found: ${msg.name}`);
          const result = await fn(...args);
          worker.postMessage({
            type: 'callResult',
            callId: msg.callId,
            result: encodeMainThreadToIsolate(result),
          });
        } catch (e: any) {
          worker.postMessage({
            type: 'callError',
            callId: msg.callId,
            error: e.message,
          });
        }
      } else if (msg.type === 'socketCall') {
        try {
          const args = JSON.parse(msg.argsJson);
          const fn = ctx?.$socket?.[msg.method];
          if (typeof fn === 'function') fn(...args);
        } catch {}
      } else if (msg.type === 'cacheCall') {
        try {
          const args = JSON.parse(msg.argsJson);
          const fn = ctx?.$cache?.[msg.method];
          if (typeof fn !== 'function')
            throw new Error(`Cache not found: ${msg.method}`);
          const result = await fn(...args);
          worker.postMessage({
            type: 'callResult',
            callId: msg.callId,
            result: encodeMainThreadToIsolate(result),
          });
        } catch (e: any) {
          worker.postMessage({
            type: 'callError',
            callId: msg.callId,
            error: e.message,
          });
        }
      } else if (msg.type === 'dispatchCall') {
        try {
          const args = JSON.parse(msg.argsJson);
          const fn = ctx?.$dispatch?.[msg.method];
          if (typeof fn !== 'function')
            throw new Error(`$dispatch.${msg.method} not available`);
          const result = await fn(...args);
          worker.postMessage({
            type: 'callResult',
            callId: msg.callId,
            result: encodeMainThreadToIsolate(result),
          });
        } catch (e: any) {
          worker.postMessage({
            type: 'callError',
            callId: msg.callId,
            error: e.message,
          });
        }
      }
    });

    worker.on('error', (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    });

    worker.on('exit', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`Worker exited unexpectedly with code ${code}`));
    });

    worker.postMessage({ type: messageType, id, ...payload });
  });
}
