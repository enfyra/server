import { Worker } from 'worker_threads';

const WORKER_SCRIPT =
  process.env.ENFYRA_KERNEL_WORKER_SCRIPT ||
  require.resolve('@enfyra/kernel/execution/worker.js');

export interface CodeBlock {
  code: string;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
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

function isWorkerEnvelope(
  value: Record<string, unknown>,
  requiredKey: string,
  allowedKeys: string[],
): boolean {
  if (!Object.prototype.hasOwnProperty.call(value, requiredKey)) return false;
  const allowed = new Set(allowedKeys);
  return Object.keys(value).every((key) => allowed.has(key));
}

function defineWorkerValue(
  target: Record<string, unknown>,
  key: string,
  value: unknown,
): void {
  Object.defineProperty(target, key, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}

function decodeWorkerValue(value: unknown): unknown {
  if (!value || typeof value !== 'object') return value;
  const record = value as Record<string, any>;
  if (isWorkerEnvelope(record, '__plainObject', ['__plainObject'])) {
    const output: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(record.__plainObject || {})) {
      defineWorkerValue(output, key, decodeWorkerValue(item));
    }
    return output;
  }
  if (isWorkerEnvelope(record, '__nullPrototype', ['__nullPrototype'])) {
    const output = Object.create(null) as Record<string, unknown>;
    for (const [key, item] of Object.entries(record.__nullPrototype || {})) {
      defineWorkerValue(output, key, decodeWorkerValue(item));
    }
    return output;
  }
  if (isWorkerEnvelope(record, '__e', ['__e']) && record.__e === 'u') {
    return undefined;
  }
  if (isWorkerEnvelope(record, '__number', ['__number'])) {
    if (record.__number === 'nan') return Number.NaN;
    if (record.__number === 'infinity') return Number.POSITIVE_INFINITY;
    if (record.__number === '-infinity') return Number.NEGATIVE_INFINITY;
    if (record.__number === '-0') return -0;
    throw new TypeError(`Unsupported worker number: ${record.__number}`);
  }
  if (isWorkerEnvelope(record, '__bigint', ['__bigint'])) {
    return BigInt(record.__bigint);
  }
  if (isWorkerEnvelope(record, '__map', ['__map'])) {
    return new Map(
      (record.__map || []).map(([key, item]: [unknown, unknown]) => [
        decodeWorkerValue(key),
        decodeWorkerValue(item),
      ]),
    );
  }
  if (isWorkerEnvelope(record, '__arrayBuffer', ['__arrayBuffer'])) {
    return Uint8Array.from(record.__arrayBuffer || []).buffer;
  }
  if (isWorkerEnvelope(record, '__typedArray', ['__typedArray', 'bytes', 'data'])) {
    const bytes = Uint8Array.from(record.bytes || record.data || []);
    if (record.__typedArray === 'DataView') return new DataView(bytes.buffer);
    const constructors: Record<string, any> = {
      Int8Array,
      Uint8Array,
      Uint8ClampedArray,
      Int16Array,
      Uint16Array,
      Int32Array,
      Uint32Array,
      Float32Array,
      Float64Array,
      ...(typeof BigInt64Array === 'undefined' ? {} : { BigInt64Array }),
      ...(typeof BigUint64Array === 'undefined' ? {} : { BigUint64Array }),
    };
    const Constructor = constructors[String(record.__typedArray)];
    if (typeof Constructor !== 'function') {
      throw new TypeError(`Unsupported worker typed array: ${record.__typedArray}`);
    }
    return new Constructor(bytes.buffer);
  }
  if (Array.isArray(value)) {
    return value.map((item) => decodeWorkerValue(item));
  }
  const output: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(record)) {
    defineWorkerValue(output, key, decodeWorkerValue(item));
  }
  return output;
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
  sourceCode?: string | null;
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
      sourceCode: opts.sourceCode,
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
    let awaitingRelease = false;
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
      if (msg.type === 'taskReleased' && awaitingRelease) {
        awaitingRelease = false;
        index++;
        sendNext();
        return;
      }
      if (msg.type !== 'result') return;
      if (timer) clearTimeout(timer);
      if (msg.success) {
        const res: any = {
          value: decodeWorkerValue(msg.value),
          valueAbsent: msg.valueAbsent === true,
          ctxChanges: decodeWorkerValue(msg.ctxChanges),
        };
        if (msg.shortCircuit) res.shortCircuit = true;
        results.push(res);
        awaitingRelease = true;
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
    let terminalResult: any;

    const timer = setTimeout(async () => {
      if (settled) return;
      settled = true;
      await cleanup();
      const err: any = new Error(
        `Script execution timed out after ${timeoutMs}ms`,
      );
      err.code = 'ERR_SCRIPT_EXECUTION_TIMEOUT';
      err.isTimeout = true;
      reject(err);
    }, timeoutMs + 5000);

    const cleanup = async (graceful = false) => {
      clearTimeout(timer);
      if (graceful) {
        const exited = await new Promise<boolean>((resolve) => {
          let shutdownTimer: ReturnType<typeof setTimeout>;
          const onExit = () => {
            clearTimeout(shutdownTimer);
            resolve(true);
          };
          shutdownTimer = setTimeout(() => {
            worker.off('exit', onExit);
            resolve(false);
          }, 1_000);
          worker.once('exit', onExit);
          try {
            worker.postMessage({ type: 'shutdown' });
          } catch {
            clearTimeout(shutdownTimer);
            worker.off('exit', onExit);
            resolve(false);
          }
        });
        if (exited) return;
      }
      try {
        await worker.terminate();
      } catch {}
    };

    worker.on('message', async (msg) => {
      if (msg.type === 'result') {
        if (settled || terminalResult) return;
        terminalResult = msg;
      } else if (msg.type === 'taskReleased' && msg.id === id) {
        if (settled || !terminalResult) return;
        settled = true;
        const result = terminalResult;
        await cleanup(true);
        if (result.success) {
          const res: any = {
            value: decodeWorkerValue(result.value),
            valueAbsent: result.valueAbsent === true,
            ctxChanges: decodeWorkerValue(result.ctxChanges),
          };
          if (result.shortCircuit) res.shortCircuit = true;
          resolve(res);
        } else {
          const err: any = new Error(
            result.error?.message || 'Handler execution failed',
          );
          err.statusCode = result.error?.statusCode;
          err.code = result.error?.code;
          err.details = result.error?.details;
          if (result.error?.stack) err.stack = result.error.stack;
          if (result.ctxChanges) {
            err.ctxChanges = decodeWorkerValue(result.ctxChanges);
          }
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

    worker.on('error', async (err) => {
      if (settled) return;
      settled = true;
      await cleanup();
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
