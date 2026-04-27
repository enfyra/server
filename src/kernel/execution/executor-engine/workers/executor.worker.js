'use strict';

const { parentPort } = require('worker_threads');
const { fork } = require('child_process');
const path = require('path');

let ivm;
try {
  ivm = require('isolated-vm');
} catch (err) {
  parentPort.postMessage({ type: 'error', message: 'isolated-vm not available: ' + err.message });
  process.exit(1);
}

const pendingCallbacks = new Map();
let callbackCounter = 0;
const taskPackages = new Map();
const pendingPackageRuntimeCalls = new Map();
let packageRuntimeChild = null;
let packageRuntimeCounter = 0;

const SCRIPT_CACHE_MAX = 500;
const HEAP_SAMPLE_INTERVAL_MS = 5_000;
const CONTEXT_POOL_MAX_PER_ISOLATE = 32;
const isolateCaches = new WeakMap();
const contextPackageKeys = new WeakMap();
const contextPackageMaps = new WeakMap();
const contextStats = {
  created: 0,
  reused: 0,
  released: 0,
  evicted: 0,
  scrubFailed: 0,
};
let isolatePool = null;
let isolatePoolIdx = 0;
let lastHeapSampleAt = 0;
let lastHeapRatio = 0;

async function sampleHeapRatio() {
  const now = Date.now();
  if (now - lastHeapSampleAt < HEAP_SAMPLE_INTERVAL_MS) return lastHeapRatio;
  lastHeapSampleAt = now;
  if (!isolatePool) return 0;
  let max = 0;
  for (const entry of isolatePool) {
    const iso = entry?.isolate;
    if (!iso || iso.isDisposed) continue;
    try {
      const s = await iso.getHeapStatistics();
      const limit = s.heap_size_limit || 1;
      const ratio = s.used_heap_size / limit;
      if (ratio > max) max = ratio;
    } catch {}
  }
  lastHeapRatio = max;
  return max;
}

async function postResultMessage(msg) {
  msg.heapRatio = await sampleHeapRatio();
  msg.contextStats = getContextStats();
  parentPort.postMessage(msg);
}

function getContextStats() {
  let idle = 0;
  if (isolatePool) {
    for (const entry of isolatePool) idle += entry?.idleContexts?.length || 0;
  }
  return { ...contextStats, idle };
}

function getOrCreateIsolate(memoryLimitMb, poolSize) {
  if (!isolatePool) {
    const size = Math.max(1, Math.trunc(Number(poolSize) || 8));
    isolatePool = new Array(size).fill(null);
  }
  const idx = isolatePoolIdx++ % isolatePool.length;
  let entry = isolatePool[idx];
  if (!entry || !entry.isolate || entry.isolate.isDisposed) {
    entry = {
      isolate: new ivm.Isolate({ memoryLimit: memoryLimitMb }),
      idleContexts: [],
    };
    isolatePool[idx] = entry;
  }
  return entry;
}

async function getCachedScript(isolate, code, filename) {
  let cache = isolateCaches.get(isolate);
  if (!cache) {
    cache = new Map();
    isolateCaches.set(isolate, cache);
  }
  const cached = cache.get(code);
  if (cached) return cached;
  const script = await isolate.compileScript(code, { filename });
  cache.set(code, script);
  if (cache.size > SCRIPT_CACHE_MAX) {
    const firstKey = cache.keys().next().value;
    cache.delete(firstKey);
  }
  return script;
}

parentPort.on('message', async (msg) => {
  if (msg.type === 'execute') {
    handleExecute(msg).catch(async (err) => {
      await postResultMessage({
        type: 'result',
        id: msg.id,
        success: false,
        error: { message: err.message, stack: err.stack },
      });
    });
  } else if (msg.type === 'executeBatch') {
    handleExecuteBatch(msg).catch(async (err) => {
      await postResultMessage({
        type: 'result',
        id: msg.id,
        success: false,
        error: { message: err.message, stack: err.stack },
      });
    });
  } else if (msg.type === 'callResult') {
    const cb = pendingCallbacks.get(msg.callId);
    if (cb) {
      pendingCallbacks.delete(msg.callId);
      cb.resolve(msg.result);
    }
  } else if (msg.type === 'callError') {
    const cb = pendingCallbacks.get(msg.callId);
    if (cb) {
      pendingCallbacks.delete(msg.callId);
      // msg.error may be a plain string (legacy) or JSON-encoded payload
      // with `__userThrow`. Keep message as JSON so extractThrowInfo()
      // recovers statusCode/code/details across the isolate boundary.
      cb.reject(new Error(msg.error));
    }
  }
});

function callMain(taskId, type, data) {
  return new Promise((resolve, reject) => {
    const callId = `cb_${++callbackCounter}`;
    pendingCallbacks.set(callId, { resolve, reject });
    parentPort.postMessage({ type, id: taskId, callId, ...data });
  });
}

function fireMain(taskId, type, data) {
  parentPort.postMessage({ type, id: taskId, callId: `f_${++callbackCounter}`, ...data });
}

function rejectPendingPackageRuntimeCalls(error) {
  for (const pending of pendingPackageRuntimeCalls.values()) {
    pending.reject(error);
  }
  pendingPackageRuntimeCalls.clear();
}

function getPackageRuntimeChild() {
  if (packageRuntimeChild && packageRuntimeChild.connected) {
    return packageRuntimeChild;
  }

  const childPath = path.join(__dirname, 'package-runtime.child.js');
  const child = fork(childPath, [], {
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
  });
  packageRuntimeChild = child;

  child.on('message', (msg) => {
    if (!msg || !msg.id) return;
    const pending = pendingPackageRuntimeCalls.get(msg.id);
    if (!pending) return;
    pendingPackageRuntimeCalls.delete(msg.id);
    if (msg.ok) {
      pending.resolve(msg.value);
    } else {
      const error = new Error(msg.error?.message || 'Package runtime call failed');
      error.stack = msg.error?.stack || error.stack;
      pending.reject(error);
    }
  });

  child.on('exit', (code, signal) => {
    if (packageRuntimeChild === child) packageRuntimeChild = null;
    rejectPendingPackageRuntimeCalls(
      new Error(`Package runtime exited${signal ? ` with signal ${signal}` : ` with code ${code}`}`),
    );
  });

  child.on('error', (error) => {
    if (packageRuntimeChild === child) packageRuntimeChild = null;
    rejectPendingPackageRuntimeCalls(error);
  });

  return child;
}

function callPackageRuntime(payload) {
  return new Promise((resolve, reject) => {
    const child = getPackageRuntimeChild();
    const id = `pkg_${++packageRuntimeCounter}`;
    pendingPackageRuntimeCalls.set(id, { resolve, reject });
    try {
      child.send({ ...payload, id }, (error) => {
        if (!error) return;
        pendingPackageRuntimeCalls.delete(id);
        reject(error);
      });
    } catch (error) {
      pendingPackageRuntimeCalls.delete(id);
      reject(error);
    }
  });
}

function firePackageRuntime(payload) {
  try {
    if (!packageRuntimeChild || !packageRuntimeChild.connected) return;
    const child = packageRuntimeChild;
    child.send({ ...payload, id: `pkg_${++packageRuntimeCounter}`, fire: true });
  } catch {}
}

async function executePackageRuntimeCall(taskId, data) {
  const packages = taskPackages.get(taskId);
  if (!packages) throw new Error('Package context is not available');

  if (data.handleId) {
    return callPackageRuntime({
      op: 'handleCall',
      taskId,
      handleId: data.handleId,
      path: data.path || [],
      argsJson: data.argsJson || '[]',
    });
  }

  const pkg = packages.get(data.packageName);
  if (!pkg) throw new Error(`Package "${data.packageName}" is not available`);

  return callPackageRuntime({
    op: data.kind === 'construct' ? 'construct' : 'call',
    taskId,
    packageName: data.packageName,
    package: {
      name: pkg.name,
      fileUrl: pkg.fileUrl,
    },
    path: data.path || [],
    argsJson: data.argsJson || '[]',
  });
}

async function loadEsmPackageIntoContext(isolate, context, sourceCode, safeName) {
  try {
    const mod = await isolate.compileModule(sourceCode, { filename: `pkg:${safeName}` });

    await mod.instantiate(context, (specifier) => {
      throw new Error(`Unresolved import in package ${safeName}: ${specifier}`);
    });

    await mod.evaluate({ timeout: 10000 });

    const defaultRef = await mod.namespace.get('default');
    if (defaultRef !== undefined && defaultRef.typeof !== 'undefined') {
      return defaultRef.derefInto({ release: false });
    }

    return mod.namespace.derefInto({ release: false });
  } catch (_err) {
    return null;
  }
}

const SETUP_CODE_TEMPLATE = `
"use strict";
let $ctx = {};
let $pkgs = {};
let __taskId = null;
let __logs = [];

const __applyOpts = { result: { promise: true, copy: true } };

function __unwrapMainThreadPayload(v) {
  if (v !== null && typeof v === 'object' && v.__e === 'u') return undefined;
  if (v !== null && typeof v === 'object' && v.__e === 'v') return __unwrapMainThreadPayload(v.d);
  return v;
}

function __parseMainThreadResult(s) {
  const w = typeof s === 'string' ? JSON.parse(s) : s;
  return __unwrapMainThreadPayload(w);
}

async function __call(type, dataJson) {
  const r = await __callRef.apply(undefined, [__taskId, type, dataJson], __applyOpts);
  return __parseMainThreadResult(r);
}

function __safeClone(v) {
  if (v === undefined) return undefined;
  try { return JSON.parse(JSON.stringify(v)); }
  catch { return null; }
}

function __extractResult(result, isAbsent) {
  const valueAbsent = isAbsent === true;
  const out = {
    valueAbsent,
    ctxChanges: {
      $body: __safeClone($ctx.$body),
      $query: __safeClone($ctx.$query),
      $params: __safeClone($ctx.$params),
      $data: __safeClone($ctx.$data),
      $share: __safeClone($ctx.$share),
      $flow: __safeClone($ctx.$flow),
      $error: __safeClone($ctx.$error),
      $api: __safeClone($ctx.$api),
      $statusCode: $ctx.$statusCode,
    }
  };
  if (!valueAbsent) out.value = __safeClone(result);
  return JSON.stringify(out);
}

const __throwDefaults = { 400: 'Bad request', 401: 'Unauthorized', 403: 'Forbidden', 404: 'Not found', 409: 'Conflict', 422: 'Validation failed', 429: 'Too many requests', 500: 'Internal server error', 503: 'Service unavailable' };

function __createPkgHandleProxy(handlePromise, path) {
  const fn = function (...args) {
    return handlePromise.then((handleId) =>
      __call('pkgCall', JSON.stringify({ handleId, path, argsJson: JSON.stringify(args), kind: 'call' })),
    );
  };
  return new Proxy(fn, {
    get: (_, prop) => {
      if (prop === 'then') return undefined;
      if (prop === '__pkgHandlePromise') return handlePromise;
      return __createPkgHandleProxy(handlePromise, path.concat(String(prop)));
    },
    apply: async (_target, _thisArg, args) => {
      const handleId = await handlePromise;
      return __call('pkgCall', JSON.stringify({ handleId, path, argsJson: JSON.stringify(args), kind: 'call' }));
    }
  });
}

function __wrapPkgValue(value) {
  if (value && typeof value === 'object' && value.__pkgHandle) {
    return __createPkgHandleProxy(Promise.resolve(value.__pkgHandle), []);
  }
  return value;
}

function __createPkgProxy(packageName, path) {
  const fn = function (...args) {
    return __call('pkgCall', JSON.stringify({ packageName, path, argsJson: JSON.stringify(args), kind: 'call' }))
      .then(__wrapPkgValue);
  };
  return new Proxy(fn, {
    get: (_, prop) => {
      if (prop === 'then') return undefined;
      return __createPkgProxy(packageName, path.concat(String(prop)));
    },
    apply: async (_target, _thisArg, args) =>
      __wrapPkgValue(await __call('pkgCall', JSON.stringify({ packageName, path, argsJson: JSON.stringify(args), kind: 'call' }))),
    construct: (_target, args) => {
      const handlePromise = __call('pkgCall', JSON.stringify({ packageName, path, argsJson: JSON.stringify(args), kind: 'construct' }))
        .then((value) => {
          if (!value || !value.__pkgHandle) throw new Error('Package constructor did not return a handle');
          return value.__pkgHandle;
        });
      return __createPkgHandleProxy(handlePromise, []);
    }
  });
}

const require = (name) => {
  if ($pkgs[name] === undefined) throw new Error('Module "' + name + '" is not available. Install it via Settings \\u2192 Packages');
  return $pkgs[name];
};

const console = {
  log: (...a) => $ctx.$logs(...a),
  warn: (...a) => $ctx.$logs(...a),
  error: (...a) => $ctx.$logs(...a),
  info: (...a) => $ctx.$logs(...a),
};

function __resetTask(snapshot, taskId) {
  $ctx = snapshot || {};
  $pkgs = {};
  __taskId = taskId;
  __logs = [];
  %%PKG_SETUP%%
  $ctx.$pkgs = $pkgs;
  $ctx.$share = $ctx.$share || {};
  $ctx.$share.$logs = __logs;
  $ctx.$repos = new Proxy({}, {
    get: (_, table) => new Proxy({}, {
      get: (_, method) => async (...args) =>
        __call('repoCall', JSON.stringify({ table: String(table), method: String(method), argsJson: JSON.stringify(args) }))
    })
  });
  $ctx.$helpers = new Proxy({}, {
    get: (_, name) => {
      const basePath = String(name);
      const fn = async (...args) =>
        __call('helpersCall', JSON.stringify({ name: basePath, argsJson: JSON.stringify(args) }));
      return new Proxy(fn, {
        get: (_, subName) => async (...args) =>
          __call('helpersCall', JSON.stringify({ name: basePath + '.' + String(subName), argsJson: JSON.stringify(args) }))
      });
    }
  });
  $ctx.$socket = new Proxy({}, {
    get: (_, method) => async (...args) =>
      __call('socketCall', JSON.stringify({ method: String(method), argsJson: JSON.stringify(args) }))
  });
  $ctx.$cache = new Proxy({}, {
    get: (_, method) => async (...args) =>
      __call('cacheCall', JSON.stringify({ method: String(method), argsJson: JSON.stringify(args) }))
  });
  $ctx.$trigger = async (...args) =>
    __call('dispatchCall', JSON.stringify({ method: 'trigger', argsJson: JSON.stringify(args) }));
  $ctx.$throw = new Proxy({}, {
    get: (_, status) => (message, details) => {
      const code = parseInt(String(status));
      const msg = (message !== undefined && message !== null && message !== '') ? String(message) : (__throwDefaults[code] || 'Error');
      const payload = JSON.stringify({ __userThrow: true, statusCode: code, message: msg, details: details || null });
      throw new Error(payload);
    }
  });
  $ctx.$logs = (...args) => {
    for (const a of args) {
      try { __logs.push(JSON.parse(JSON.stringify(a))); }
      catch { __logs.push(String(a)); }
    }
  };
}

const __baselineGlobalKeys = new Set(Reflect.ownKeys(globalThis));
const __baselineObjectPrototypeKeys = new Set(Reflect.ownKeys(Object.prototype));
const __baselineArrayPrototypeKeys = new Set(Reflect.ownKeys(Array.prototype));
const __baselineFunctionPrototypeKeys = new Set(Reflect.ownKeys(Function.prototype));

function __deleteExtraKeys(target, baseline) {
  for (const key of Reflect.ownKeys(target)) {
    if (baseline.has(key)) continue;
    try { delete target[key]; } catch {}
  }
}

function __cleanupTask() {
  __deleteExtraKeys(globalThis, __baselineGlobalKeys);
  __deleteExtraKeys(Object.prototype, __baselineObjectPrototypeKeys);
  __deleteExtraKeys(Array.prototype, __baselineArrayPrototypeKeys);
  __deleteExtraKeys(Function.prototype, __baselineFunctionPrototypeKeys);
  $ctx = {};
  $pkgs = {};
  __taskId = null;
  __logs = [];
  return true;
}
`;

function getPackageKey(pkgSources) {
  const hashString = (value) => {
    let hash = 2166136261;
    for (let i = 0; i < value.length; i++) {
      hash ^= value.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(36);
  };
  return pkgSources
    .map((pkg) => `${pkg.name}:${pkg.version || ''}:${hashString(pkg.sourceCode || '')}`)
    .join('|');
}

async function createPreparedContext(isolate, id, pkgSources) {
  const context = await isolate.createContext();
  const jail = context.global;
  await jail.set('global', jail.derefInto());

  const loadedPkgNames = [];
  const proxiedPkgNames = [];
  const packageMap = new Map();
  for (const pkg of pkgSources) {
    packageMap.set(pkg.name, pkg);
    const pkgVal = pkg.sourceCode
      ? await loadEsmPackageIntoContext(isolate, context, pkg.sourceCode, pkg.safeName)
      : null;
    if (pkgVal !== null) {
      await jail.set(`__pkg_${pkg.safeName}`, pkgVal);
      loadedPkgNames.push(pkg);
    } else {
      proxiedPkgNames.push(pkg);
    }
  }
  taskPackages.set(id, packageMap);

  await jail.set(
    '__callRef',
    new ivm.Reference(async (taskId, type, dataJson) => {
      const data = JSON.parse(dataJson);
      if (type === 'pkgCall') {
        return JSON.stringify(await executePackageRuntimeCall(taskId, data));
      }
      return callMain(taskId, type, data);
    }),
  );

  await jail.set(
    '__fireRef',
    new ivm.Reference((taskId, type, dataJson) => {
      const data = JSON.parse(dataJson);
      fireMain(taskId, type, data);
    }),
  );

  const pkgSetupLines = loadedPkgNames
    .map((p) => `$pkgs[${JSON.stringify(p.name)}] = __pkg_${p.safeName};`)
    .concat(
      proxiedPkgNames.map(
        (p) => `$pkgs[${JSON.stringify(p.name)}] = __createPkgProxy(${JSON.stringify(p.name)}, []);`,
      ),
    )
    .join('\n');

  const setupCode = SETUP_CODE_TEMPLATE.replace('%%PKG_SETUP%%', pkgSetupLines);
  const setupScript = await getCachedScript(isolate, setupCode, 'setup.js');
  await setupScript.run(context, { timeout: 5000 });

  contextPackageKeys.set(context, getPackageKey(pkgSources));
  contextPackageMaps.set(context, packageMap);
  return context;
}

async function prepareContext(entry, id, pkgSources, snapshot) {
  const isolate = entry.isolate;
  const pkgKey = getPackageKey(pkgSources);
  let context = null;
  for (let i = entry.idleContexts.length - 1; i >= 0; i--) {
    const candidate = entry.idleContexts[i];
    entry.idleContexts.splice(i, 1);
    if (contextPackageKeys.get(candidate) === pkgKey) {
      context = candidate;
      contextStats.reused++;
      break;
    }
    try {
      candidate.release();
    } catch {}
  }
  if (!context) {
    context = await createPreparedContext(isolate, id, pkgSources);
    contextStats.created++;
  }

  taskPackages.set(id, contextPackageMaps.get(context) || new Map());
  const jail = context.global;
  await jail.set('__snapshot', new ivm.ExternalCopy(snapshot).copyInto());
  await jail.set('__nextTaskId', id);
  const resetScript = await getCachedScript(
    isolate,
    '__resetTask(__snapshot, __nextTaskId);',
    'reset-task.js',
  );
  await resetScript.run(context, { timeout: 5000 });
  return context;
}

async function releaseContext(entry, context) {
  if (!context || entry.isolate.isDisposed) return;
  try {
    const cleanupScript = await getCachedScript(
      entry.isolate,
      '__cleanupTask();',
      'cleanup-task.js',
    );
    await cleanupScript.run(context, { timeout: 5000 });
  } catch {
    contextStats.scrubFailed++;
    try {
      context.release();
    } catch {}
    return;
  }
  if (entry.idleContexts.length >= CONTEXT_POOL_MAX_PER_ISOLATE) {
    try {
      context.release();
    } catch {}
    contextStats.evicted++;
    return;
  }
  entry.idleContexts.push(context);
  contextStats.released++;
}

function extractThrowInfo(error) {
  try {
    const parsed = JSON.parse(error.message);
    if (parsed && parsed.__userThrow) {
      return {
        ...parsed,
        message: parsed.message ?? error.message,
      };
    }
  } catch {}
  return null;
}

const WRAPPER_LINE_OFFSET = 3;
const SKIP_FILENAMES = ['setup.js', 'extract.js', 'extract-error-ctx.js', 'set-status.js', 'populate-error.js'];

function parseUserCodeLocation(stack) {
  if (!stack) return null;
  const lines = stack.split('\n');
  for (const line of lines) {
    const m = line.match(/at\s+(.+\.js):(\d+):(\d+)/);
    if (!m) continue;
    const filename = m[1].trim();
    if (SKIP_FILENAMES.includes(filename)) continue;
    const rawLine = parseInt(m[2]);
    const userLine = rawLine - WRAPPER_LINE_OFFSET;
    if (userLine <= 0) continue;
    return { line: userLine, column: parseInt(m[3]), phase: filename.replace('.js', '') };
  }
  return null;
}

function buildErrorPayload(error) {
  const throwInfo = extractThrowInfo(error);
  if (throwInfo) {
    return {
      message: throwInfo.message,
      statusCode: throwInfo.statusCode,
      details: throwInfo.details,
    };
  }
  const loc = parseUserCodeLocation(error.stack);
  const baseMsg = error.message || 'Unknown error';
  const message = loc ? `${baseMsg} (${loc.phase}, line ${loc.line})` : baseMsg;
  return {
    message,
    statusCode: error.statusCode || null,
    details: loc || undefined,
  };
}

async function postError(id, error) {
  const payload = buildErrorPayload(error);
  await postResultMessage({
    type: 'result',
    id,
    success: false,
    error: {
      ...payload,
      stack: error.stack,
      code: error.code,
    },
  });
}

async function handleExecute(msg) {
  const { id, code, pkgSources, snapshot, memoryLimitMb, isolatePoolSize } = msg;
  const timeoutMs = Math.max(1, Math.trunc(Number(msg.timeoutMs) || 30000));
  const entry = getOrCreateIsolate(memoryLimitMb, isolatePoolSize);
  const isolate = entry.isolate;
  const context = await prepareContext(entry, id, pkgSources, snapshot);

  try {
    const wrappedCode = `
(async () => {
  "use strict";
  ${code}
})().then((__result) => __extractResult(__result, __result === undefined));
`;

    const script = await getCachedScript(isolate, wrappedCode, 'handler.js');
    const jsonStr = await script.run(context, { timeout: timeoutMs, promise: true });

    const result = JSON.parse(jsonStr);
    await postResultMessage({ type: 'result', id, success: true, ...result });
  } catch (error) {
    await postError(id, error);
  } finally {
    await releaseContext(entry, context);
    cleanupTaskPackages(id);
  }
}

async function handleExecuteBatch(msg) {
  const { id, codeBlocks, pkgSources, snapshot, memoryLimitMb, isolatePoolSize } = msg;
  const totalTimeoutMs = Math.max(1, Math.trunc(Number(msg.timeoutMs) || 30000));
  const entry = getOrCreateIsolate(memoryLimitMb, isolatePoolSize);
  const isolate = entry.isolate;
  const context = await prepareContext(entry, id, pkgSources, snapshot);

  const preHooksAndHandler = [];
  const postHooks = [];
  for (const block of codeBlocks) {
    const type = block.type || 'handler';
    if (type === 'postHook') {
      postHooks.push(block);
    } else {
      preHooksAndHandler.push(block);
    }
  }

  try {
    let caughtError = null;

    for (let i = 0; i < preHooksAndHandler.length; i++) {
      const block = preHooksAndHandler[i];
      const blockType = block.type || 'handler';
      const blockLabel = blockType === 'handler' ? 'handler' : `${blockType} #${i + 1}`;

      let wrappedBlock;
      if (blockType === 'preHook') {
        wrappedBlock = `
(async () => {
  "use strict";
  ${block.code}
})().then((__r) => {
  if (__r !== undefined) return JSON.stringify({ __shortCircuit: true, value: __r });
  return JSON.stringify({ __shortCircuit: false });
});`;
      } else {
        wrappedBlock = `
(async () => {
  "use strict";
  ${block.code}
})().then((__r) => {
  $ctx.$data = __r;
  return "__handler_done__";
});`;
      }

      try {
        const script = await getCachedScript(isolate, wrappedBlock, `${blockLabel}.js`);
        const rawResult = await script.run(context, { timeout: totalTimeoutMs, promise: true });

        if (blockType === 'preHook' && typeof rawResult === 'string') {
          const parsed = JSON.parse(rawResult);
          if (parsed.__shortCircuit) {
            const out = await extractFinalResult(isolate, context, parsed.value);
            await postResultMessage({ type: 'result', id, success: true, ...out, shortCircuit: true });
            return;
          }
        }
      } catch (error) {
        caughtError = error;
        break;
      }
    }

    if (!caughtError && postHooks.length > 0) {
      const setStatusCode = `$ctx.$statusCode = 200; "__status_set__";`;
      const statusScript = await getCachedScript(isolate, setStatusCode, 'set-status.js');
      await statusScript.run(context, { timeout: 5000 });
    }

    if (caughtError && postHooks.length > 0) {
      const errPayload = buildErrorPayload(caughtError);
      const errorInfo = {
        message: errPayload.message || 'Unknown error',
        name: caughtError.constructor?.name || 'Error',
        stack: caughtError.stack || '',
        statusCode: errPayload.statusCode || 500,
        details: errPayload.details || {},
        timestamp: new Date().toISOString(),
      };

      const populateErrorCode = `
$ctx.$error = ${JSON.stringify(errorInfo)};
$ctx.$data = null;
$ctx.$statusCode = ${errorInfo.statusCode};
if ($ctx.$api) {
  $ctx.$api.error = ${JSON.stringify(errorInfo)};
  $ctx.$api.response = {
    statusCode: ${errorInfo.statusCode},
    responseTime: $ctx.$api.request && $ctx.$api.request.timestamp
      ? Date.now() - new Date($ctx.$api.request.timestamp).getTime()
      : 0,
    timestamp: ${JSON.stringify(errorInfo.timestamp)},
  };
}
"__error_populated__";`;
      const populateScript = await isolate.compileScript(populateErrorCode, { filename: 'populate-error.js' });
      await populateScript.run(context, { timeout: 5000 });
    }

    for (let i = 0; i < postHooks.length; i++) {
      const block = postHooks[i];
      const blockLabel = `postHook #${i + 1}`;

      const wrappedBlock = `
(async () => {
  "use strict";
  ${block.code}
})().then((__r) => {
  if (__r !== undefined) $ctx.$data = __r;
  return "__posthook_done__";
});`;

      try {
        const script = await getCachedScript(isolate, wrappedBlock, `${blockLabel}.js`);
        await script.run(context, { timeout: totalTimeoutMs, promise: true });
      } catch (postHookError) {
        // Individual postHook failure should not stop other postHooks
      }
    }

    if (caughtError) {
      const errPayloadFinal = buildErrorPayload(caughtError);
      let ctxChanges;
      try {
        const extractCode = `JSON.stringify({
          $body: __safeClone($ctx.$body),
          $query: __safeClone($ctx.$query),
          $params: __safeClone($ctx.$params),
          $data: __safeClone($ctx.$data),
          $share: __safeClone($ctx.$share),
          $flow: __safeClone($ctx.$flow),
          $error: __safeClone($ctx.$error),
          $api: __safeClone($ctx.$api),
          $statusCode: $ctx.$statusCode,
        })`;
        const extractScript = await getCachedScript(isolate, extractCode, 'extract-error-ctx.js');
        const json = await extractScript.run(context, { timeout: 5000 });
        ctxChanges = JSON.parse(json);
      } catch {}
      await postResultMessage({
        type: 'result',
        id,
        success: false,
        error: {
          message: errPayloadFinal.message,
          statusCode: errPayloadFinal.statusCode,
          stack: caughtError.stack,
          code: caughtError.code,
          details: errPayloadFinal.details,
        },
        ctxChanges,
      });
      return;
    }

    const out = await extractFinalResult(isolate, context);
    await postResultMessage({ type: 'result', id, success: true, ...out });
  } catch (error) {
    await postError(id, error);
  } finally {
    await releaseContext(entry, context);
    cleanupTaskPackages(id);
  }
}

function cleanupTaskPackages(taskId) {
  taskPackages.delete(taskId);
  firePackageRuntime({ op: 'releaseTask', taskId });
}

function shutdownPackageRuntime() {
  if (!packageRuntimeChild) return;
  try {
    packageRuntimeChild.kill();
  } catch {}
  packageRuntimeChild = null;
}

process.on('exit', shutdownPackageRuntime);

async function extractFinalResult(isolate, context, overrideValue) {
  if (overrideValue !== undefined) {
    const callCode = `__extractResult(${JSON.stringify(overrideValue)}, false)`;
    const script = await isolate.compileScript(callCode, { filename: 'extract.js' });
    const jsonStr = await script.run(context, { timeout: 5000 });
    return JSON.parse(jsonStr);
  }
  const callCode = '__extractResult($ctx.$data, $ctx.$data === undefined)';
  const script = await getCachedScript(isolate, callCode, 'extract.js');
  const jsonStr = await script.run(context, { timeout: 5000 });
  return JSON.parse(jsonStr);
}
