'use strict';

const { parentPort } = require('worker_threads');
const fs = require('fs');
const { fileURLToPath } = require('url');
const { createPackageRuntimeBridge } = require('./package-runtime-bridge');
const { buildSetupCode } = require('./worker-setup-template');

let ivm;
try {
  ivm = require('isolated-vm');
} catch (err) {
  parentPort.postMessage({
    type: 'error',
    message: 'isolated-vm not available: ' + err.message,
  });
  process.exit(1);
}

const pendingCallbacks = new Map();
let callbackCounter = 0;
const activeTaskContexts = new Map();
const packageRuntimeBridge = createPackageRuntimeBridge({
  workerDir: __dirname,
  activeTaskContexts,
});

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
  parentPort.postMessage({
    type,
    id: taskId,
    callId: `f_${++callbackCounter}`,
    ...data,
  });
}

function getPackageSourceCode(pkg) {
  if (typeof pkg.sourceCode === 'string' && pkg.sourceCode.length > 0) {
    return pkg.sourceCode;
  }
  let filePath = pkg.filePath;
  if (
    !filePath &&
    typeof pkg.fileUrl === 'string' &&
    pkg.fileUrl.startsWith('file://')
  ) {
    filePath = fileURLToPath(pkg.fileUrl);
  }
  if (!filePath || !fs.existsSync(filePath)) return null;
  return fs.readFileSync(filePath, 'utf8');
}

async function loadEsmPackageIntoContext(isolate, context, pkg) {
  try {
    const sourceCode = getPackageSourceCode(pkg);
    if (!sourceCode) return null;

    const mod = await isolate.compileModule(sourceCode, {
      filename: `pkg:${pkg.safeName}`,
    });

    await mod.instantiate(context, (specifier) => {
      throw new Error(
        `Unresolved import in package ${pkg.safeName}: ${specifier}`,
      );
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

function getPackageKey(pkgSources, isolatePackageNames) {
  const hashString = (value) => {
    let hash = 2166136261;
    for (let i = 0; i < value.length; i++) {
      hash ^= value.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(36);
  };
  const isolateNames = new Set(isolatePackageNames || []);
  return pkgSources
    .map((pkg) => {
      const sourceKey =
        pkg.cacheKey ||
        `${pkg.filePath || ''}:${pkg.fileUrl || ''}:${pkg.mtimeMs || ''}:${pkg.size || ''}:${hashString(pkg.sourceCode || '')}`;
      const mode = isolateNames.has(pkg.name) ? 'isolate' : 'proxy';
      return `${pkg.name}:${pkg.version || ''}:${sourceKey}:${mode}`;
    })
    .join('|');
}

function collectReferencedPackageNames(codeText, pkgSources) {
  const available = new Set(pkgSources.map((pkg) => pkg.name));
  const referenced = new Set();
  const text = String(codeText || '');
  const bracketPattern = /\b(?:\$ctx\.)?\$pkgs\s*\[\s*(['"`])([^'"`]+)\1\s*\]/g;
  const requirePattern = /\brequire\s*\(\s*(['"`])([^'"`]+)\1\s*\)/g;
  const dotPattern = /\b(?:\$ctx\.)?\$pkgs\.([A-Za-z_$][\w$]*)/g;

  for (const pattern of [bracketPattern, requirePattern]) {
    for (const match of text.matchAll(pattern)) {
      const name = match[2];
      if (available.has(name)) referenced.add(name);
    }
  }
  for (const match of text.matchAll(dotPattern)) {
    const name = match[1];
    if (available.has(name)) referenced.add(name);
  }
  return referenced;
}

async function createPreparedContext(
  isolate,
  id,
  pkgSources,
  isolatePackageNames,
) {
  const context = await isolate.createContext();
  const jail = context.global;
  await jail.set('global', jail.derefInto());

  const loadedPkgNames = [];
  const proxiedPkgNames = [];
  const packageMap = new Map();
  for (const pkg of pkgSources) {
    packageMap.set(pkg.name, pkg);
    const pkgVal = isolatePackageNames.has(pkg.name)
      ? await loadEsmPackageIntoContext(isolate, context, pkg)
      : null;
    if (pkgVal !== null) {
      await jail.set(`__pkg_${pkg.safeName}`, pkgVal);
      loadedPkgNames.push(pkg);
    } else {
      proxiedPkgNames.push(pkg);
    }
  }
  packageRuntimeBridge.setTaskPackages(id, packageMap);

  await jail.set(
    '__callRef',
    new ivm.Reference(async (taskId, type, dataJson) => {
      const data = JSON.parse(dataJson);
      if (type === 'pkgCall') {
        return JSON.stringify(
          await packageRuntimeBridge.executePackageRuntimeCall(taskId, data),
        );
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
        (p) =>
          `$pkgs[${JSON.stringify(p.name)}] = __createPkgProxy(${JSON.stringify(p.name)}, []);`,
      ),
    )
    .join('\n');

  const setupCode = buildSetupCode(pkgSetupLines);
  const setupScript = await getCachedScript(isolate, setupCode, 'setup.js');
  await setupScript.run(context, { timeout: 5000 });

  contextPackageKeys.set(
    context,
    getPackageKey(pkgSources, isolatePackageNames),
  );
  contextPackageMaps.set(context, packageMap);
  return context;
}

async function prepareContext(
  entry,
  id,
  pkgSources,
  snapshot,
  isolatePackageNames,
) {
  const isolate = entry.isolate;
  const pkgKey = getPackageKey(pkgSources, isolatePackageNames);
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
    context = await createPreparedContext(
      isolate,
      id,
      pkgSources,
      isolatePackageNames,
    );
    contextStats.created++;
  }

  packageRuntimeBridge.setTaskPackages(
    id,
    contextPackageMaps.get(context) || new Map(),
  );
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
const SKIP_FILENAMES = [
  'setup.js',
  'extract.js',
  'extract-error-ctx.js',
  'set-status.js',
  'populate-error.js',
];

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
    return {
      line: userLine,
      column: parseInt(m[3]),
      phase: filename.replace('.js', ''),
    };
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
  const { id, code, pkgSources, snapshot, memoryLimitMb, isolatePoolSize } =
    msg;
  const packages = pkgSources || [];
  const timeoutMs = Math.max(1, Math.trunc(Number(msg.timeoutMs) || 30000));
  const isolatePackageNames = collectReferencedPackageNames(code, packages);
  const entry = getOrCreateIsolate(memoryLimitMb, isolatePoolSize);
  const isolate = entry.isolate;
  const context = await prepareContext(
    entry,
    id,
    packages,
    snapshot,
    isolatePackageNames,
  );
  activeTaskContexts.set(id, context);

  try {
    const wrappedCode = `
(async () => {
  "use strict";
  ${code}
})().then((__result) => __extractResult(__result, __result === undefined));
`;

    const script = await getCachedScript(isolate, wrappedCode, 'handler.js');
    const jsonStr = await script.run(context, {
      timeout: timeoutMs,
      promise: true,
    });

    const result = JSON.parse(jsonStr);
    await postResultMessage({ type: 'result', id, success: true, ...result });
  } catch (error) {
    await postError(id, error);
  } finally {
    activeTaskContexts.delete(id);
    await releaseContext(entry, context);
    cleanupTaskPackages(id);
  }
}

async function handleExecuteBatch(msg) {
  const {
    id,
    codeBlocks,
    pkgSources,
    snapshot,
    memoryLimitMb,
    isolatePoolSize,
  } = msg;
  const packages = pkgSources || [];
  const totalTimeoutMs = Math.max(
    1,
    Math.trunc(Number(msg.timeoutMs) || 30000),
  );
  const isolatePackageNames = collectReferencedPackageNames(
    (codeBlocks || []).map((block) => block.code || '').join('\n'),
    packages,
  );
  const entry = getOrCreateIsolate(memoryLimitMb, isolatePoolSize);
  const isolate = entry.isolate;
  const context = await prepareContext(
    entry,
    id,
    packages,
    snapshot,
    isolatePackageNames,
  );
  activeTaskContexts.set(id, context);

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
      const blockLabel =
        blockType === 'handler' ? 'handler' : `${blockType} #${i + 1}`;

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
        const script = await getCachedScript(
          isolate,
          wrappedBlock,
          `${blockLabel}.js`,
        );
        const rawResult = await script.run(context, {
          timeout: totalTimeoutMs,
          promise: true,
        });

        if (blockType === 'preHook' && typeof rawResult === 'string') {
          const parsed = JSON.parse(rawResult);
          if (parsed.__shortCircuit) {
            const out = await extractFinalResult(
              isolate,
              context,
              parsed.value,
            );
            await postResultMessage({
              type: 'result',
              id,
              success: true,
              ...out,
              shortCircuit: true,
            });
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
      const statusScript = await getCachedScript(
        isolate,
        setStatusCode,
        'set-status.js',
      );
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
      const populateScript = await isolate.compileScript(populateErrorCode, {
        filename: 'populate-error.js',
      });
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
        const script = await getCachedScript(
          isolate,
          wrappedBlock,
          `${blockLabel}.js`,
        );
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
        const extractScript = await getCachedScript(
          isolate,
          extractCode,
          'extract-error-ctx.js',
        );
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
    activeTaskContexts.delete(id);
    await releaseContext(entry, context);
    cleanupTaskPackages(id);
  }
}

function cleanupTaskPackages(taskId) {
  packageRuntimeBridge.cleanupTaskPackages(taskId);
}

function shutdownPackageRuntime() {
  packageRuntimeBridge.shutdownPackageRuntime();
}

process.on('exit', shutdownPackageRuntime);

async function extractFinalResult(isolate, context, overrideValue) {
  if (overrideValue !== undefined) {
    const callCode = `__extractResult(${JSON.stringify(overrideValue)}, false)`;
    const script = await isolate.compileScript(callCode, {
      filename: 'extract.js',
    });
    const jsonStr = await script.run(context, { timeout: 5000 });
    return JSON.parse(jsonStr);
  }
  const callCode = '__extractResult($ctx.$data, $ctx.$data === undefined)';
  const script = await getCachedScript(isolate, callCode, 'extract.js');
  const jsonStr = await script.run(context, { timeout: 5000 });
  return JSON.parse(jsonStr);
}
