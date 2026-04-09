'use strict';

const { parentPort } = require('worker_threads');

let ivm;
try {
  ivm = require('isolated-vm');
} catch (err) {
  parentPort.postMessage({ type: 'error', message: 'isolated-vm not available: ' + err.message });
  process.exit(1);
}

const pendingCallbacks = new Map();
let callbackCounter = 0;

parentPort.on('message', async (msg) => {
  if (msg.type === 'execute') {
    handleExecute(msg).catch((err) => {
      parentPort.postMessage({
        type: 'result',
        id: msg.id,
        success: false,
        error: { message: err.message, stack: err.stack },
      });
    });
  } else if (msg.type === 'executeBatch') {
    handleExecuteBatch(msg).catch((err) => {
      parentPort.postMessage({
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
const $ctx = __snapshot;
const $pkgs = {};
%%PKG_SETUP%%
$ctx.$pkgs = $pkgs;

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
  const r = await __callRef.apply(undefined, [type, dataJson], __applyOpts);
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
  get: (_, method) => (...args) => {
    __fireRef.applyIgnored(undefined, ['socketCall', JSON.stringify({ method: String(method), argsJson: JSON.stringify(args) })]);
  }
});

$ctx.$cache = new Proxy({}, {
  get: (_, method) => async (...args) =>
    __call('cacheCall', JSON.stringify({ method: String(method), argsJson: JSON.stringify(args) }))
});

$ctx.$dispatch = new Proxy({}, {
  get: (_, method) => async (...args) =>
    __call('dispatchCall', JSON.stringify({ method: String(method), argsJson: JSON.stringify(args) }))
});

const __throwDefaults = { 400: 'Bad request', 401: 'Unauthorized', 403: 'Forbidden', 404: 'Not found', 409: 'Conflict', 422: 'Validation failed', 429: 'Too many requests', 500: 'Internal server error', 503: 'Service unavailable' };
$ctx.$throw = new Proxy({}, {
  get: (_, status) => (message, details) => {
    const code = parseInt(String(status));
    const msg = (message !== undefined && message !== null && message !== '') ? String(message) : (__throwDefaults[code] || 'Error');
    const payload = JSON.stringify({ __userThrow: true, statusCode: code, message: msg, details: details || null });
    throw new Error(payload);
  }
});

const __logs = [];
$ctx.$logs = (...args) => {
  for (const a of args) {
    try { __logs.push(JSON.parse(JSON.stringify(a))); }
    catch { __logs.push(String(a)); }
  }
};
$ctx.$share = $ctx.$share || {};
$ctx.$share.$logs = __logs;

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
`;

async function createIsolateContext(id, pkgSources, snapshot, memoryLimitMb) {
  const isolate = new ivm.Isolate({ memoryLimit: memoryLimitMb });
  const context = await isolate.createContext();
  const jail = context.global;
  await jail.set('global', jail.derefInto());

  const loadedPkgNames = [];
  for (const pkg of pkgSources) {
    const pkgVal = await loadEsmPackageIntoContext(isolate, context, pkg.sourceCode, pkg.safeName);
    if (pkgVal !== null) {
      await jail.set(`__pkg_${pkg.safeName}`, pkgVal);
      loadedPkgNames.push(pkg);
    }
  }

  await jail.set('__snapshot', new ivm.ExternalCopy(snapshot).copyInto());

  await jail.set(
    '__callRef',
    new ivm.Reference(async (type, dataJson) => {
      const data = JSON.parse(dataJson);
      return callMain(id, type, data);
    }),
  );

  await jail.set(
    '__fireRef',
    new ivm.Reference((type, dataJson) => {
      const data = JSON.parse(dataJson);
      fireMain(id, type, data);
    }),
  );

  const pkgSetupLines = loadedPkgNames
    .map((p) => `$pkgs[${JSON.stringify(p.name)}] = __pkg_${p.safeName};`)
    .join('\n');

  const setupCode = SETUP_CODE_TEMPLATE.replace('%%PKG_SETUP%%', pkgSetupLines);
  await (await isolate.compileScript(setupCode, { filename: 'setup.js' })).run(context, { timeout: 5000 });

  return { isolate, context };
}

function extractThrowInfo(error) {
  try {
    const parsed = JSON.parse(error.message);
    if (parsed && parsed.__userThrow) return parsed;
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

function postError(id, error) {
  const payload = buildErrorPayload(error);
  parentPort.postMessage({
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
  const { id, code, pkgSources, snapshot, memoryLimitMb } = msg;
  const timeoutMs = Math.max(1, Math.trunc(Number(msg.timeoutMs) || 30000));
  const { isolate, context } = await createIsolateContext(id, pkgSources, snapshot, memoryLimitMb);

  try {
    const wrappedCode = `
(async () => {
  "use strict";
  ${code}
})().then((__result) => __extractResult(__result, __result === undefined));
`;

    const script = await isolate.compileScript(wrappedCode, { filename: 'handler.js' });
    const jsonStr = await script.run(context, { timeout: timeoutMs, promise: true });

    const result = JSON.parse(jsonStr);
    parentPort.postMessage({ type: 'result', id, success: true, ...result });
  } catch (error) {
    postError(id, error);
  } finally {
    if (!isolate.isDisposed) {
      isolate.dispose();
    }
  }
}

async function handleExecuteBatch(msg) {
  const { id, codeBlocks, pkgSources, snapshot, memoryLimitMb } = msg;
  const totalTimeoutMs = Math.max(1, Math.trunc(Number(msg.timeoutMs) || 30000));
  const { isolate, context } = await createIsolateContext(id, pkgSources, snapshot, memoryLimitMb);

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
        const script = await isolate.compileScript(wrappedBlock, {
          filename: `${blockLabel}.js`,
        });
        const rawResult = await script.run(context, { timeout: totalTimeoutMs, promise: true });

        if (blockType === 'preHook' && typeof rawResult === 'string') {
          const parsed = JSON.parse(rawResult);
          if (parsed.__shortCircuit) {
            const out = await extractFinalResult(isolate, context, parsed.value);
            parentPort.postMessage({ type: 'result', id, success: true, ...out, shortCircuit: true });
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
      const statusScript = await isolate.compileScript(setStatusCode, { filename: 'set-status.js' });
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
        const script = await isolate.compileScript(wrappedBlock, {
          filename: `${blockLabel}.js`,
        });
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
        const extractScript = await isolate.compileScript(extractCode, { filename: 'extract-error-ctx.js' });
        const json = await extractScript.run(context, { timeout: 5000 });
        ctxChanges = JSON.parse(json);
      } catch {}
      parentPort.postMessage({
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
    parentPort.postMessage({ type: 'result', id, success: true, ...out });
  } catch (error) {
    postError(id, error);
  } finally {
    if (!isolate.isDisposed) {
      isolate.dispose();
    }
  }
}

async function extractFinalResult(isolate, context, overrideValue) {
  const callCode = overrideValue !== undefined
    ? `__extractResult(${JSON.stringify(overrideValue)}, false)`
    : '__extractResult($ctx.$data, $ctx.$data === undefined)';
  const script = await isolate.compileScript(callCode, { filename: 'extract.js' });
  const jsonStr = await script.run(context, { timeout: 5000 });
  return JSON.parse(jsonStr);
}
