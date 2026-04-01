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

async function handleExecute(msg) {
  const { id, code, pkgSources, snapshot, memoryLimitMb } = msg;
  const timeoutMs = Math.max(1, Math.trunc(Number(msg.timeoutMs) || 30000));
  const isolate = new ivm.Isolate({ memoryLimit: memoryLimitMb });

  try {
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
      '__repoCallRef',
      new ivm.Reference(async (table, method, argsJson) =>
        callMain(id, 'repoCall', { table, method, argsJson }),
      ),
    );

    await jail.set(
      '__helpersCallRef',
      new ivm.Reference(async (name, argsJson) => callMain(id, 'helpersCall', { name, argsJson })),
    );

    await jail.set(
      '__socketCallRef',
      new ivm.Reference((method, argsJson) => {
        parentPort.postMessage({
          type: 'socketCall',
          id,
          callId: `socket_${++callbackCounter}`,
          method,
          argsJson,
        });
      }),
    );

    await jail.set(
      '__cacheCallRef',
      new ivm.Reference(async (method, argsJson) => callMain(id, 'cacheCall', { method, argsJson })),
    );

    const pkgSetupLines = loadedPkgNames
      .map((p) => `$pkgs[${JSON.stringify(p.name)}] = __pkg_${p.safeName};`)
      .join('\n');

    const setupCode = `
"use strict";
const $ctx = __snapshot;
const $pkgs = {};
${pkgSetupLines}
$ctx.$pkgs = $pkgs;

const __applyOpts = { result: { promise: true, copy: true } };

function __parseMainThreadResult(s) {
  const w = JSON.parse(s);
  if (w !== null && typeof w === 'object' && w.__e === 'u') return undefined;
  if (w !== null && typeof w === 'object' && w.__e === 'v') return w.d;
  throw new Error('Invalid main-thread bridge payload');
}

$ctx.$repos = new Proxy({}, {
  get: (_, table) => new Proxy({}, {
    get: (_, method) => async (...args) => {
      const r = await __repoCallRef.apply(undefined, [String(table), String(method), JSON.stringify(args)], __applyOpts);
      return __parseMainThreadResult(r);
    }
  })
});

$ctx.$helpers = new Proxy({}, {
  get: (_, name) => {
    const basePath = String(name);
    const fn = async (...args) => {
      const r = await __helpersCallRef.apply(undefined, [basePath, JSON.stringify(args)], __applyOpts);
      return __parseMainThreadResult(r);
    };
    return new Proxy(fn, {
      get: (_, subName) => async (...args) => {
        const r = await __helpersCallRef.apply(undefined, [basePath + '.' + String(subName), JSON.stringify(args)], __applyOpts);
        return __parseMainThreadResult(r);
      }
    });
  }
});

$ctx.$socket = new Proxy({}, {
  get: (_, method) => (...args) => {
    __socketCallRef.applyIgnored(undefined, [String(method), JSON.stringify(args)]);
  }
});

$ctx.$cache = new Proxy({}, {
  get: (_, method) => async (...args) => {
    const r = await __cacheCallRef.apply(undefined, [String(method), JSON.stringify(args)], __applyOpts);
    return __parseMainThreadResult(r);
  }
});

$ctx.$throw = new Proxy({}, {
  get: (_, status) => (message, details) => {
    const err = new Error(message);
    err.statusCode = parseInt(String(status));
    err.details = details;
    throw err;
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
  if ($pkgs[name] === undefined) throw new Error('Module "' + name + '" is not available. Install it via Settings \u2192 Packages');
  return $pkgs[name];
};

const console = {
  log: (...a) => $ctx.$logs(...a),
  warn: (...a) => $ctx.$logs(...a),
  error: (...a) => $ctx.$logs(...a),
  info: (...a) => $ctx.$logs(...a),
};
`;

    await (await isolate.compileScript(setupCode, { filename: 'setup.js' })).run(context, { timeout: 5000 });

    const wrappedCode = `
(async () => {
  "use strict";
  ${code}
})().then((__result) => {
  function safeClone(v) {
    if (v === undefined) return undefined;
    try { return JSON.parse(JSON.stringify(v)); }
    catch { return null; }
  }
  const valueAbsent = __result === undefined;
  const out = {
    valueAbsent,
    ctxChanges: {
      $body: safeClone($ctx.$body),
      $query: safeClone($ctx.$query),
      $params: safeClone($ctx.$params),
      $data: safeClone($ctx.$data),
      $statusCode: $ctx.$statusCode,
      $share: safeClone($ctx.$share),
    }
  };
  if (!valueAbsent) out.value = safeClone(__result);
  return JSON.stringify(out);
});
`;

    const script = await isolate.compileScript(wrappedCode, { filename: 'handler.js' });
    const jsonStr = await script.run(context, { timeout: timeoutMs, promise: true });

    const result = JSON.parse(jsonStr);
    parentPort.postMessage({ type: 'result', id, success: true, ...result });
  } catch (error) {
    parentPort.postMessage({
      type: 'result',
      id,
      success: false,
      error: {
        message: error.message,
        statusCode: error.statusCode || null,
        stack: error.stack,
        code: error.code,
      },
    });
  } finally {
    if (!isolate.isDisposed) {
      isolate.dispose();
    }
  }
}
