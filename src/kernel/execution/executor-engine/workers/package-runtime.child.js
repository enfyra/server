'use strict';

const moduleCache = new Map();
const handles = new Map();
const pendingCallbackCalls = new Map();
let handleCounter = 0;
let callbackCounter = 0;

async function importPackage(pkg) {
  const cacheKey = pkg.fileUrl || pkg.name;
  if (moduleCache.has(cacheKey)) return moduleCache.get(cacheKey);
  const specifier = pkg.name && pkg.name.startsWith('node:') ? pkg.name : (pkg.fileUrl || pkg.name);
  if (!specifier) throw new Error(`Package "${pkg.name}" cannot be imported by package runtime`);
  const mod = await import(specifier);
  moduleCache.set(cacheKey, mod);
  return mod;
}

function resolvePackageTarget(root, path) {
  if (!path || path.length === 0) {
    return {
      target: root && root.default !== undefined ? root.default : root,
      receiver: undefined,
    };
  }

  let current = root;
  let receiver;
  for (let i = 0; i < path.length; i++) {
    const key = path[i];
    if ((current === undefined || current === null) && i === 0 && root.default !== undefined) {
      current = root.default;
    }
    if (current !== undefined && current !== null && key in current) {
      receiver = current;
      current = current[key];
      continue;
    }
    if (i === 0 && root.default !== undefined && root.default !== current && key in root.default) {
      receiver = root.default;
      current = root.default[key];
      continue;
    }
    throw new Error(`Package export not found: ${path.join('.')}`);
  }

  return { target: current, receiver };
}

function serializePackageValue(taskId, value) {
  if (value === undefined || value === null) return value;
  const valueType = typeof value;
  if (valueType === 'string' || valueType === 'number' || valueType === 'boolean') return value;
  if (Array.isArray(value)) return value;
  if (valueType === 'object' && isPlainObject(value)) {
    try {
      JSON.stringify(value);
      return value;
    } catch {}
  }
  const handleId = `${taskId}:pkg_${++handleCounter}`;
  handles.set(handleId, value);
  return { __pkgHandle: handleId };
}

function isPlainObject(value) {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return false;
  return !Object.values(value).some((item) => typeof item === 'function');
}

function createPackageHandle(taskId, value) {
  const handleId = `${taskId}:pkg_${++handleCounter}`;
  handles.set(handleId, value);
  return { __pkgHandle: handleId };
}

function callIsolateCallback(taskId, callbackId, args) {
  return new Promise((resolve, reject) => {
    const id = `cb_${++callbackCounter}`;
    pendingCallbackCalls.set(id, { resolve, reject });
    try {
      process.send?.({
        type: 'pkgCallbackCall',
        id,
        taskId,
        callbackId,
        argsJson: JSON.stringify(args),
      });
    } catch (error) {
      pendingCallbackCalls.delete(id);
      reject(error);
    }
  });
}

function deserializePackageArg(taskId, value) {
  if (!value || typeof value !== 'object') return value;
  if (value.__fnRef) {
    return (...args) => callIsolateCallback(taskId, value.__fnRef, args);
  }
  if (value.__pkgHandleArg) {
    const handle = handles.get(value.__pkgHandleArg);
    if (!handle) throw new Error(`Package argument handle not found: ${value.__pkgHandleArg}`);
    return handle;
  }
  if (value.__date) {
    return new Date(value.__date);
  }
  if (value.__typedArray) {
    const Ctor = globalThis[value.__typedArray] || Uint8Array;
    return new Ctor(value.data || []);
  }
  if (value.__arrayBuffer) {
    return Uint8Array.from(value.__arrayBuffer).buffer;
  }
  if (Array.isArray(value)) {
    return value.map((item) => deserializePackageArg(taskId, item));
  }
  const out = {};
  for (const [key, item] of Object.entries(value)) {
    out[key] = deserializePackageArg(taskId, item);
  }
  return out;
}

function deserializePackageArgs(taskId, argsJson) {
  return JSON.parse(argsJson || '[]').map((arg) =>
    deserializePackageArg(taskId, arg),
  );
}

async function executePackageCall(msg) {
  const args = deserializePackageArgs(msg.taskId, msg.argsJson);

  if (msg.op === 'handleCall') {
    const root = handles.get(msg.handleId);
    if (!root) throw new Error(`Package handle not found: ${msg.handleId}`);
    const { target, receiver } = resolvePackageTarget(root, msg.path || []);
    if (typeof target !== 'function') {
      return serializePackageValue(msg.taskId, target);
    }
    const result = await target.apply(receiver || root, args);
    return serializePackageValue(msg.taskId, result);
  }

  const mod = await importPackage(msg.package);
  const { target, receiver } = resolvePackageTarget(mod, msg.path || []);

  if (msg.op === 'construct') {
    if (typeof target !== 'function') {
      throw new Error(`Package export is not constructable: ${msg.packageName}.${(msg.path || []).join('.')}`);
    }
    return createPackageHandle(msg.taskId, new target(...args));
  }

  if (typeof target === 'function') {
    return serializePackageValue(msg.taskId, await target.apply(receiver, args));
  }

  return serializePackageValue(msg.taskId, target);
}

function releaseTask(taskId) {
  for (const handleId of handles.keys()) {
    if (handleId.startsWith(`${taskId}:`)) handles.delete(handleId);
  }
}

process.on('message', async (msg) => {
  if (!msg) return;

  if (msg.type === 'pkgCallbackResult' || msg.type === 'pkgCallbackError') {
    const pending = pendingCallbackCalls.get(msg.id);
    if (!pending) return;
    pendingCallbackCalls.delete(msg.id);
    if (msg.type === 'pkgCallbackResult') {
      pending.resolve(msg.value);
    } else {
      pending.reject(new Error(msg.error?.message || 'Package callback failed'));
    }
    return;
  }

  if (!msg.op) return;

  try {
    if (msg.op === 'releaseTask') {
      releaseTask(msg.taskId);
      if (!msg.fire) process.send?.({ id: msg.id, ok: true, value: true });
      return;
    }

    const value = await executePackageCall(msg);
    process.send?.({ id: msg.id, ok: true, value });
  } catch (error) {
    const context = [
      msg.op ? `op=${msg.op}` : null,
      msg.packageName ? `package=${msg.packageName}` : null,
      msg.handleId ? `handle=${msg.handleId}` : null,
      Array.isArray(msg.path) ? `path=${msg.path.join('.') || '<root>'}` : null,
    ].filter(Boolean).join(' ');
    process.send?.({
      id: msg.id,
      ok: false,
      error: {
        message: context ? `${error.message} (${context})` : error.message,
        stack: error.stack,
      },
    });
  }
});
