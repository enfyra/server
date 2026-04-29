'use strict';

const { createPackageRuntimeCodec } = require('./package-runtime-codec');

const moduleCache = new Map();
const handles = new Map();
const pendingCallbackCalls = new Map();
let handleCounter = 0;
let callbackCounter = 0;

const {
  createPackageHandle,
  deserializePackageArg,
  deserializePackageArgs,
  serializePackageValue,
} = createPackageRuntimeCodec({
  handles,
  createHandleId: (taskId) => `${taskId}:pkg_${++handleCounter}`,
  callIsolateCallback,
});

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

function callIsolateCallback(taskId, callbackId, args) {
  return new Promise((resolve, reject) => {
    const id = `cb_${++callbackCounter}`;
    pendingCallbackCalls.set(id, { resolve, reject, taskId });
    try {
      process.send?.({
        type: 'pkgCallbackCall',
        id,
        taskId,
        callbackId,
        argsJson: JSON.stringify(args.map((arg) => serializePackageValue(taskId, arg))),
      });
    } catch (error) {
      pendingCallbackCalls.delete(id);
      reject(error);
    }
  });
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

  if (msg.op === 'handleGet') {
    const root = handles.get(msg.handleId);
    if (!root) throw new Error(`Package handle not found: ${msg.handleId}`);
    const { target, receiver } = resolvePackageTarget(root, msg.path || []);
    return serializePackageValue(
      msg.taskId,
      typeof target === 'function' && receiver ? target.bind(receiver) : target,
    );
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
      pending.resolve(deserializePackageArg(pending.taskId, msg.value));
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
