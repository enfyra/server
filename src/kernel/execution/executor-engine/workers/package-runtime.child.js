'use strict';

const moduleCache = new Map();
const handles = new Map();
let handleCounter = 0;

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
  if (valueType === 'object') {
    try {
      JSON.stringify(value);
      return value;
    } catch {}
  }
  const handleId = `${taskId}:pkg_${++handleCounter}`;
  handles.set(handleId, value);
  return { __pkgHandle: handleId };
}

function createPackageHandle(taskId, value) {
  const handleId = `${taskId}:pkg_${++handleCounter}`;
  handles.set(handleId, value);
  return { __pkgHandle: handleId };
}

async function executePackageCall(msg) {
  const args = JSON.parse(msg.argsJson || '[]');

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
  const { target } = resolvePackageTarget(mod, msg.path || []);

  if (msg.op === 'construct') {
    if (typeof target !== 'function') {
      throw new Error(`Package export is not constructable: ${msg.packageName}.${(msg.path || []).join('.')}`);
    }
    return createPackageHandle(msg.taskId, new target(...args));
  }

  if (typeof target === 'function') {
    return serializePackageValue(msg.taskId, await target(...args));
  }

  return serializePackageValue(msg.taskId, target);
}

function releaseTask(taskId) {
  for (const handleId of handles.keys()) {
    if (handleId.startsWith(`${taskId}:`)) handles.delete(handleId);
  }
}

process.on('message', async (msg) => {
  if (!msg || !msg.op) return;

  try {
    if (msg.op === 'releaseTask') {
      releaseTask(msg.taskId);
      if (!msg.fire) process.send?.({ id: msg.id, ok: true, value: true });
      return;
    }

    const value = await executePackageCall(msg);
    process.send?.({ id: msg.id, ok: true, value });
  } catch (error) {
    process.send?.({
      id: msg.id,
      ok: false,
      error: {
        message: error.message,
        stack: error.stack,
      },
    });
  }
});
