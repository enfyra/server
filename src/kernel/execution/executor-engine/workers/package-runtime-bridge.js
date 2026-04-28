'use strict';

const { fork } = require('child_process');
const path = require('path');

function createPackageRuntimeBridge({ workerDir, activeTaskContexts }) {
  const taskPackages = new Map();
  const pendingPackageRuntimeCalls = new Map();
  let packageRuntimeChild = null;
  let packageRuntimeCounter = 0;

  function rejectPendingPackageRuntimeCalls(error) {
    for (const pending of pendingPackageRuntimeCalls.values()) {
      pending.reject(error);
    }
    pendingPackageRuntimeCalls.clear();
  }

  function parseCallbackResult(resultJson) {
    const result = typeof resultJson === 'string' ? JSON.parse(resultJson) : resultJson;
    if (result?.error) throw new Error(result.error);
    return result?.value;
  }

  async function executePackageCallback(msg) {
    const context = activeTaskContexts.get(msg.taskId);
    if (!context) throw new Error(`Package callback context not found: ${msg.taskId}`);
    const callbackId = JSON.stringify(msg.callbackId);
    const argsJson = JSON.stringify(msg.argsJson || '[]');
    const result = await context.evalClosure(
      `return __invokeRuntimeCallback(${callbackId}, ${argsJson});`,
      [],
      { arguments: { copy: true }, result: { promise: true, copy: true } },
    );
    return parseCallbackResult(result);
  }

  function getPackageRuntimeChild() {
    if (packageRuntimeChild && packageRuntimeChild.connected) {
      return packageRuntimeChild;
    }

    const childPath = path.join(workerDir, 'package-runtime.child.js');
    const child = fork(childPath, [], {
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
    });
    packageRuntimeChild = child;

    child.on('message', (msg) => {
      if (!msg || !msg.id) return;
      if (msg.type === 'pkgCallbackCall') {
        executePackageCallback(msg)
          .then((value) => {
            try {
              child.send({ type: 'pkgCallbackResult', id: msg.id, value });
            } catch {}
          })
          .catch((error) => {
            try {
              child.send({
                type: 'pkgCallbackError',
                id: msg.id,
                error: { message: error.message, stack: error.stack },
              });
            } catch {}
          });
        return;
      }
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
        op: data.kind === 'get' ? 'handleGet' : 'handleCall',
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

  function setTaskPackages(taskId, packages) {
    taskPackages.set(taskId, packages);
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

  return {
    cleanupTaskPackages,
    executePackageRuntimeCall,
    setTaskPackages,
    shutdownPackageRuntime,
  };
}

module.exports = {
  createPackageRuntimeBridge,
};
