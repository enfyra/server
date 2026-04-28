'use strict';

function createPackageRuntimeCodec({ handles, createHandleId, callIsolateCallback }) {
  function serializePackageValue(taskId, value) {
    if (value === undefined || value === null) return value;
    const valueType = typeof value;
    if (valueType === 'string' || valueType === 'number' || valueType === 'boolean') return value;
    if (value instanceof Date) return { __date: value.toISOString() };
    if (value instanceof Set) {
      return { __set: Array.from(value.values()).map((item) => serializePackageValue(taskId, item)) };
    }
    if (value instanceof Map) {
      return {
        __map: Array.from(value.entries()).map(([key, item]) => [
          serializePackageValue(taskId, key),
          serializePackageValue(taskId, item),
        ]),
      };
    }
    if (value instanceof RegExp) {
      return { __regexp: { source: value.source, flags: value.flags } };
    }
    if (value instanceof Error) {
      return {
        __error: {
          name: value.name,
          message: value.message,
          stack: value.stack,
        },
      };
    }
    if (typeof URL !== 'undefined' && value instanceof URL) {
      return { __url: value.href };
    }
    if (typeof URLSearchParams !== 'undefined' && value instanceof URLSearchParams) {
      return { __urlSearchParams: value.toString() };
    }
    if (
      (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) ||
      value._isBuffer === true
    ) {
      return createPackageHandle(taskId, value);
    }
    if (ArrayBuffer.isView(value)) {
      return {
        __typedArray: value.constructor?.name || 'Uint8Array',
        data: Array.from(value),
      };
    }
    if (value instanceof ArrayBuffer) {
      return { __arrayBuffer: Array.from(new Uint8Array(value)) };
    }
    if (Array.isArray(value)) {
      return value.map((item) => serializePackageValue(taskId, item));
    }
    if (valueType === 'object' && isPlainObject(value)) {
      const out = {};
      for (const [key, item] of Object.entries(value)) {
        out[key] = serializePackageValue(taskId, item);
      }
      return out;
    }
    return createPackageHandle(taskId, value);
  }

  function createPackageHandle(taskId, value) {
    const handleId = createHandleId(taskId);
    handles.set(handleId, value);
    return { __pkgHandle: handleId };
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
    if (value.__date) return new Date(value.__date);
    if (value.__set) {
      return new Set(value.__set.map((item) => deserializePackageArg(taskId, item)));
    }
    if (value.__map) {
      return new Map(
        value.__map.map(([key, item]) => [
          deserializePackageArg(taskId, key),
          deserializePackageArg(taskId, item),
        ]),
      );
    }
    if (value.__regexp) {
      return new RegExp(value.__regexp.source, value.__regexp.flags || '');
    }
    if (value.__error) {
      const error = new Error(value.__error.message || '');
      error.name = value.__error.name || 'Error';
      if (value.__error.stack) error.stack = value.__error.stack;
      return error;
    }
    if (value.__url) return new URL(value.__url);
    if (value.__urlSearchParams) return new URLSearchParams(value.__urlSearchParams);
    if (value.__blob) {
      return new Blob([Uint8Array.from(value.__blob.data || [])], {
        type: value.__blob.type || '',
      });
    }
    if (value.__file) {
      const blobParts = [Uint8Array.from(value.__file.data || [])];
      const options = {
        type: value.__file.type || '',
        lastModified: value.__file.lastModified,
      };
      if (typeof File !== 'undefined') {
        return new File(blobParts, value.__file.name || 'file', options);
      }
      return new Blob(blobParts, options);
    }
    if (value.__formData) {
      const form = new FormData();
      for (const [key, item] of value.__formData) {
        form.append(key, deserializePackageArg(taskId, item));
      }
      return form;
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

  return {
    createPackageHandle,
    deserializePackageArg,
    deserializePackageArgs,
    serializePackageValue,
  };
}

function isPlainObject(value) {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return false;
  return !Object.values(value).some((item) => typeof item === 'function');
}

module.exports = {
  createPackageRuntimeCodec,
};
