'use strict';

const SETUP_CODE_TEMPLATE = `
"use strict";
let $ctx = {};
let $pkgs = {};
let __taskId = null;
let __logs = [];
let __runtimeCallbackCounter = 0;
let __runtimeCallbacks = new Map();

const __applyOpts = { result: { promise: true, copy: true } };

function __unwrapMainThreadPayload(v) {
  if (v !== null && typeof v === 'object' && v.__e === 'u') return undefined;
  if (v !== null && typeof v === 'object' && v.__e === 'v') return __unwrapMainThreadPayload(v.d);
  if (v !== null && typeof v === 'object' && v.__date) return new Date(v.__date);
  if (v !== null && typeof v === 'object' && v.__set) {
    return new Set(v.__set.map(__unwrapMainThreadPayload));
  }
  if (v !== null && typeof v === 'object' && v.__map) {
    return new Map(v.__map.map(([key, value]) => [
      __unwrapMainThreadPayload(key),
      __unwrapMainThreadPayload(value),
    ]));
  }
  if (v !== null && typeof v === 'object' && v.__regexp) {
    return new RegExp(v.__regexp.source, v.__regexp.flags || '');
  }
  if (v !== null && typeof v === 'object' && v.__error) {
    const error = new Error(v.__error.message || '');
    error.name = v.__error.name || 'Error';
    if (v.__error.stack) error.stack = v.__error.stack;
    return error;
  }
  if (v !== null && typeof v === 'object' && v.__url) return new URL(v.__url);
  if (v !== null && typeof v === 'object' && v.__urlSearchParams) {
    return new URLSearchParams(v.__urlSearchParams);
  }
  if (v !== null && typeof v === 'object' && v.__blob) {
    return new Blob([Uint8Array.from(v.__blob.data || [])], {
      type: v.__blob.type || '',
    });
  }
  if (v !== null && typeof v === 'object' && v.__file) {
    const parts = [Uint8Array.from(v.__file.data || [])];
    const options = {
      type: v.__file.type || '',
      lastModified: v.__file.lastModified,
    };
    return typeof File !== 'undefined'
      ? new File(parts, v.__file.name || 'file', options)
      : new Blob(parts, options);
  }
  if (v !== null && typeof v === 'object' && v.__formData) {
    const form = new FormData();
    for (const [key, value] of v.__formData) {
      form.append(key, __unwrapMainThreadPayload(value));
    }
    return form;
  }
  if (v !== null && typeof v === 'object' && v.__typedArray) {
    const Ctor = globalThis[v.__typedArray] || Uint8Array;
    return new Ctor(v.data || []);
  }
  if (v !== null && typeof v === 'object' && v.__arrayBuffer) {
    return Uint8Array.from(v.__arrayBuffer).buffer;
  }
  if (v !== null && typeof v === 'object' && v.__pkgHandle) {
    return __createPkgHandleProxy(Promise.resolve(v.__pkgHandle), []);
  }
  if (Array.isArray(v)) return v.map(__unwrapMainThreadPayload);
  if (v !== null && typeof v === 'object' && !v.__pkgHandle) {
    const out = {};
    for (const [key, value] of Object.entries(v)) {
      out[key] = __unwrapMainThreadPayload(value);
    }
    return out;
  }
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

async function __encodeRuntimeArg(value) {
  if (typeof value === 'function') {
    if (value.__pkgHandlePromise) {
      return { __pkgHandleArg: await value.__pkgHandlePromise };
    }
    if (typeof value.then === 'function') {
      return __encodeRuntimeArg(await value);
    }
    const id = 'fn_' + (++__runtimeCallbackCounter);
    __runtimeCallbacks.set(id, value);
    return { __fnRef: id };
  }
  if (value instanceof Date) return { __date: value.toISOString() };
  if (value instanceof Set) {
    const items = [];
    for (const item of value.values()) items.push(await __encodeRuntimeArg(item));
    return { __set: items };
  }
  if (value instanceof Map) {
    const entries = [];
    for (const [key, item] of value.entries()) {
      entries.push([
        await __encodeRuntimeArg(key),
        await __encodeRuntimeArg(item),
      ]);
    }
    return { __map: entries };
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
  if (typeof URL !== 'undefined' && value instanceof URL) return { __url: value.href };
  if (typeof URLSearchParams !== 'undefined' && value instanceof URLSearchParams) {
    return { __urlSearchParams: value.toString() };
  }
  if (typeof File !== 'undefined' && value instanceof File) {
    return {
      __file: {
        name: value.name,
        type: value.type,
        lastModified: value.lastModified,
        data: Array.from(new Uint8Array(await value.arrayBuffer())),
      },
    };
  }
  if (typeof Blob !== 'undefined' && value instanceof Blob) {
    return {
      __blob: {
        type: value.type,
        data: Array.from(new Uint8Array(await value.arrayBuffer())),
      },
    };
  }
  if (typeof FormData !== 'undefined' && value instanceof FormData) {
    const entries = [];
    for (const [key, item] of value.entries()) {
      entries.push([key, await __encodeRuntimeArg(item)]);
    }
    return { __formData: entries };
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
  if (Array.isArray(value)) return Promise.all(value.map(__encodeRuntimeArg));
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value)) {
      out[key] = await __encodeRuntimeArg(value[key]);
    }
    return out;
  }
  return value;
}

async function __stringifyRuntimeArgs(args) {
  return JSON.stringify(await Promise.all(args.map(__encodeRuntimeArg)));
}

async function __invokeRuntimeCallback(id, argsJson) {
  const fn = __runtimeCallbacks.get(id);
  if (typeof fn !== 'function') {
    return JSON.stringify({ error: 'Package callback not found: ' + id });
  }
  try {
    const value = await fn(...JSON.parse(argsJson || '[]').map(__unwrapMainThreadPayload));
    return JSON.stringify({ value: await __encodeRuntimeArg(value) });
  } catch (error) {
    return JSON.stringify({ error: error && error.message ? error.message : String(error) });
  }
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

if (typeof TextEncoder === 'undefined') {
  globalThis.TextEncoder = class TextEncoder {
    encode(input = '') {
      const encoded = unescape(encodeURIComponent(String(input)));
      const out = new Uint8Array(encoded.length);
      for (let i = 0; i < encoded.length; i++) out[i] = encoded.charCodeAt(i);
      return out;
    }
  };
}

if (typeof TextDecoder === 'undefined') {
  globalThis.TextDecoder = class TextDecoder {
    decode(input = new Uint8Array()) {
      const bytes = ArrayBuffer.isView(input) ? input : new Uint8Array(input);
      let binary = '';
      for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
      return decodeURIComponent(escape(binary));
    }
  };
}

if (typeof FormData === 'undefined') {
  globalThis.FormData = class FormData {
    constructor() {
      this.__entries = [];
    }

    append(name, value) {
      this.__entries.push([String(name), value]);
    }

    get(name) {
      const key = String(name);
      const match = this.__entries.find(([entryName]) => entryName === key);
      return match ? match[1] : null;
    }

    entries() {
      return this.__entries[Symbol.iterator]();
    }

    [Symbol.iterator]() {
      return this.entries();
    }
  };
}

if (typeof URLSearchParams === 'undefined') {
  globalThis.URLSearchParams = class URLSearchParams {
    constructor(input = '') {
      this.__entries = [];
      const inputText = String(input);
      const text = inputText.startsWith('?') ? inputText.slice(1) : inputText;
      if (!text) return;
      for (const part of text.split('&')) {
        if (!part) continue;
        const [key, value = ''] = part.split('=');
        this.append(decodeURIComponent(key), decodeURIComponent(value));
      }
    }

    append(name, value) {
      this.__entries.push([String(name), String(value)]);
    }

    get(name) {
      const key = String(name);
      const match = this.__entries.find(([entryName]) => entryName === key);
      return match ? match[1] : null;
    }

    toString() {
      return this.__entries
        .map(([key, value]) => encodeURIComponent(key) + '=' + encodeURIComponent(value))
        .join('&');
    }

    entries() {
      return this.__entries[Symbol.iterator]();
    }

    [Symbol.iterator]() {
      return this.entries();
    }
  };
}

if (typeof URL === 'undefined') {
  globalThis.URL = class URL {
    constructor(input) {
      this.href = String(input);
      const schemeIndex = this.href.indexOf('://');
      if (schemeIndex >= 0) {
        const hostStart = schemeIndex + 3;
        const hostEndCandidates = ['/', '?', '#']
          .map((token) => this.href.indexOf(token, hostStart))
          .filter((index) => index >= 0);
        const hostEnd = hostEndCandidates.length ? Math.min(...hostEndCandidates) : this.href.length;
        this.host = this.href.slice(hostStart, hostEnd);
      } else {
        this.host = '';
      }
      const query = this.href.includes('?') ? this.href.slice(this.href.indexOf('?') + 1).split('#')[0] : '';
      this.search = query ? '?' + query : '';
      this.searchParams = new URLSearchParams(query);
    }

    toString() {
      return this.href;
    }
  };
}

function __createPkgHandleProxy(handlePromise, path) {
  const fn = function (...args) {
    return __createAsyncPkgResultProxy(
      handlePromise.then((handleId) =>
        __stringifyRuntimeArgs(args).then((argsJson) =>
          __call('pkgCall', JSON.stringify({ handleId, path, argsJson, kind: 'call' })),
        ),
      ),
      [],
    );
  };
  return new Proxy(fn, {
    get: (_, prop) => {
      if (prop === 'then') return undefined;
      if (prop === '__pkgHandle') return undefined;
      if (prop === '__pkgHandlePromise') return handlePromise;
      return __createAsyncPkgResultProxy(
        handlePromise.then((handleId) =>
          __call('pkgCall', JSON.stringify({
            handleId,
            path: path.concat(String(prop)),
            kind: 'get',
          })),
        ),
        [],
      );
    },
    apply: (_target, _thisArg, args) =>
      __createAsyncPkgResultProxy(
        handlePromise.then((handleId) =>
          __stringifyRuntimeArgs(args).then((argsJson) =>
            __call('pkgCall', JSON.stringify({ handleId, path, argsJson, kind: 'call' })),
          ),
        ),
        [],
      )
  });
}

function __resolvePkgResultPath(base, path) {
  let current = base;
  let receiver = undefined;
  for (const key of path) {
    if (current === undefined || current === null) {
      throw new Error('Package result property not found: ' + path.join('.'));
    }
    receiver = current;
    current = current[key];
  }
  return { target: current, receiver };
}

function __createAsyncPkgResultProxy(valuePromise, path) {
  const resolveValue = () => valuePromise.then(__wrapPkgValue);
  const resolvePath = () =>
    resolveValue().then((base) => __resolvePkgResultPath(base, path));
  const promiseForPath = () =>
    path.length === 0 ? resolveValue() : resolvePath().then(({ target }) => target);

  const fn = function (...args) {
    return __createAsyncPkgResultProxy(
      resolvePath().then(({ target, receiver }) => {
        if (typeof target !== 'function') {
          throw new Error('Package result is not callable: ' + (path.join('.') || '<root>'));
        }
        return Reflect.apply(target, receiver, args);
      }),
      [],
    );
  };

  return new Proxy(fn, {
    get: (_target, prop) => {
      if (prop === '__pkgHandle') return undefined;
      if (prop === '__pkgHandlePromise') return undefined;
      if (prop === 'then') {
        const promise = promiseForPath();
        return promise.then.bind(promise);
      }
      if (prop === 'catch') {
        const promise = promiseForPath();
        return promise.catch.bind(promise);
      }
      if (prop === 'finally') {
        const promise = promiseForPath();
        return promise.finally.bind(promise);
      }
      return __createAsyncPkgResultProxy(valuePromise, path.concat(String(prop)));
    },
    apply: (_target, _thisArg, args) =>
      __createAsyncPkgResultProxy(
        resolvePath().then(({ target, receiver }) => {
          if (typeof target !== 'function') {
            throw new Error('Package result is not callable: ' + (path.join('.') || '<root>'));
          }
          return Reflect.apply(target, receiver, args);
        }),
        [],
      )
  });
}

function __createAsyncMainResultProxy(valuePromise, path) {
  const resolveValue = () => valuePromise;
  const resolvePath = () =>
    resolveValue().then((base) => __resolvePkgResultPath(base, path));
  const promiseForPath = () =>
    path.length === 0 ? resolveValue() : resolvePath().then(({ target }) => target);

  const fn = function (...args) {
    return __createAsyncMainResultProxy(
      resolvePath().then(({ target, receiver }) => {
        if (typeof target !== 'function') {
          throw new Error('Async result is not callable: ' + (path.join('.') || '<root>'));
        }
        return Reflect.apply(target, receiver, args);
      }),
      [],
    );
  };

  return new Proxy(fn, {
    get: (_target, prop) => {
      if (prop === 'then') {
        const promise = promiseForPath();
        return promise.then.bind(promise);
      }
      if (prop === 'catch') {
        const promise = promiseForPath();
        return promise.catch.bind(promise);
      }
      if (prop === 'finally') {
        const promise = promiseForPath();
        return promise.finally.bind(promise);
      }
      return __createAsyncMainResultProxy(valuePromise, path.concat(String(prop)));
    },
    apply: (_target, _thisArg, args) =>
      __createAsyncMainResultProxy(
        resolvePath().then(({ target, receiver }) => {
          if (typeof target !== 'function') {
            throw new Error('Async result is not callable: ' + (path.join('.') || '<root>'));
          }
          return Reflect.apply(target, receiver, args);
        }),
        [],
      )
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
    return __createAsyncPkgResultProxy(
      __stringifyRuntimeArgs(args).then((argsJson) =>
        __call('pkgCall', JSON.stringify({ packageName, path, argsJson, kind: 'call' })),
      ),
      [],
    );
  };
  return new Proxy(fn, {
    get: (_, prop) => {
      if (prop === 'then') return undefined;
      return __createPkgProxy(packageName, path.concat(String(prop)));
    },
    apply: (_target, _thisArg, args) =>
      __createAsyncPkgResultProxy(
        __stringifyRuntimeArgs(args).then((argsJson) =>
          __call('pkgCall', JSON.stringify({ packageName, path, argsJson, kind: 'call' })),
        ),
        [],
      ),
    construct: (_target, args) => {
      const handlePromise = __stringifyRuntimeArgs(args)
        .then((argsJson) =>
          __call('pkgCall', JSON.stringify({ packageName, path, argsJson, kind: 'construct' })),
        )
        .then((value) => {
          if (value && value.__pkgHandlePromise) return value.__pkgHandlePromise;
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
  __runtimeCallbackCounter = 0;
  __runtimeCallbacks = new Map();
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
      const fn = (...args) =>
        __createAsyncMainResultProxy(
          __call('helpersCall', JSON.stringify({ name: basePath, argsJson: JSON.stringify(args) })),
          [],
        );
      return new Proxy(fn, {
        get: (_, subName) => (...args) =>
          __createAsyncMainResultProxy(
            __call('helpersCall', JSON.stringify({ name: basePath + '.' + String(subName), argsJson: JSON.stringify(args) })),
            [],
          )
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
  __runtimeCallbackCounter = 0;
  __runtimeCallbacks = new Map();
  return true;
}
`;


function buildSetupCode(pkgSetupLines) {
  return SETUP_CODE_TEMPLATE.replace('%%PKG_SETUP%%', pkgSetupLines);
}

module.exports = {
  buildSetupCode,
};
