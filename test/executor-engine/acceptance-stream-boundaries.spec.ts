import { IsolatedExecutorService } from '@enfyra/kernel';
import { EventEmitter } from 'node:events';

function executor() { return new IsolatedExecutorService({ packageCacheService: { getPackages: async () => [] }, packageCdnLoaderService: { getPackageSources: () => [] } }); }
function context() { return { $body: {}, $query: {}, $params: {}, $share: {}, $api: { request: {} } }; }

describe('acceptance stream and host boundaries', () => {
  it('resolves promised Readable.from values and closes a rejected source', async () => {
    const e = executor();
    try {
      const result = await e.run(`
        const { Readable } = require('stream');
        const first = await Readable.from([Promise.resolve('abc')])[Symbol.asyncIterator]().next();
        let closed = false;
        const source = { [Symbol.iterator]() { return { next() { return { done: false, value: Promise.reject('bad') }; }, return() { closed = true; return {done:true}; } }; } };
        let rejected = false;
        try { await Readable.from(source)[Symbol.asyncIterator]().next(); } catch { rejected = true; }
        return { value: first.value, isPromise: first.value instanceof Promise, closed, rejected };
      `, context(), 5000);
      expect(result).toEqual({ value: 'abc', isPromise: false, closed: true, rejected: true });
    } finally { e.onDestroy(); }
  });

  it('does not reacquire a tapped source after cancellation before consumption', async () => {
    const e = executor();
    try {
      const result = await e.run(`
        let opened = 0;
        const source = { __enfyraLocalReadable: true, __enfyraCancel: async () => {}, [Symbol.asyncIterator]() { opened++; return { next: async () => ({done:true}), return: async () => ({done:true}) }; } };
        const tap = $ctx.$streams.tap(source, {});
        await $ctx.$streams.cancel(tap);
        const next = await tap[Symbol.asyncIterator]().next();
        return {opened, done:next.done};
      `, context(), 5000);
      expect(result).toEqual({ opened: 0, done: true });
    } finally { e.onDestroy(); }
  });

  it('cleans callback listeners after synchronous serialization failure', async () => {
    const e = executor() as any;
    const worker = Object.assign(new EventEmitter(), { postMessage: async () => {} });
    try {
      await expect(e.hostIo.invokeRuntimeCallback(worker, 'task', 'cb', [1n])).rejects.toThrow();
      expect(worker.listenerCount('message')).toBe(0);
      expect(worker.listenerCount('exit')).toBe(0);
    } finally { e.onDestroy(); }
  });
});
