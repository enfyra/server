import { IsolatedExecutorService } from '@enfyra/kernel';

function createExecutor() {
  return new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] },
    packageCdnLoaderService: { getPackageSources: () => [] },
  });
}

function context() {
  return {
    $body: {},
    $query: {},
    $params: {},
    $share: {},
    $api: { request: {} },
  };
}

describe('executor result codec', () => {
  it('rejects inherited typed-array constructor names at the host boundary', () => {
    const executor = createExecutor() as any;
    try {
      for (const name of ['constructor', 'toString', '__proto__']) {
        expect(() => executor.hostIo.parseExecutorArgs(JSON.stringify([{ __typedArray: name, bytes: [] }]))).toThrow('Unsupported executor typed array');
      }
    } finally { executor.onDestroy(); }
  });

  it('preserves an own __proto__ field in a pre-hook short-circuit value', async () => {
    const executor = createExecutor();
    try {
      const { value: result } = await executor.runBatch([{ type: 'preHook', code: `return JSON.parse('{"__proto__":{"flag":true},"value":1}');` }], context(), 5_000);
      expect(Object.prototype.hasOwnProperty.call(result, '__proto__')).toBe(true);
      expect(result.__proto__).toEqual({ flag: true });
      expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
    } finally { executor.onDestroy(); }
  });
  it('preserves binary result views and their exact byte windows', async () => {
    const executor = createExecutor();

    try {
      const result = await executor.run(
        `
          const bytes = Uint8Array.from([1, 2, 3, 4]);
          return {
            arrayBuffer: bytes.buffer,
            typed: new Uint16Array(Uint8Array.from([5, 0, 6, 0]).buffer),
            view: new DataView(bytes.buffer, 1, 2),
          };
        `,
        context(),
        5_000,
      );

      expect(Array.from(new Uint8Array(result.arrayBuffer))).toEqual([1, 2, 3, 4]);
      expect(result.typed).toBeInstanceOf(Uint16Array);
      expect(Array.from(result.typed)).toEqual([5, 6]);
      expect(result.view).toBeInstanceOf(DataView);
      expect(Array.from(new Uint8Array(
        result.view.buffer,
        result.view.byteOffset,
        result.view.byteLength,
      ))).toEqual([2, 3]);
    } finally {
      executor.onDestroy();
    }
  });

  it('preserves null-prototype and reserved-envelope result objects', async () => {
    const executor = createExecutor();

    try {
      const result = await executor.run(
        `
          const dictionary = Object.create(null);
          dictionary.marker = 7;
          return {
            dictionary,
            reserved: { __typedArray: 'business', bytes: [9] },
            map: new Map([[1, 'number'], ['1', 'string']]),
            big: 9007199254740993n,
            nestedUndefined: { value: undefined },
          };
        `,
        context(),
        5_000,
      );

      expect(Object.getPrototypeOf(result.dictionary)).toBeNull();
      expect(result.dictionary.marker).toBe(7);
      expect(result.reserved).toEqual({
        __typedArray: 'business',
        bytes: [9],
      });
      expect(result.map).toBeInstanceOf(Map);
      expect(Array.from(result.map.entries())).toEqual([
        [1, 'number'],
        ['1', 'string'],
      ]);
      expect(result.big).toBe(9007199254740993n);
      expect(Object.prototype.hasOwnProperty.call(
        result.nestedUndefined,
        'value',
      )).toBe(true);
      expect(result.nestedUndefined.value).toBeUndefined();
    } finally {
      executor.onDestroy();
    }
  });

  it('preserves binary and null-prototype host capability arguments', async () => {
    const executor = createExecutor();
    let captured: any;

    try {
      const result = await executor.run(
        `
          const dictionary = Object.create(null);
          dictionary.marker = 9;
          return await $ctx.$helpers.capture(
            new DataView(Uint8Array.from([1, 2, 3]).buffer, 1, 2),
            dictionary,
          );
        `,
        {
          ...context(),
          $helpers: {
            capture: (view: DataView, dictionary: Record<string, unknown>) => {
              captured = { view, dictionary };
              return true;
            },
          },
        },
        5_000,
      );

      expect(result).toBe(true);
      expect(captured.view).toBeInstanceOf(DataView);
      expect(Array.from(new Uint8Array(
        captured.view.buffer,
        captured.view.byteOffset,
        captured.view.byteLength,
      ))).toEqual([2, 3]);
      expect(Object.getPrototypeOf(captured.dictionary)).toBeNull();
      expect(captured.dictionary.marker).toBe(9);
    } finally {
      executor.onDestroy();
    }
  });

  it('decodes nested callbacks on callback-enabled host boundaries', async () => {
    const executor = createExecutor();

    try {
      const result = await executor.run(
        `
          return await $ctx.$storage.run({
            nested: {
              callback: (value) => value + 1,
            },
          });
        `,
        {
          ...context(),
          $storage: {
            run: async (value: any) => value.nested.callback(41),
          },
        },
        5_000,
      );

      expect(result).toBe(42);
    } finally {
      executor.onDestroy();
    }
  });

  it('rejects forged typed-array constructor names at the host boundary', () => {
    const executor = createExecutor() as any;

    try {
      expect(() => executor.hostIo.parseExecutorArgs(JSON.stringify([
        {
          __typedArray: 'Function',
          bytes: [],
        },
      ]))).toThrow('Unsupported executor typed array: Function');
    } finally {
      executor.onDestroy();
    }
  });

  it('exposes literal then, catch, and finally result properties', async () => {
    const executor = createExecutor();

    try {
      const result = await executor.run(
        `
          const value = $ctx.$helpers.namedProperties();
          return {
            then: await value.$property('then'),
            catch: await value.$property('catch'),
            finally: await value.$property('finally'),
            caller: await value.caller,
            arguments: await value.arguments,
            prototype: await value.prototype,
          };
        `,
        {
          ...context(),
          $helpers: {
            namedProperties: () => ({
              then: 'then-value',
              catch: 'catch-value',
              finally: 'finally-value',
              caller: 'caller-value',
              arguments: 'arguments-value',
              prototype: 'prototype-value',
            }),
          },
        },
        5_000,
      );

      expect(result).toEqual({
        then: 'then-value',
        catch: 'catch-value',
        finally: 'finally-value',
        caller: 'caller-value',
        arguments: 'arguments-value',
        prototype: 'prototype-value',
      });
    } finally {
      executor.onDestroy();
    }
  });

  it('merges explicit undefined for core context fields', async () => {
    const executor = createExecutor();
    const ctx = {
      ...context(),
      $data: 'stale-data',
      $error: 'stale-error',
      $statusCode: 503,
    } as any;

    try {
      await executor.run(
        `
          $ctx.$data = undefined;
          $ctx.$error = undefined;
          $ctx.$statusCode = undefined;
          return true;
        `,
        ctx,
        5_000,
      );

      expect(Object.prototype.hasOwnProperty.call(ctx, '$data')).toBe(true);
      expect(Object.prototype.hasOwnProperty.call(ctx, '$error')).toBe(true);
      expect(Object.prototype.hasOwnProperty.call(ctx, '$statusCode')).toBe(true);
      expect(ctx.$data).toBeUndefined();
      expect(ctx.$error).toBeUndefined();
      expect(ctx.$statusCode).toBeUndefined();
    } finally {
      executor.onDestroy();
    }
  });

  it('decodes binary and null-prototype context changes before merging', async () => {
    const executor = createExecutor();
    const ctx = context() as any;

    try {
      await executor.run(
        `
          const dictionary = Object.create(null);
          dictionary.safe = true;
          $ctx.$data = {
            bytes: Uint8Array.from([8, 7]),
            dictionary,
          };
          return true;
        `,
        ctx,
        5_000,
      );

      expect(ctx.$data.bytes).toBeInstanceOf(Uint8Array);
      expect(Array.from(ctx.$data.bytes)).toEqual([8, 7]);
      expect(Object.getPrototypeOf(ctx.$data.dictionary)).toBeNull();
      expect(ctx.$data.dictionary.safe).toBe(true);
    } finally {
      executor.onDestroy();
    }
  });
});
