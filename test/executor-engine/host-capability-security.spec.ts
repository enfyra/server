import { IsolatedExecutorService } from '@enfyra/kernel';

function createExecutor() {
  return new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] },
    packageCdnLoaderService: { getPackageSources: () => [] },
  });
}

function context(overrides: Record<string, unknown> = {}) {
  return {
    $body: {},
    $query: {},
    $params: {},
    $share: {},
    $api: { request: {} },
    ...overrides,
  };
}

describe('executor host capability security', () => {
  it('rejects inherited constructor traversal through helpers', async () => {
    const executor = createExecutor();
    delete (globalThis as Record<string, unknown>).__enfyraHostEscaped;

    try {
      await expect(
        executor.run(
          `
            return await $ctx.$helpers.constructor.constructor(
              'globalThis.__enfyraHostEscaped = true',
            )();
          `,
          context({ $helpers: {} }),
          5_000,
        ),
      ).rejects.toThrow('Host capability property is forbidden: constructor');
      expect(
        (globalThis as Record<string, unknown>).__enfyraHostEscaped,
      ).toBeUndefined();
    } finally {
      delete (globalThis as Record<string, unknown>).__enfyraHostEscaped;
      executor.onDestroy();
    }
  });

  it('does not treat host capability namespace proxies as thenables', async () => {
    const executor = createExecutor();

    try {
      const result = await executor.run(
        `
          return {
            repos: await $ctx.$repos === $ctx.$repos,
            helpers: await $ctx.$helpers === $ctx.$helpers,
            socket: await $ctx.$socket === $ctx.$socket,
            cache: await $ctx.$cache === $ctx.$cache,
            storage: await $ctx.$storage === $ctx.$storage,
            res: await $ctx.$res === $ctx.$res,
            throwing: await $ctx.$throw === $ctx.$throw,
          };
        `,
        context({
          $repos: {},
          $helpers: {},
          $socket: {},
          $cache: {},
          $storage: {},
          $res: {},
        }),
        5_000,
      );

      expect(result).toEqual({
        repos: true,
        helpers: true,
        socket: true,
        cache: true,
        storage: true,
        res: true,
        throwing: true,
      });
    } finally {
      executor.onDestroy();
    }
  });

  it('rejects inherited names as unavailable modules', async () => {
    const executor = createExecutor();

    try {
      await expect(
        executor.run(
          `return require('constructor');`,
          context(),
          5_000,
        ),
      ).rejects.toThrow('Module "constructor" is not available');
    } finally {
      executor.onDestroy();
    }
  });

  it('does not apply unsafe flow prototype keys to the host object', async () => {
    const executor = createExecutor();
    const flow = { state: 'initial' };

    try {
      const result = await executor.runBatch(
        [
          {
            type: 'handler',
            code: `
              Object.defineProperty($ctx.$flow, '__proto__', {
                value: { privileged: true },
                enumerable: true,
              });
              $ctx.$flow.state = 'updated';
              return true;
            `,
          },
        ],
        context({ $flow: flow }),
        5_000,
      );

      expect(result.value).toBe(true);
      expect(flow.state).toBe('updated');
      expect(Object.getPrototypeOf(flow)).toBe(Object.prototype);
      expect((flow as Record<string, unknown>).privileged).toBeUndefined();
      expect(Object.prototype.hasOwnProperty.call(flow, '__proto__')).toBe(false);
    } finally {
      executor.onDestroy();
    }
  });
});
