import { IsolatedExecutorService } from '@enfyra/kernel';
import { describe, expect, it } from 'vitest';

function prototypeChain(length: number): object {
  let current: object | null = null;
  for (let index = 0; index < length; index += 1) {
    const parent = current;
    current = new Proxy({}, { getPrototypeOf: () => parent });
  }
  return current!;
}

describe('host capability property lookup', () => {
  it('bounds hostile prototype traversal', async () => {
    const executor = new IsolatedExecutorService({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    });

    try {
      await expect(
        executor.run(
          'return await $ctx.$helpers.missing();',
          {
            $body: {},
            $query: {},
            $params: {},
            $share: {},
            $api: { request: {} },
            $helpers: prototypeChain(65),
          },
          5_000,
        ),
      ).rejects.toThrow(/prototype traversal limit/i);
    } finally {
      executor.onDestroy();
    }
  });
});
