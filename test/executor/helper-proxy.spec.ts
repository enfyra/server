import { describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';
import { autoSlug } from '../../src/shared/utils/auto-slug.helper';

function createService() {
  return new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => [],
    } as any,
    packageCdnLoaderService: {
      getPackageSources: () => [],
    } as any,
  });
}

function createContext() {
  const ctx: any = {
    $body: {},
    $query: {},
    $params: {},
    $share: { $logs: [] },
    $helpers: { autoSlug },
    $cache: {},
    $repos: {},
    $user: null,
  };
  ctx.$logs = (...args: any[]) => ctx.$share.$logs.push(...args);
  return ctx;
}

describe('isolated executor helper proxy', () => {
  it('auto-awaits direct helper return values', async () => {
    const service = createService();
    try {
      const result = await service.run(
        `return $ctx.$helpers.autoSlug('Xin chào thế giới');`,
        createContext(),
        5000,
      );

      expect(result).toBe('xin-chao-the-gioi');
    } finally {
      service.onDestroy();
    }
  });

  it('auto-awaits helper results before calling chained string methods', async () => {
    const service = createService();
    try {
      const result = await service.run(
        `return $ctx.$helpers.autoSlug('Hello World').toUpperCase().replaceAll('-', '_');`,
        createContext(),
        5000,
      );

      expect(result).toBe('HELLO_WORLD');
    } finally {
      service.onDestroy();
    }
  });
});
