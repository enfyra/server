import { describe, expect, it, vi } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

function createService() {
  return new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] } as any,
    packageCdnLoaderService: { getPackageSources: () => [] } as any,
  });
}

describe('isolated executor secure repository proxy', () => {
  it('routes nested secure repository calls to the field-permission-enforced registry', async () => {
    const service = createService();
    const secureFind = vi.fn(async (options: any) => ({
      data: [{ id: 1, name: 'secure' }],
      options,
    }));
    const trustedFind = vi.fn(async () => ({ data: [{ id: 2, name: 'trusted' }] }));
    const context = {
      $body: {},
      $query: {},
      $params: {},
      $share: { $logs: [] },
      $helpers: {},
      $cache: {},
      $user: { id: 1 },
      $repos: {
        secure: {
          projects: { find: secureFind },
        },
        projects: { find: trustedFind },
      },
    };

    try {
      const result = await service.run(
        `return await $ctx.$repos.secure.projects.find({ fields: ['id', 'name'], limit: 1 });`,
        context,
        5000,
      );

      expect(result).toEqual({
        data: [{ id: 1, name: 'secure' }],
        options: { fields: ['id', 'name'], limit: 1 },
      });
      expect(secureFind).toHaveBeenCalledOnce();
      expect(trustedFind).not.toHaveBeenCalled();
    } finally {
      service.onDestroy();
    }
  });
});
