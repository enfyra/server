import { describe, expect, it, vi } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';
import { EventEmitter2 } from 'eventemitter2';
import { RepoRegistryService } from '../../src/engines/cache/services/repo-registry.service';

function createService() {
  return new IsolatedExecutorService({
    packageCacheService: { getPackages: async () => [] } as any,
    packageCdnLoaderService: { getPackageSources: () => [] } as any,
  });
}

describe('isolated executor secure repository proxy', () => {
  it('resolves main, aliases and secure access through the actual ESV lazy registry', async () => {
    const service = createService();
    const factory = { create: vi.fn((table, _context, enforce) => ({ find: async () => ({ table, enforce }) })) };
    const registry = new RepoRegistryService({
      metadataCacheService: { getAllTablesMetadata: async () => [{ name: 'projects', alias: 'work' }] } as any,
      dynamicRepositoryFactory: factory as any,
      eventEmitter: new EventEmitter2(),
    });
    await registry.rebuildFromMetadata();
    const context: any = { $share: {}, $helpers: {} };
    context.$repos = registry.createReposProxy(context, 'projects');
    try {
      expect(await service.run(`return [await $ctx.$repos.main.find(), await $ctx.$repos.work.find(), await $ctx.$repos.secure.work.find()];`, context, 5000)).toEqual([
        { table: 'projects', enforce: true },
        { table: 'projects', enforce: false },
        { table: 'projects', enforce: true },
      ]);
      await expect(service.run(`return await $ctx.$repos.constructor.find();`, context, 5000)).rejects.toThrow(/forbidden/);
      await expect(service.run(`return await $ctx.$repos.toString.find();`, context, 5000)).rejects.toThrow(/not found/);
    } finally {
      await service.onDestroy();
    }
  });
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

  it('exposes the many-mutation repository methods to dynamic scripts', async () => {
    const service = createService();
    const updateMany = vi.fn(async (options: any) => ({
      data: [{ id: 1, status: 'done' }],
      count: 1,
      options,
    }));
    const context = {
      $body: {},
      $query: {},
      $params: {},
      $share: { $logs: [] },
      $helpers: {},
      $cache: {},
      $user: { id: 1 },
      $repos: {
        projects: { updateMany },
      },
    };

    try {
      const result = await service.run(
        `return await $ctx.$repos.projects.updateMany({ ids: [1], data: { status: 'done' } });`,
        context,
        5000,
      );

      expect(result).toMatchObject({
        data: [{ id: 1, status: 'done' }],
        count: 1,
      });
      expect(updateMany).toHaveBeenCalledWith({
        ids: [1],
        data: { status: 'done' },
      });
    } finally {
      service.onDestroy();
    }
  });
});
