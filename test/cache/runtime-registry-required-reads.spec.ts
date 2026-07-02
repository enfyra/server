import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import {
  ColumnRuleCacheService,
  FlowCacheService,
  FieldPermissionCacheService,
  GuardCacheService,
  OAuthConfigCacheService,
  RuntimeRegistryService,
  StorageConfigCacheService,
  WebsocketCacheService,
} from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

function registrySnapshot(identifier: string, data: unknown) {
  return {
    getSnapshot: vi.fn((requested: string) =>
      requested === identifier
        ? {
            identifier,
            version: 1,
            activatedAt: '2026-07-02T00:00:00.000Z',
            data,
          }
        : undefined,
    ),
    getActiveData: vi.fn((requested: string) =>
      requested === identifier ? data : undefined,
    ),
    requireActiveData: vi.fn((requested: string) => {
      if (requested === identifier) return data;
      throw new Error(`Runtime cache ${requested} is not activated`);
    }),
  };
}

describe('runtime registry required reads', () => {
  it('serves package names from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.PACKAGE, {
      getCacheAsync: vi.fn(async () => ['lodash']),
    });

    expect(registry.getPackages()).toEqual(['lodash']);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('fails clearly instead of falling back when the package registry is not activated', () => {
    const queryBuilderService = {
      find: vi.fn(async () => ['stale-db-package']),
    };
    const registry = new RuntimeRegistryService();

    expect(() => registry.getPackages()).toThrow(
      'Runtime cache package is not activated',
    );
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves flow lookups from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const service = new FlowCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(CACHE_IDENTIFIERS.FLOW, [
        {
          id: 7,
          name: 'scheduled-flow',
          triggerType: 'schedule',
          steps: [],
        },
      ]) as any,
    });

    await expect(service.getFlowByName('scheduled-flow')).resolves.toEqual(
      expect.objectContaining({ id: 7 }),
    );
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves websocket gateway lookups from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const service = new WebsocketCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(CACHE_IDENTIFIERS.WEBSOCKET, [
        {
          id: 3,
          path: '/chat',
          isEnabled: true,
          events: [],
        },
      ]) as any,
    });

    await expect(service.getGatewayByPath('/chat')).resolves.toEqual(
      expect.objectContaining({ id: 3 }),
    );
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves guard lookups from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const guard = {
      id: 1,
      name: 'global-read',
      position: 'pre_auth',
      combinator: 'and',
      priority: 1,
      isEnabled: true,
      isGlobal: true,
      parentId: null,
      routeId: null,
      routePath: null,
      methodIds: [],
      methods: ['GET'],
      children: [],
      rules: [],
    };
    const service = new GuardCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(CACHE_IDENTIFIERS.GUARD, {
        preAuthGlobal: [guard],
        postAuthGlobal: [],
        preAuthByRoute: new Map(),
        postAuthByRoute: new Map(),
      }) as any,
    });

    await expect(
      service.getGuardsForRoute('pre_auth', '/posts', 'GET'),
    ).resolves.toEqual([guard]);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves field permission lookups from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const policy = {
      unconditionalAllowedColumns: new Set(['title']),
      unconditionalAllowedRelations: new Set<string>(),
      unconditionalDeniedColumns: new Set<string>(),
      unconditionalDeniedRelations: new Set<string>(),
      rules: [],
    };
    const service = new FieldPermissionCacheService({
      queryBuilderService: queryBuilderService as any,
      metadataCacheService: {} as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(
        CACHE_IDENTIFIERS.FIELD_PERMISSION,
        new Map([['r:editor|posts|read', policy]]),
      ) as any,
    });

    await expect(
      service.getPoliciesFor({ role: { id: 'editor' } }, 'posts', 'read'),
    ).resolves.toEqual([policy]);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves column rule lookups from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const rule = {
      id: 11,
      ruleType: 'minLength',
      value: 3,
      message: null,
      isEnabled: true,
      columnId: 'title-column',
    };
    const service = new ColumnRuleCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(
        CACHE_IDENTIFIERS.COLUMN_RULE,
        new Map([['title-column', [rule]]]),
      ) as any,
    });

    await expect(service.getRulesForColumn('title-column')).resolves.toEqual([
      rule,
    ]);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves settings from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.SETTING, {
      getCacheAsync: vi.fn(async () => ({
        maxQueryDepth: 17,
        maxUploadFileSize: 4,
        maxRequestBodySize: 2,
        customFlag: 'on',
      })),
    });

    expect(registry.getMaxQueryDepth()).toBe(17);
    expect(registry.getSetting('customFlag')).toBe('on');
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves storage configs from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
      isMongoDb: vi.fn(() => false),
    };
    const service = new StorageConfigCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(
        CACHE_IDENTIFIERS.STORAGE,
        new Map<any, any>([
          [9, { id: 9, type: 'local', isEnabled: true }],
          ['s3-main', { id: 's3-main', type: 's3', isEnabled: true }],
        ]),
      ) as any,
    });

    await expect(service.getStorageConfigById(9)).resolves.toEqual(
      expect.objectContaining({ type: 'local' }),
    );
    await expect(service.getStorageConfigByType('s3')).resolves.toEqual(
      expect.objectContaining({ id: 's3-main' }),
    );
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves OAuth configs from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const service = new OAuthConfigCacheService({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
      runtimeRegistryService: registrySnapshot(
        CACHE_IDENTIFIERS.OAUTH_CONFIG,
        new Map([
          [
            'google',
            {
              id: 1,
              provider: 'google',
              clientId: 'client',
              clientSecret: 'secret',
              redirectUri: 'https://example.com/callback',
              autoSetCookies: true,
              isEnabled: true,
            },
          ],
        ]),
      ) as any,
    });

    await expect(service.getDirectConfigByProvider('google')).resolves.toEqual(
      expect.objectContaining({ clientId: 'client' }),
    );
    await expect(service.getAllProviders()).resolves.toEqual(['google']);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves GraphQL definitions from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.GRAPHQL, {
      getCacheAsync: vi.fn(
        async () =>
          new Map([
            [
              'posts',
              {
                id: 1,
                isEnabled: true,
                isSystem: false,
                description: null,
                metadata: null,
                tableName: 'posts',
              },
            ],
          ]),
      ),
    });

    expect(registry.isGraphqlEnabledForTable('posts')).toBe(true);
    expect(registry.getAllEnabledGraphqlDefinitions()).toEqual([
      expect.objectContaining({ tableName: 'posts' }),
    ]);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });

  it('serves folder tree helpers from the active registry snapshot', async () => {
    const queryBuilderService = {
      find: vi.fn(async () => {
        throw new Error('DB should not be read');
      }),
    };
    const root = {
      id: 'root',
      parentId: null,
      name: 'Root',
      slug: 'root',
      order: 1,
      icon: 'folder',
      description: null,
      children: [],
    };
    const child = {
      id: 'child',
      parentId: 'root',
      name: 'Child',
      slug: 'child',
      order: 1,
      icon: 'folder',
      description: null,
      children: [],
    };
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.FOLDER_TREE, {
      getCacheAsync: vi.fn(async () => ({
        folders: new Map([
          ['root', root],
          ['child', child],
        ]),
        tree: [root],
      })),
    });

    expect(registry.getFolderTree()).toEqual([root]);
    expect(registry.isCircularFolderParent('root', 'child')).toBe(true);
    expect(queryBuilderService.find).not.toHaveBeenCalled();
  });
});
