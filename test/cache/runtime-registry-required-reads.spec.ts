import { describe, expect, it, vi } from 'vitest';
import { RuntimeRegistryService } from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

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
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.FLOW, {
      getCacheAsync: vi.fn(async () => [
        {
          id: 7,
          name: 'scheduled-flow',
          triggerType: 'schedule',
          steps: [],
        },
      ]),
    });

    expect(registry.getFlowByName('scheduled-flow')).toEqual(
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
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.WEBSOCKET, {
      getCacheAsync: vi.fn(async () => [
        {
          id: 3,
          path: '/chat',
          isEnabled: true,
          events: [],
        },
      ]),
    });

    expect(registry.getWebsocketGatewayByPath('/chat')).toEqual(
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
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.GUARD, {
      getCacheAsync: vi.fn(async () => ({
        preAuthGlobal: [guard],
        postAuthGlobal: [],
        preAuthByRoute: new Map(),
        postAuthByRoute: new Map(),
      })),
    });

    expect(registry.getGuardsForRoute('pre_auth', '/posts', 'GET')).toEqual([
      guard,
    ]);
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
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.FIELD_PERMISSION, {
      getCacheAsync: vi.fn(
        async () => new Map([['r:editor|posts|read', policy]]),
      ),
    });

    expect(
      registry.getFieldPermissionPoliciesFor(
        { role: { id: 'editor' } },
        'posts',
        'read',
      ),
    ).toEqual([policy]);
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
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.COLUMN_RULE, {
      getCacheAsync: vi.fn(async () => new Map([['title-column', [rule]]])),
    });

    expect(registry.getColumnRulesForColumn('title-column')).toEqual([rule]);
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
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.STORAGE, {
      getCacheAsync: vi.fn(
        async () =>
          new Map<any, any>([
            [9, { id: 9, type: 'local', isEnabled: true }],
            ['s3-main', { id: 's3-main', type: 's3', isEnabled: true }],
          ]),
      ),
    });

    expect(registry.getStorageConfigById(9)).toEqual(
      expect.objectContaining({ type: 'local' }),
    );
    expect(registry.getStorageConfigByType('s3')).toEqual(
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
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.OAUTH_CONFIG, {
      getCacheAsync: vi.fn(
        async () =>
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
      ),
    });

    expect(registry.getOauthConfigByProvider('google')).toEqual(
      expect.objectContaining({ clientId: 'client' }),
    );
    expect(registry.getOauthProviders()).toEqual(['google']);
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
