import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';
import { EventEmitter2 } from 'eventemitter2';
import { SharedHelperCacheService } from '../../src/engines/cache/services/shared-helper-cache.service';
import {
  normalizeScriptRecord,
  normalizeScriptPatch,
} from '../../src/shared/utils/script-code.util';
import {
  RuntimeRegistryService,
  CacheOrchestratorService,
} from '../../src/engines/cache';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap';
import { bootstrapSourceArtifacts } from '../../src/data';
import { PolicyService } from '../../src/domain/policy';
import {
  phaseCoreCaches,
  phaseParallelCacheReload,
  phasePublishActivatedSnapshots,
} from '../../src/wiring/init/phases';

beforeEach(() => DatabaseConfigService.overrideForTesting('postgres'));
afterEach(() => DatabaseConfigService.resetForTesting());

function cache(data: unknown[]) {
  return new SharedHelperCacheService({
    queryBuilderService: { find: vi.fn(async () => ({ data })) },
    eventEmitter: new EventEmitter2(),
  } as any);
}

describe('metadata shared helpers', () => {
  it('compiles source without trusting saved compiled code', async () => {
    const c = cache([
      {
        id: 1,
        name: 'oneendpoint_search',
        isEnabled: true,
        scriptLanguage: 'typescript',
        sourceCode:
          'const value: string = @PARAMS.query; return @USER.id + value;',
        compiledCode: 'return "stale";',
      },
    ]);
    await c.reload(false);
    const result = await c.getCacheAsync();
    expect(result[0].code).toContain('$ctx.$user.id');
    expect(result[0].code).not.toContain(': string');
    expect(result[0].code).not.toContain('stale');
  });

  it('loads all helper pages with stable primary-key ordering', async () => {
    const rows = Array.from({ length: 1001 }, (_, id) => ({
      id,
      name: `helper_${id}`,
      isEnabled: false,
    }));
    const find = vi.fn(async ({ page, limit }) => ({
      data: rows.slice((page - 1) * limit, page * limit),
    }));
    const c = new SharedHelperCacheService({
      queryBuilderService: { find },
      eventEmitter: new EventEmitter2(),
    } as any);
    await c.reload();
    expect((await c.getCacheAsync()).length).toBe(1001);
    expect(find).toHaveBeenCalledTimes(2);
    expect(find.mock.calls[0][0]).toMatchObject({
      page: 1,
      limit: 1000,
      sort: ['id'],
    });
  });

  it('projects the Mongo primary identity without assuming SQL id', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const c = cache([
      {
        _id: 'mongo-id',
        name: 'echo',
        isEnabled: true,
        sourceCode: 'return @PARAMS;',
      },
    ]);
    await c.reload();
    expect((await c.getCacheAsync())[0].id).toBe('mongo-id');
  });

  it('retains disabled definitions without executing their source', async () => {
    const c = cache([
      {
        id: 1,
        name: 'disabled',
        isEnabled: false,
        sourceCode: 'not valid code !!!',
      },
    ]);
    await c.reload(false);
    expect((await c.getCacheAsync())[0]).toMatchObject({
      name: 'disabled',
      isEnabled: false,
      code: '',
    });
  });

  it.each(['BadName', '__proto__', 'has spaces', '', 'run'])(
    'rejects invalid or reserved helper name %s',
    (name) => {
      expect(() =>
        normalizeScriptRecord('enfyra_helper', {
          name,
          sourceCode: 'return @PARAMS;',
        }),
      ).toThrow();
      expect(() => normalizeScriptPatch('enfyra_helper', { name })).toThrow();
    },
  );

  it('rejects duplicate names in runtime metadata', async () => {
    const c = cache([
      { id: 1, name: 'same', isEnabled: false },
      { id: 2, name: 'same', isEnabled: false },
    ]);
    await expect(c.reload(false)).rejects.toThrow();
  });

  it('declares unique helper names for both database projections and privileged CRUD', () => {
    for (const databaseType of ['postgres', 'mongodb'] as const) {
      const definition = new BootstrapDefinitionService().resolveForVersion(
        '2.3.4',
        databaseType,
      );
      expect(definition.snapshot.enfyra_helper.uniques).toEqual([['name']]);
      expect(definition.snapshot.enfyra_helper.columns).toContainEqual(
        expect.objectContaining({
          name: 'compiledCode',
          isGenerated: true,
          isUpdatable: false,
        }),
      );
    }
    const route = bootstrapSourceArtifacts.defaultData.enfyra_route.find(
      (row: any) => row.path === '/enfyra_helper',
    );
    expect(route).toMatchObject({
      mainTable: 'enfyra_helper',
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    });
    expect(route?.publicMethods).toBeUndefined();
    expect(route?.skipRoleGuardMethods).toBeUndefined();
  });

  it('denies helper code CRUD to anonymous and ordinary users without an explicit grant', () => {
    const policy = new PolicyService({} as any);
    for (const method of ['GET', 'POST', 'PATCH', 'DELETE']) {
      const routeData = {
        path: '/enfyra_helper',
        routeMethodConfig: { isPublic: false, skipRoleGuard: false },
        routePermissions: [],
      };
      expect(policy.checkRequestAccess({ method, routeData })).toMatchObject({
        allow: false,
        statusCode: 401,
      });
      expect(
        policy.checkRequestAccess({
          method,
          routeData,
          user: { id: 1, roles: [{ id: 'member' }] },
        }),
      ).toMatchObject({ allow: false, statusCode: 403 });
      expect(
        policy.checkRequestAccess({
          method,
          routeData,
          user: { id: 1, isRootAdmin: true },
        }).allow,
      ).toBe(true);
    }
  });

  it('keeps activated helper snapshots coherent until a successful publish', async () => {
    const registry = new RuntimeRegistryService();
    expect(() => registry.getSharedHelpers()).toThrow('not activated');
    const data = [
      { name: 'echo', isEnabled: true, sourceCode: 'return "old";' },
    ];
    const c = cache(data);
    await c.reload();
    await registry.publishFromCache(CACHE_IDENTIFIERS.HELPER, c);
    data[0].sourceCode = 'return "new";';
    await c.reload();
    expect(registry.getSharedHelpers()[0].code).toContain('old');
    await registry.publishFromCache(CACHE_IDENTIFIERS.HELPER, c);
    expect(registry.getSharedHelpers()[0].code).toContain('new');
    data[0].sourceCode = 'const = ;';
    await expect(c.reload()).rejects.toThrow();
    expect(registry.getSharedHelpers()[0].code).toContain('new');
  });

  it('reloads and activates helper definitions through the existing orchestrator', async () => {
    const c = cache([
      { name: 'echo', isEnabled: true, sourceCode: 'return @PARAMS;' },
    ]);
    const registry = new RuntimeRegistryService();
    const orchestrator = new CacheOrchestratorService({
      sharedHelperCacheService: c,
      runtimeRegistryService: registry,
      redisPubSubService: { publish: vi.fn() },
      instanceService: { getInstanceId: () => 'fixture' },
      eventEmitter: new EventEmitter2(),
      dynamicWebSocketGateway: { emitToNamespace: vi.fn() },
    } as any);
    await (orchestrator as any).executeChain({
      table: 'enfyra_helper',
      action: 'update',
      scope: 'full',
    });
    expect(registry.getSharedHelpers()[0].code).toContain(
      'return $ctx.$params',
    );
  });

  it('rejects enabled empty helpers at authoring, including source clearing and re-enable', () => {
    expect(() =>
      normalizeScriptRecord('enfyra_helper', { name: 'empty' }),
    ).toThrow('executable sourceCode');
    expect(() =>
      normalizeScriptPatch(
        'enfyra_helper',
        { sourceCode: null },
        { isEnabled: true, sourceCode: 'return 1;' },
      ),
    ).toThrow('executable sourceCode');
    expect(() =>
      normalizeScriptPatch(
        'enfyra_helper',
        { isEnabled: true },
        { isEnabled: false, sourceCode: null },
      ),
    ).toThrow('executable sourceCode');
    expect(() =>
      normalizeScriptRecord('enfyra_helper', {
        name: 'draft',
        isEnabled: false,
      }),
    ).not.toThrow();
  });

  it('loads helpers before dependent script caches and activates them during boot', async () => {
    const c = cache([
      { name: 'echo', isEnabled: true, sourceCode: 'return @PARAMS;' },
    ]);
    const registry = new RuntimeRegistryService();
    const cradle: any = {
      sharedHelperCacheService: c,
      runtimeRegistryService: registry,
      sqlFunctionService: { installExtensions: vi.fn() },
      cacheOrchestratorService: { init: vi.fn() },
      repoRegistryService: { rebuildFromMetadata: vi.fn() },
      eventEmitter: new EventEmitter2(),
    };
    for (const name of [
      'metadataCacheService',
      'routeCacheService',
      'fieldPermissionCacheBuilder',
      'columnRuleCacheBuilder',
      'settingCacheService',
      'authHeaderCacheBuilder',
      'storageConfigCacheBuilder',
      'oauthConfigCacheBuilder',
      'websocketCacheBuilder',
      'flowCacheBuilder',
      'packageCacheService',
      'folderTreeCacheService',
      'guardCacheBuilder',
      'gqlDefinitionCacheService',
    ]) {
      cradle[name] = {
        reload: vi.fn(async () => {
          if (name !== 'metadataCacheService')
            expect(registry.getSharedHelpers()[0].name).toBe('echo');
        }),
        getCacheAsync: async () => [],
      };
    }
    await phaseCoreCaches(cradle);
    expect(registry.getSharedHelpers()[0].name).toBe('echo');
    await phaseParallelCacheReload(cradle);
    await phasePublishActivatedSnapshots(cradle);
    expect(registry.getSharedHelpers()[0].name).toBe('echo');
  });

  it('hydrates a shared helper snapshot without querying the receiving database', async () => {
    const find = vi.fn(() => {
      throw new Error('receiver must not read database');
    });
    const getSnapshot = vi.fn(async () => ({
      data: [{ name: 'echo', isEnabled: true, code: 'return @PARAMS;' }],
    }));
    const c = new SharedHelperCacheService({
      queryBuilderService: { find },
      eventEmitter: new EventEmitter2(),
      redisRuntimeCacheStore: {
        isEnabled: () => true,
        getSnapshot,
        waitForSnapshot: getSnapshot,
      },
    } as any);
    await c.syncFromSharedCache();
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.HELPER, c);
    expect(registry.getSharedHelpers()[0].name).toBe('echo');
    expect(find).not.toHaveBeenCalled();
  });

  it('supports the exact selected tool name', () => {
    const record = normalizeScriptRecord('enfyra_helper', {
      name: '1endpoint_web_search',
      sourceCode: 'return @PARAMS.query;',
      scriptLanguage: 'typescript',
    });
    expect(record.compiledCode).toContain('$ctx.$params.query');
  });
});
