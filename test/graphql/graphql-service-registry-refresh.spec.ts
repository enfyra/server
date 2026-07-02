import { describe, expect, it, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { GraphqlService } from '../../src/modules/graphql/services/graphql.service';

function metadata() {
  const postTable = {
    id: 7,
    name: 'posts',
    columns: [
      { name: 'id', type: 'int', isPrimary: true, isNullable: false },
      { name: 'title', type: 'varchar', isNullable: true },
    ],
    relations: [],
  };
  return {
    tables: new Map([['posts', postTable]]),
    tablesList: [postTable],
    version: 1,
    timestamp: new Date(),
  };
}

describe('GraphqlService registry refresh', () => {
  it('builds schema from freshly reloaded definition and metadata cache data', async () => {
    const metadataCacheService = {
      getMetadata: vi.fn(async () => metadata()),
    };
    const gqlDefinitionCacheService = {
      reload: vi.fn(async () => undefined),
      syncFromSharedCache: vi.fn(async () => undefined),
      getAllEnabled: vi.fn(async () => {
        throw new Error('stale registry-backed definition read');
      }),
      getAllEnabledFromCache: vi.fn(async () => [
        {
          id: 1,
          isEnabled: true,
          isSystem: false,
          description: null,
          metadata: null,
          tableName: 'posts',
        },
      ]),
    };
    const service = new GraphqlService({
      metadataCacheService: metadataCacheService as any,
      settingCacheService: {
        getMaxQueryDepth: vi.fn(async () => 10),
      } as any,
      gqlDefinitionCacheService: gqlDefinitionCacheService as any,
      dynamicResolver: {
        dynamicResolver: vi.fn(),
        dynamicMutationResolver: vi.fn(),
      } as any,
      eventEmitter: new EventEmitter2(),
      envService: { isProd: false } as any,
    });

    await service.reloadSchema();

    expect(gqlDefinitionCacheService.reload).toHaveBeenCalled();
    expect(gqlDefinitionCacheService.getAllEnabledFromCache).toHaveBeenCalled();
    expect(gqlDefinitionCacheService.getAllEnabled).not.toHaveBeenCalled();
    expect(metadataCacheService.getMetadata).toHaveBeenCalled();
    expect(service.getSchemaSdl()).toContain('type posts');
  });
});
