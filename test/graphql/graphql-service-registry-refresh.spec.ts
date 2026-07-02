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
  it('builds schema from activated registry definition and metadata data', async () => {
    const runtimeRegistryService = {
      requireMetadata: vi.fn(() => metadata()),
      getAllEnabledGraphqlDefinitions: vi.fn(() => [
        {
          id: 1,
          isEnabled: true,
          isSystem: false,
          description: null,
          metadata: null,
          tableName: 'posts',
        },
      ]),
      getMaxQueryDepth: vi.fn(() => 10),
    };
    const service = new GraphqlService({
      runtimeRegistryService: runtimeRegistryService as any,
      dynamicResolver: {
        dynamicResolver: vi.fn(),
        dynamicMutationResolver: vi.fn(),
      } as any,
      eventEmitter: new EventEmitter2(),
      envService: { isProd: false } as any,
    });

    await service.reloadSchema();

    expect(runtimeRegistryService.requireMetadata).toHaveBeenCalled();
    expect(
      runtimeRegistryService.getAllEnabledGraphqlDefinitions,
    ).toHaveBeenCalled();
    expect(runtimeRegistryService.getMaxQueryDepth).toHaveBeenCalled();
    expect(service.getSchemaSdl()).toContain('type posts');
    expect(service.getStatus()).toEqual(
      expect.objectContaining({
        schemaReady: true,
        lastReload: expect.objectContaining({ status: 'ok' }),
      }),
    );
  });

  it('marks GraphQL runtime degraded when reload fails after a registry commit', async () => {
    const runtimeRegistryService = {
      requireMetadata: vi.fn(() => {
        throw new Error('metadata missing');
      }),
      getAllEnabledGraphqlDefinitions: vi.fn(() => []),
      getMaxQueryDepth: vi.fn(() => 10),
    };
    const service = new GraphqlService({
      runtimeRegistryService: runtimeRegistryService as any,
      dynamicResolver: {
        dynamicResolver: vi.fn(),
        dynamicMutationResolver: vi.fn(),
      } as any,
      eventEmitter: new EventEmitter2(),
      envService: { isProd: false } as any,
    });

    await expect(service.reloadSchema()).rejects.toThrow('metadata missing');

    expect(service.getStatus()).toEqual(
      expect.objectContaining({
        schemaReady: false,
        lastReload: expect.objectContaining({
          status: 'degraded',
          error: 'metadata missing',
        }),
      }),
    );
  });
});
