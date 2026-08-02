import { describe, expect, it, vi } from 'vitest';
import { SystemSafetyAuditorService } from '../../src/domain/policy/services/system-safety-auditor.service';

function makeService(options: {
  operationNamesById?: Record<string, string>;
  graphqlConfigsById?: Record<string, any>;
  permissions?: any[];
} = {}) {
  const queryBuilderService = {
    getPkField: vi.fn().mockReturnValue('id'),
    find: vi.fn().mockResolvedValue({ data: options.permissions ?? [] }),
    findOne: vi.fn().mockImplementation(async (query: any) => {
      const id = String(query.where.id);
      if (query.table === 'enfyra_graphql_operation') {
        const name = options.operationNamesById?.[id];
        return name ? { id, name } : null;
      }
      if (query.table === 'enfyra_graphql') {
        return options.graphqlConfigsById?.[id] ?? null;
      }
      return null;
    }),
  };
  const service = new SystemSafetyAuditorService({
    commonService: {} as any,
    runtimeRegistryService: {} as any,
    schemaMigrationValidatorService: {} as any,
    queryBuilderService: queryBuilderService as any,
  });

  return {
    service,
    queryBuilderService,
    assertGraphqlMetadataSafe: (ctx: any) =>
      (service as any).assertGraphqlMetadataSafe(ctx),
  };
}

describe('SystemSafetyAuditorService GraphQL metadata invariants', () => {
  it.each(['create', 'update', 'delete']) (
    'rejects %s for canonical GraphQL operation records',
    async (operation) => {
      const { assertGraphqlMetadataSafe } = makeService();
      await expect(
        assertGraphqlMetadataSafe({
          operation,
          tableName: 'enfyra_graphql_operation',
          data: {},
          existing: {},
        }),
      ).rejects.toThrow(
        'Canonical GraphQL operations are immutable and cannot be created, updated, or deleted',
      );
    },
  );

  it('resolves relation IDs through the canonical operation registry', async () => {
    const { assertGraphqlMetadataSafe, queryBuilderService } = makeService({
      operationNamesById: { 'op-update': 'UPDATE' },
      graphqlConfigsById: {
        'graphql-1': { id: 'graphql-1', publicOperations: [] },
      },
    });

    await expect(
      assertGraphqlMetadataSafe({
        operation: 'create',
        tableName: 'enfyra_graphql_permission',
        data: {
          role: 'role-1',
          allowedUsers: [],
          operations: ['op-update'],
          graphql: 'graphql-1',
        },
        existing: null,
      }),
    ).resolves.toBeUndefined();

    expect(queryBuilderService.findOne).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_graphql_operation',
        where: { id: 'op-update' },
      }),
    );
  });

  it('rejects an unknown operation relation ID', async () => {
    const { assertGraphqlMetadataSafe } = makeService();

    await expect(
      assertGraphqlMetadataSafe({
        operation: 'create',
        tableName: 'enfyra_graphql_permission',
        data: {
          role: 'role-1',
          allowedUsers: [],
          operations: ['missing-operation'],
          graphql: 'graphql-1',
        },
        existing: null,
      }),
    ).rejects.toThrow(
      'Unknown GraphQL operation reference: missing-operation',
    );
  });

  it('uses effective update state when validating permission scope', async () => {
    const { assertGraphqlMetadataSafe } = makeService({
      graphqlConfigsById: {
        'graphql-1': { id: 'graphql-1', publicOperations: [] },
      },
    });

    await expect(
      assertGraphqlMetadataSafe({
        operation: 'update',
        tableName: 'enfyra_graphql_permission',
        data: { isEnabled: false },
        existing: {
          role: { id: 'role-1' },
          allowedUsers: [],
          operations: [{ name: 'UPDATE' }],
          graphql: { id: 'graphql-1' },
        },
      }),
    ).resolves.toBeUndefined();
  });

  it('rejects permissions with neither or both scopes', async () => {
    const { assertGraphqlMetadataSafe } = makeService();
    const baseData = {
      operations: [{ name: 'UPDATE' }],
      graphql: 'graphql-1',
    };

    await expect(
      assertGraphqlMetadataSafe({
        operation: 'create',
        tableName: 'enfyra_graphql_permission',
        data: { ...baseData, role: null, allowedUsers: [] },
        existing: null,
      }),
    ).rejects.toThrow('GraphQL permission must target exactly one scope');

    await expect(
      assertGraphqlMetadataSafe({
        operation: 'create',
        tableName: 'enfyra_graphql_permission',
        data: {
          ...baseData,
          role: 'role-1',
          allowedUsers: ['user-1'],
        },
        existing: null,
      }),
    ).rejects.toThrow('GraphQL permission must target exactly one scope');
  });

  it('rejects permission operations that overlap the config public operations', async () => {
    const { assertGraphqlMetadataSafe } = makeService({
      graphqlConfigsById: {
        'graphql-1': {
          id: 'graphql-1',
          publicOperations: [{ name: 'UPDATE' }],
        },
      },
    });

    await expect(
      assertGraphqlMetadataSafe({
        operation: 'create',
        tableName: 'enfyra_graphql_permission',
        data: {
          role: 'role-1',
          allowedUsers: [],
          operations: [{ name: 'UPDATE' }],
          graphql: 'graphql-1',
        },
        existing: null,
      }),
    ).rejects.toThrow(
      'Public GraphQL operations cannot also be granted by permissions: UPDATE',
    );
  });

  it('rejects a config public operation that overlaps an existing permission', async () => {
    const { assertGraphqlMetadataSafe, queryBuilderService } = makeService({
      permissions: [
        { id: 'permission-1', operations: [{ name: 'QUERY' }] },
      ],
    });

    await expect(
      assertGraphqlMetadataSafe({
        operation: 'update',
        tableName: 'enfyra_graphql',
        data: { publicOperations: [{ name: 'QUERY' }] },
        existing: { id: 'graphql-1', publicOperations: [] },
      }),
    ).rejects.toThrow(
      'Public GraphQL operations cannot also be granted by permissions: QUERY',
    );

    expect(queryBuilderService.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_graphql_permission',
        filter: { graphql: { _eq: 'graphql-1' } },
      }),
    );
  });
});
