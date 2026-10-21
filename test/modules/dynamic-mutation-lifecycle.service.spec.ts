import { describe, expect, it } from 'vitest';
import { DynamicMutationLifecycleService } from '../../src/modules/dynamic-api/services/dynamic-mutation-lifecycle.service';
import { ConflictException } from '../../src/domain/exceptions';

function createOptions(overrides: Record<string, unknown> = {}): any {
  return {
    runtime: {
      find: async () => ({ data: [], count: 0 }),
      getIdField: () => 'id',
      reload: async () => {},
      emit: () => {},
    },
    routeRouter: { getStrategy: () => ({ kind: 'generic' }) },
    runtimeMetadataSchemaRouterService: { handles: () => false },
    queryBuilderService: { getDatabaseType: () => 'postgres' },
    schemaActivationService: { activate: async () => {} },
    tableName: 'enfyra_column_rule',
    tableMetadata: {},
    context: {},
    mutationPreparationService: {
      prepareCreateBody: async () => ({}),
      executeCreateBody: async () => ({ id: 1 }),
    },
    mutationAuthorizationService: {},
    tableValidationService: {},
    batchCreationService: {},
    data: {},
    ...overrides,
  };
}

describe('DynamicMutationLifecycleService', () => {
  it('runs generic mutation stages in lifecycle order', async () => {
    const service = new DynamicMutationLifecycleService();
    const calls: string[] = [];

    const result = await service.run({
      context: {
        tableName: 'enfyra_flow',
        id: 42,
        body: {},
        existing: { id: 42 },
      },
      persist: async () => {
        calls.push('persist');
        return undefined;
      },
      afterWrite: async () => {
        calls.push('afterWrite');
      },
      buildResult: () => {
        calls.push('buildResult');
        return { message: 'Delete successfully!', statusCode: 200 };
      },
      reload: async () => {
        calls.push('reload');
      },
      afterReload: async () => {
        calls.push('afterReload');
      },
      emit: () => {
        calls.push('emit');
      },
    });

    expect(result).toEqual({
      message: 'Delete successfully!',
      statusCode: 200,
    });
    expect(calls).toEqual([
      'persist',
      'afterWrite',
      'buildResult',
      'reload',
      'afterReload',
      'emit',
    ]);
  });

  it('recovers only errors after the write lifecycle hook', async () => {
    const service = new DynamicMutationLifecycleService();
    const calls: string[] = [];

    const result = await service.run({
      context: {
        tableName: 'enfyra_route',
        id: 42,
        body: {},
        existing: null,
      },
      persist: async () => {
        calls.push('persist');
        return { id: 42 };
      },
      afterWrite: async () => {
        calls.push('afterWrite');
      },
      buildResult: () => {
        calls.push('buildResult');
        throw new Error('incompatible operator');
      },
      reload: async () => {
        calls.push('reload');
      },
      recover: async (_context, persisted, error) => {
        calls.push('recover');
        expect(persisted).toEqual({ id: 42 });
        expect(error).toHaveProperty('message', 'incompatible operator');
        return { data: [persisted] };
      },
    });

    expect(result).toEqual({ data: [{ id: 42 }] });
    expect(calls).toEqual(['persist', 'afterWrite', 'buildResult', 'recover']);
  });

  it('waits for critical reload after creating a route or method', async () => {
    const service = new DynamicMutationLifecycleService();
    const reloadOptions: any[] = [];

    await service.create(
      createOptions({
        tableName: 'enfyra_route',
        runtime: {
          find: async () => ({ data: [{ id: 1 }], count: 1 }),
          getIdField: () => 'id',
          reload: async (options: any) => {
            reloadOptions.push(options);
          },
          emit: () => {},
        },
        routeRouter: {
          getStrategy: () => ({
            kind: 'generic',
            requiresCriticalReload: true,
          }),
        },
      }),
    );

    expect(reloadOptions).toEqual([{ ids: [1], critical: true }]);
  });

  it('preserves a domain conflict status raised while preparing a create', async () => {
    const service = new DynamicMutationLifecycleService();

    await expect(
      service.create(
        createOptions({
          mutationPreparationService: {
            prepareCreateBody: async () => {
              throw new ConflictException(
                "Rule of type 'format' already exists for this column",
              );
            },
            executeCreateBody: async () => ({ id: 1 }),
          },
        }),
      ),
    ).rejects.toMatchObject({ statusCode: 409, errorCode: 'CONFLICT' });
  });

  it('preserves a domain conflict status raised while preparing an update', async () => {
    const service = new DynamicMutationLifecycleService();

    await expect(
      service.update(
        createOptions({
          id: 1,
          mutationPreparationService: {
            prepareUpdateBody: () => {
              throw new ConflictException(
                "Rule of type 'format' already exists for this column",
              );
            },
            executeUpdateBody: async () => ({ id: 1 }),
          },
        }),
      ),
    ).rejects.toMatchObject({ statusCode: 409, errorCode: 'CONFLICT' });
  });

  it('appends a pre-image audit entry for a versioned update', async () => {
    const service = new DynamicMutationLifecycleService();
    const inserted: any[] = [];
    const knex = () => ({
      insert: async (row: Record<string, any>) => {
        inserted.push(row);
      },
    });
    const queryBuilderService = {
      getDatabaseType: () => 'postgres',
      isMongoDb: () => false,
      getKnex: () => knex,
      find: async () => ({
        data: [
          {
            id: 3,
            name: 'handler',
            version: '1.0.0',
            sourceCode: 'return 1;',
            compiledCode: 'compiled()',
          },
        ],
      }),
      update: async () => undefined,
    };

    await service.update(
      createOptions({
        tableName: 'enfyra_route_handler',
        tableMetadata: {
          columns: [
            { name: 'id' },
            { name: 'name' },
            { name: 'version' },
            { name: 'sourceCode' },
            { name: 'compiledCode' },
          ],
        },
        queryBuilderService,
        runtime: {
          find: async () => ({
            data: [{ id: 3, version: '1.0.0', sourceCode: 'return 1;' }],
            count: 1,
          }),
          getIdField: () => 'id',
          reload: async () => {},
          emit: () => {},
        },
        context: {
          $user: { id: 9, email: 'root@example.com' },
          $authSource: 'jwt',
        },
        id: 3,
        data: { version: '1.0.1', sourceCode: 'return 2;' },
        mutationPreparationService: {
          prepareUpdateBody: (body: any) => body,
          normalizeUpdate: (_table: string, body: any) => body,
        },
        mutationAuthorizationService: {
          stripUnauthorizedDirectFields: async (_a: any, body: any) => body,
          assertMutationSafety: async () => {},
          runWithFieldPermissionCheck: async (fn: any) => fn(),
          runWithMutationPolicy: async (fn: any) => fn(),
        },
        tableValidationService: { assertTableValid: async () => {} },
      }),
    );

    expect(inserted).toHaveLength(1);
    expect(inserted[0]).toMatchObject({
      tableName: 'enfyra_route_handler',
      recordId: '3',
      action: 'update',
      fromVersion: '1.0.0',
      toVersion: '1.0.1',
      changedById: 9,
      changedByEmail: 'root@example.com',
      changedBySource: 'jwt',
    });
    expect(JSON.parse(inserted[0].previous)).toEqual({
      name: 'handler',
      version: '1.0.0',
      sourceCode: 'return 1;',
    });
  });

  it('does not audit a table outside the versioned set', async () => {
    const service = new DynamicMutationLifecycleService();
    const inserted: any[] = [];
    const knex = () => ({
      insert: async (row: Record<string, any>) => {
        inserted.push(row);
      },
    });

    await service.update(
      createOptions({
        tableName: 'enfyra_flow',
        queryBuilderService: {
          getDatabaseType: () => 'postgres',
          isMongoDb: () => false,
          getKnex: () => knex,
          find: async () => ({ data: [{ id: 3 }] }),
          update: async () => undefined,
        },
        runtime: {
          find: async () => ({ data: [{ id: 3 }], count: 1 }),
          getIdField: () => 'id',
          reload: async () => {},
          emit: () => {},
        },
        id: 3,
        data: { name: 'flow' },
        mutationPreparationService: {
          prepareUpdateBody: (body: any) => body,
          normalizeUpdate: (_table: string, body: any) => body,
        },
        mutationAuthorizationService: {
          stripUnauthorizedDirectFields: async (_a: any, body: any) => body,
          assertMutationSafety: async () => {},
          runWithFieldPermissionCheck: async (fn: any) => fn(),
          runWithMutationPolicy: async (fn: any) => fn(),
        },
        tableValidationService: { assertTableValid: async () => {} },
      }),
    );

    expect(inserted).toHaveLength(0);
  });
});
