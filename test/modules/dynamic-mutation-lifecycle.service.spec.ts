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

    expect(result).toEqual({ message: 'Delete successfully!', statusCode: 200 });
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
});
