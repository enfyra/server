import { describe, expect, it, vi } from 'vitest';
import { DynamicMutationLifecycleService } from '../../src/modules/dynamic-api/services/dynamic-mutation-lifecycle.service';
import { DynamicMutationAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-mutation-authorization.service';
import { PolicyService } from '../../src/domain/policy';
import { SystemSafetyAuditorService } from '../../src/domain/policy/services/system-safety-auditor.service';

function buildAuditor() {
  const metadata = {
    tables: new Map([
      [
        'enfyra_user',
        {
          name: 'enfyra_user',
          columns: [{ name: 'isSystem' }, { name: 'isRootAdmin' }, { name: 'email' }],
        },
      ],
    ]),
  };
  return new SystemSafetyAuditorService({
    commonService: { assertNoSystemFlagDeep: () => undefined } as any,
    runtimeRegistryService: { requireMetadata: () => metadata } as any,
    schemaMigrationValidatorService: {
      getAllRelationFieldsWithInverse: async () => [],
      getChangedFields: () => [],
      getJsonFields: async () => [],
      excludeJsonFields: (data: any) => data,
      getAllowedFields: (fields: string[]) => fields,
      enrichTableDefinitionData: (data: any) => data,
    } as any,
    queryBuilderService: { find: async () => ({ data: [] }) } as any,
  });
}

function buildService(stored: Record<string, any>) {
  const auditor = buildAuditor();
  const policyService = new PolicyService({
    systemSafetyAuditorService: auditor,
    schemaMigrationValidatorService: {} as any,
  });
  const authorization = new DynamicMutationAuthorizationService({
    context: { $user: { id: 'ordinary' } } as any,
    enforceFieldPermission: true,
    policyService,
    queryBuilderService: {
      runWithPolicy: async (_check: unknown, callback: () => unknown) => callback(),
    } as any,
    runtimeRegistryService: { lookupTableByName: async () => ({ columns: [], relations: [] }) } as any,
    tableName: 'enfyra_user',
  });
  const find = vi.fn(async () => ({ data: [{ ...stored }] }));
  const remove = vi.fn(async () => ({ deleted: 1 }));
  const service = new DynamicMutationLifecycleService();
  const options = {
    runtime: {
      find: async () => ({ data: [{ id: stored.id }] }),
      getIdField: () => 'id',
      reload: async () => {},
      emit: () => {},
    },
    routeRouter: { getStrategy: () => ({ kind: 'generic' }) },
    runtimeMetadataSchemaRouterService: { handles: () => false },
    queryBuilderService: { find, delete: remove, getDatabaseType: () => 'postgres' },
    schemaActivationService: { activate: async () => {} },
    mutationPreparationService: {},
    mutationAuthorizationService: authorization,
    tableValidationService: { assertTableValid: async () => {} },
    batchCreationService: {},
    tableName: 'enfyra_user',
    tableMetadata: {},
    context: { $user: { id: 'ordinary' }, $query: { fields: 'id' } },
    id: stored.id,
  };
  return { service, options, find, remove };
}

describe('delete safety reads the canonical persisted record', () => {
  it('rejects deleting a root user even when the caller projects only the id', async () => {
    const { service, options, find, remove } = buildService({
      id: 'root',
      isRootAdmin: true,
    });

    await expect(service.delete(options as any)).rejects.toThrow(
      'Cannot delete Root Admin user',
    );
    expect(find).toHaveBeenCalledWith(
      expect.objectContaining({ table: 'enfyra_user', fields: '*' }),
    );
    expect(remove).not.toHaveBeenCalled();
  });

  it('rejects deleting a system row the caller cannot see', async () => {
    const { service, options, remove } = buildService({
      id: 'root',
      isSystem: true,
    });

    await expect(service.delete(options as any)).rejects.toThrow(
      'Cannot delete system record!',
    );
    expect(remove).not.toHaveBeenCalled();
  });

  it('deletes an ordinary record', async () => {
    const { service, options, remove } = buildService({
      id: 'member',
      isRootAdmin: false,
    });

    await expect(service.delete(options as any)).resolves.toBeDefined();
    expect(remove).toHaveBeenCalledWith('enfyra_user', 'member');
  });
});
