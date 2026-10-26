import { describe, expect, it } from 'vitest';
import { SystemSafetyAuditorService } from '../../src/domain/policy/services/system-safety-auditor.service';

describe('code audit append-only policy', () => {
  it.each(['create', 'update'])(
    'rejects application %s even for a root administrator',
    async (operation) => {
      const service = new SystemSafetyAuditorService({} as any);
      await expect(
        service.assertSystemSafe({
          operation,
          tableName: 'enfyra_audit',
          currentUser: { isRootAdmin: true },
        }),
      ).rejects.toThrow('Code history is read-only');
    },
  );

  it('allows pruning an audit entry', async () => {
    const service = new SystemSafetyAuditorService({
      runtimeRegistryService: { requireMetadata: () => ({ tables: new Map() }) },
      schemaMigrationValidatorService: {
        getAllRelationFieldsWithInverse: async () => [],
        getChangedFields: () => [],
        getJsonFields: async () => [],
        excludeJsonFields: (data: Record<string, unknown>) => data,
      },
      commonService: { assertNoSystemFlagDeep: () => {} },
    } as any);
    await expect(
      service.assertSystemSafe({
        operation: 'delete',
        tableName: 'enfyra_audit',
        currentUser: { isRootAdmin: true },
      }),
    ).resolves.toBeUndefined();
  });
});
