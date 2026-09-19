import { describe, expect, it, vi } from 'vitest';
import { SystemSafetyAuditorService } from '../../src/domain/policy/services/system-safety-auditor.service';

function makeService() {
  const schemaMigrationValidatorService = {
    getChangedFields: vi.fn().mockReturnValue([]),
    getAllowedFields: vi.fn().mockReturnValue([]),
    getJsonFields: vi.fn().mockResolvedValue([]),
    excludeJsonFields: vi.fn().mockReturnValue({}),
    getAllRelationFieldsWithInverse: vi.fn().mockResolvedValue([]),
  };
  const runtimeRegistryService = {
    requireMetadata: vi.fn().mockReturnValue({
      tables: new Map([
        [
          'enfyra_auth_header',
          { name: 'enfyra_auth_header', columns: [{ name: 'isSystem' }] },
        ],
      ]),
    }),
  };
  const service = new SystemSafetyAuditorService({
    commonService: { assertNoSystemFlagDeep: vi.fn() } as any,
    runtimeRegistryService: runtimeRegistryService as any,
    schemaMigrationValidatorService: schemaMigrationValidatorService as any,
    queryBuilderService: {} as any,
  });
  return {
    assertSystemSafe: (ctx: any) =>
      (service as any).assertSystemSafe({
        currentUser: null,
        ...ctx,
      }),
  };
}

describe('SystemSafetyAuditorService auth header invariants', () => {
  it('allows deleting an auth header mapping', async () => {
    const { assertSystemSafe } = makeService();

    await expect(
      assertSystemSafe({
        operation: 'delete',
        tableName: 'enfyra_auth_header',
        data: {},
        existing: { id: 7, headerKey: 'authorization' },
      }),
    ).resolves.toBeUndefined();
  });

  it('still rejects an uppercase headerKey on create', async () => {
    const { assertSystemSafe } = makeService();

    await expect(
      assertSystemSafe({
        operation: 'create',
        tableName: 'enfyra_auth_header',
        data: { headerKey: 'Authorization' },
        existing: null,
      }),
    ).rejects.toThrow('headerKey must be normalized lowercase');
  });
});
