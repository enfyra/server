import { describe, expect, it, vi } from 'vitest';
import { SystemSafetyAuditorService } from '../../src/domain/policy/services/system-safety-auditor.service';

function makeService(tableColumns: any[] = [{ name: 'isSystem' }]) {
  const runtimeRegistryService = {
    requireMetadata: vi.fn().mockReturnValue({
      tables: new Map([
        [
          'app_user',
          {
            name: 'app_user',
            columns: tableColumns,
          },
        ],
        [
          'enfyra_storage_config',
          {
            name: 'enfyra_storage_config',
            columns: tableColumns,
          },
        ],
      ]),
    }),
  };
  const schemaMigrationValidatorService = {
    getAllRelationFieldsWithInverse: vi.fn().mockResolvedValue([]),
    getChangedFields: vi.fn((data) => Object.keys(data || {})),
    getJsonFields: vi.fn().mockResolvedValue([]),
    excludeJsonFields: vi.fn((data) => data),
    enrichTableDefinitionData: vi.fn((data) => data),
    getAllowedFields: vi.fn((fields) => fields),
  };
  const commonService = {
    assertNoSystemFlagDeep: vi.fn(),
  };

  return {
    service: new SystemSafetyAuditorService({
      commonService: commonService as any,
      runtimeRegistryService: runtimeRegistryService as any,
      schemaMigrationValidatorService: schemaMigrationValidatorService as any,
    }),
    commonService,
  };
}

describe('SystemSafetyAuditorService isSystem field contract', () => {
  it('rejects application creates that attempt isSystem=true', async () => {
    const { service } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'create',
        tableName: 'app_user',
        data: { email: 'user@example.com', isSystem: true },
        existing: null,
      }),
    ).rejects.toThrow(
      'Cannot create application record with isSystem = true',
    );
  });

  it('allows application creates that leave isSystem false', async () => {
    const { service, commonService } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'create',
        tableName: 'app_user',
        data: { email: 'user@example.com', isSystem: false },
        existing: null,
      }),
    ).resolves.toBeUndefined();

    expect(commonService.assertNoSystemFlagDeep).toHaveBeenCalledWith([
      { email: 'user@example.com', isSystem: false },
    ]);
  });

  it('rejects application updates that change isSystem', async () => {
    const { service } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'app_user',
        data: { isSystem: true },
        existing: { id: 1, isSystem: false },
      }),
    ).rejects.toThrow('Cannot modify isSystem');
  });

  it('rejects application deletes when cascade data identifies a system row', async () => {
    const { service } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'delete',
        tableName: 'app_user',
        data: { id: 1, isSystem: true },
        existing: null,
      }),
    ).rejects.toThrow('Cannot delete system record!');
  });

  it('ignores isSystem payloads for tables without an isSystem column', async () => {
    const { service } = makeService([{ name: 'email' }]);

    await expect(
      service.assertSystemSafe({
        operation: 'create',
        tableName: 'app_user',
        data: { email: 'user@example.com', isSystem: true },
        existing: null,
      }),
    ).resolves.toBeUndefined();
  });

  it('allows system storage config default flag updates', async () => {
    const { service } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_storage_config',
        data: { isDefault: true },
        existing: { id: 1, isSystem: true, isDefault: false },
      }),
    ).resolves.toBeUndefined();
  });

  it('rejects system storage config credential updates', async () => {
    const { service } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_storage_config',
        data: { secretAccessKey: 'new-secret' },
        existing: { id: 1, isSystem: true, secretAccessKey: 'old-secret' },
      }),
    ).rejects.toThrow(
      'Cannot modify system storage config (only allowed: description, isDefault): secretAccessKey',
    );
  });
});
