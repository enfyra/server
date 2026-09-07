import { describe, expect, it, vi } from 'vitest';
import { SystemSafetyAuditorService } from '../../src/domain/policy/services/system-safety-auditor.service';

function makeService(
  tableColumns: any[] = [{ name: 'isSystem' }],
  relationFields: string[] = [],
) {
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
        [
          'enfyra_table',
          {
            name: 'enfyra_table',
            columns: tableColumns,
          },
        ],
      ]),
    }),
  };
  const schemaMigrationValidatorService = {
    getAllRelationFieldsWithInverse: vi.fn().mockResolvedValue(relationFields),
    getChangedFields: vi.fn((data) => Object.keys(data || {})),
    getJsonFields: vi.fn().mockResolvedValue([]),
    excludeJsonFields: vi.fn((data) => data),
    enrichTableDefinitionData: vi.fn((data) => data),
    getAllowedFields: vi.fn((fields) => fields),
  };
  const commonService = {
    assertNoSystemFlagDeep: vi.fn((values: unknown[]) => {
      const containsSystemFlag = (value: unknown): boolean => {
        if (!value || typeof value !== 'object') return false;
        if (Array.isArray(value)) return value.some(containsSystemFlag);
        const record = value as Record<string, unknown>;
        if (record.isSystem === true) return true;
        return Object.values(record).some(containsSystemFlag);
      };
      if (containsSystemFlag(values)) {
        throw new Error('Cannot create system-owned nested metadata');
      }
    }),
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

  it('rejects nested system metadata minted through an application table update', async () => {
    const { service } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_table',
        data: {
          columns: [
            { name: 'id', type: 'int', isSystem: false },
            { name: 'forged', type: 'varchar', isSystem: true },
          ],
        },
        existing: { id: 10, name: 'application_table', isSystem: false },
      }),
    ).rejects.toThrow('Cannot create system-owned nested metadata');
  });

  it('rejects field permissions that mix role and user scopes', async () => {
    const { service } = makeService();

    await expect(
      service.assertSystemSafe({
        operation: 'create',
        tableName: 'enfyra_field_permission',
        data: {
          column: { id: 10 },
          role: { id: 20 },
          allowedUsers: [{ id: 30 }],
          action: 'read',
          effect: 'deny',
        },
        existing: null,
      }),
    ).rejects.toThrow('exactly one scope');
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

  it('allows deleting a non-system column from a system table', async () => {
    const { service } = makeService(undefined, ['columns', 'relations']);

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_table',
        data: {
          columns: [{ id: 11, name: 'id', isSystem: true }],
        },
        existing: {
          id: 10,
          name: 'enfyra_user',
          isSystem: true,
          columns: [
            { id: 11, name: 'id', isSystem: true, table: '10' },
            { name: 'createdAt', isSystem: true },
            { id: 12, name: 'fullName', isSystem: false },
          ],
          relations: [{ id: 21, propertyName: 'roles', isSystem: true }],
        },
      }),
    ).resolves.toBeUndefined();
  });

  it('ignores projected metadata access while preserving a system column', async () => {
    const { service } = makeService(undefined, ['columns']);

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_table',
        data: {
          columns: [
            {
              id: 11,
              name: 'id',
              isSystem: true,
              metadataAccess: {
                read: true,
                create: true,
                update: true,
                delete: true,
              },
            },
          ],
        },
        existing: {
          id: 10,
          name: 'enfyra_user',
          isSystem: true,
          columns: [
            { id: 11, name: 'id', isSystem: true, table: '10' },
            { id: 12, name: 'fullName', isSystem: false },
          ],
        },
      }),
    ).resolves.toBeUndefined();
  });

  it('ignores projected metadata access while preserving a system relation', async () => {
    const { service } = makeService(undefined, ['relations']);

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_table',
        data: {
          relations: [
            {
              id: 21,
              propertyName: 'roles',
              isSystem: true,
              metadataAccess: {
                read: true,
                create: true,
                update: true,
                delete: true,
              },
            },
          ],
        },
        existing: {
          id: 10,
          name: 'enfyra_user',
          isSystem: true,
          relations: [
            {
              id: 21,
              propertyName: 'roles',
              isSystem: true,
              sourceTable: '10',
            },
          ],
        },
      }),
    ).resolves.toBeUndefined();
  });

  it('treats omitted system-table child containers as unchanged', async () => {
    const { service } = makeService(undefined, ['columns', 'relations']);

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_table',
        data: { validateBody: true },
        existing: {
          id: 10,
          name: 'enfyra_user',
          isSystem: true,
          validateBody: true,
          columns: [{ id: 11, name: 'id', isSystem: true, table: '10' }],
          relations: [{ id: 21, propertyName: 'roles', isSystem: true }],
        },
      }),
    ).resolves.toBeUndefined();
  });

  it('matches serialized Mongo child identities to incoming string ids', async () => {
    const { service } = makeService(undefined, ['columns']);
    const serializedId = {
      buffer: Object.fromEntries(
        Array.from({ length: 12 }, (_, index) => [String(index), index + 1]),
      ),
    };

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_table',
        data: {
          columns: [
            {
              id: '0102030405060708090a0b0c',
              name: '_id',
              isSystem: true,
            },
          ],
        },
        existing: {
          id: 'table-id',
          name: 'enfyra_user',
          isSystem: true,
          columns: [
            { _id: serializedId, name: '_id', isSystem: true },
            { id: 'custom-id', name: 'fullName', isSystem: false },
          ],
        },
      }),
    ).resolves.toBeUndefined();
  });

  it('still rejects deleting a system column from a system table', async () => {
    const { service } = makeService(undefined, ['columns', 'relations']);

    await expect(
      service.assertSystemSafe({
        operation: 'update',
        tableName: 'enfyra_table',
        data: {
          columns: [{ id: 12, name: 'fullName', isSystem: false }],
        },
        existing: {
          id: 10,
          name: 'enfyra_user',
          isSystem: true,
          columns: [
            { id: 11, name: 'id', isSystem: true },
            { id: 12, name: 'fullName', isSystem: false },
          ],
        },
      }),
    ).rejects.toThrow(/Cannot delete system (record|column)/);
  });
});
