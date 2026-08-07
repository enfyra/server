import { describe, expect, it, vi } from 'vitest';
import { RuntimeMetadataSchemaRouterService } from '../../src/modules/table-management/services/runtime-metadata-schema-router.service';
import { TableManagementValidationService } from '../../src/modules/table-management/services/table-validation.service';

function createRouter() {
  return new RuntimeMetadataSchemaRouterService({
    databaseConfigService: {
      isMongoDb: () => false,
      getDbType: () => 'postgres',
    },
  } as any);
}

describe('RuntimeMetadataSchemaRouterService', () => {
  it('preserves complete nested policy metadata when the request carries only child ids', () => {
    const existingPermission = {
      id: 41,
      action: 'read',
      effect: 'allow',
      condition: null,
      description: 'Local regression fixture',
      isEnabled: true,
      role: { id: 1 },
      allowedUsers: [],
    };
    const existingRule = {
      id: 8,
      ruleType: 'required',
      value: true,
      message: 'Required',
      description: null,
      isEnabled: true,
    };
    const router = createRouter();

    const target = (router as any).buildCompleteTarget(
      {
        name: 'example',
        indexes: [],
        uniques: [],
        columns: [
          {
            id: 7,
            name: 'secret',
            type: 'varchar',
            fieldPermissions: [existingPermission],
            rules: [existingRule],
          },
        ],
        relations: [],
      },
      {
        columns: [
          {
            id: 7,
            name: 'secret',
            type: 'varchar',
            isPublished: true,
            fieldPermissions: [{ id: 41 }],
            rules: [{ id: 8 }],
          },
        ],
      },
    );

    expect(target.columns[0].fieldPermissions).toEqual([existingPermission]);
    expect(target.columns[0].rules).toEqual([existingRule]);
  });

  it('preserves complete nested policy metadata for direct child updates with id-only entries', async () => {
    const existingPermission = {
      id: 41,
      action: 'read',
      effect: 'allow',
      condition: null,
      description: 'Local regression fixture',
      isEnabled: true,
      role: { id: 1 },
      allowedUsers: [],
    };
    const existingRule = {
      id: 8,
      ruleType: 'required',
      value: true,
      message: 'Required',
      description: null,
      isEnabled: true,
    };
    const compile = vi.fn().mockResolvedValue({
      contract: { context: { diff: { isDestructive: false } } },
      requiredConfirmHash: 'confirm',
    });
    const router = new RuntimeMetadataSchemaRouterService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'postgres',
      },
      queryBuilderService: {
        findOne: vi.fn().mockResolvedValue({
          id: 99,
          name: 'example',
          indexes: [],
          uniques: [],
          columns: [
            {
              id: 7,
              name: 'secret',
              type: 'varchar',
              fieldPermissions: [existingPermission],
              rules: [existingRule],
            },
          ],
          relations: [],
        }),
      },
      tableManagementValidationService: {
        validateColumns: vi.fn(),
        validateRelations: vi.fn(),
      },
      runtimeSchemaContractCompilerService: { compile },
      runtimeSchemaExecutorService: {
        execute: vi.fn().mockResolvedValue({ affectedTables: [] }),
      },
    } as any);

    await router.update({
      tableName: 'enfyra_column',
      recordId: 7,
      existing: { id: 7, table: { id: 99 } },
      data: {
        isPublished: true,
        fieldPermissions: [{ id: 41 }],
        rules: [{ id: 8 }],
      },
      context: { $query: {} },
    });

    const input = compile.mock.calls[0][0];
    expect(input.afterMetadata.columns[0].fieldPermissions).toEqual([
      existingPermission,
    ]);
    expect(input.afterMetadata.columns[0].rules).toEqual([existingRule]);
  });

  it('rejects unsupported validation rules on relations before compiling the schema contract', async () => {
    const compile = vi.fn().mockResolvedValue({
      contract: { context: { diff: { isDestructive: false } } },
      requiredConfirmHash: 'confirm',
    });
    const router = new RuntimeMetadataSchemaRouterService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'postgres',
      },
      tableManagementValidationService: new TableManagementValidationService(),
      runtimeSchemaContractCompilerService: { compile },
      runtimeSchemaExecutorService: {
        execute: vi.fn().mockResolvedValue({ affectedTables: [] }),
      },
    } as any);

    await expect(
      router.createTable({
        body: {
          name: 'example',
          columns: [],
          relations: [
            {
              propertyName: 'owner',
              type: 'many-to-one',
              rules: [{ ruleType: 'required' }],
            },
          ],
        } as any,
        context: { $query: {} },
      }),
    ).rejects.toThrow('not support validation rules');

    expect(compile).not.toHaveBeenCalled();
  });
});
