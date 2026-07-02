import { describe, expect, it, vi } from 'vitest';
import { DynamicRepository } from '../../src/modules/dynamic-api';
import { normalizeDynamicReadProjection } from '../../src/modules/dynamic-api/utils/field-selection.util';

const metadata = {
  tables: new Map<string, any>([
    [
      'enfyra_flow_step',
      {
        name: 'enfyra_flow_step',
        columns: [
          { name: 'id', isPrimary: true },
          { name: 'key' },
          { name: 'sourceCode' },
          { name: 'compiledCode' },
          { name: 'scriptLanguage' },
        ],
        relations: [
          {
            propertyName: 'owner',
            type: 'many-to-one',
            targetTableName: 'enfyra_user',
          },
          {
            propertyName: 'flow',
            type: 'many-to-one',
            targetTableName: 'enfyra_flow',
          },
        ],
      },
    ],
    [
      'enfyra_user',
      {
        name: 'enfyra_user',
        columns: [
          { name: 'id', isPrimary: true },
          { name: 'email' },
          { name: 'password', isPublished: false },
          { name: 'avatar' },
          { name: 'displayName' },
        ],
        relations: [
          {
            propertyName: 'role',
            type: 'many-to-one',
            targetTableName: 'enfyra_role',
          },
        ],
      },
    ],
    [
      'enfyra_role',
      {
        name: 'enfyra_role',
        columns: [
          { name: 'id', isPrimary: true },
          { name: 'name' },
          { name: 'permissionsBlob' },
        ],
        relations: [],
      },
    ],
    [
      'enfyra_flow',
      {
        name: 'enfyra_flow',
        columns: [
          { name: 'id', isPrimary: true },
          { name: 'name' },
          { name: 'metadata' },
        ],
        relations: [],
      },
    ],
    [
      'mongo_doc',
      {
        name: 'mongo_doc',
        columns: [
          { name: '_id', isPrimary: true },
          { name: 'title' },
          { name: 'payload' },
        ],
        relations: [],
      },
    ],
    [
      'secure_storage_config',
      {
        name: 'secure_storage_config',
        columns: [
          { name: 'id', isPrimary: true },
          { name: 'name' },
          { name: 'bucket' },
          { name: 'accessKeyId', isPublished: false },
          { name: 'secretAccessKey', isPublished: false },
          { name: 'accountId', isPublished: false },
          { name: 'credentials', isPublished: false },
        ],
        relations: [
          {
            propertyName: 'updatedBy',
            type: 'many-to-one',
            targetTableName: 'enfyra_user',
          },
        ],
      },
    ],
    [
      'enfyra_setting',
      {
        name: 'enfyra_setting',
        columns: [{ name: 'id', isPrimary: true }, { name: 'projectName' }],
        relations: [],
      },
    ],
    [
      'enfyra_menu',
      {
        name: 'enfyra_menu',
        columns: [
          { name: 'id', isPrimary: true },
          { name: 'label' },
          { name: 'path' },
        ],
        relations: [
          {
            propertyName: 'parent',
            type: 'many-to-one',
            targetTableName: 'enfyra_menu',
          },
          {
            propertyName: 'children',
            type: 'one-to-many',
            targetTableName: 'enfyra_menu',
          },
          {
            propertyName: 'extension',
            type: 'one-to-one',
            targetTableName: 'enfyra_extension',
          },
        ],
      },
    ],
    [
      'enfyra_extension',
      {
        name: 'enfyra_extension',
        columns: [{ name: 'id', isPrimary: true }, { name: 'name' }],
        relations: [],
      },
    ],
  ]),
};

const seededRows: Record<string, any[]> = {
  secure_storage_config: [
    {
      id: 2,
      name: 'Cloud R2',
      bucket: 'private-bucket',
      accessKeyId: 'access-key',
      secretAccessKey: 'secret-key',
      accountId: 'account-id',
      credentials: { private_key: 'secret' },
      updatedBy: {
        id: 'user-1',
        email: 'root@example.com',
        password: 'hashed-password',
        avatar: 'avatar.png',
        displayName: 'Root Admin',
      },
    },
  ],
};

function projectRows(tableName: string, rows: any[], args: any): any[] {
  const table = metadata.tables.get(tableName);
  const fieldTokens =
    args.fields === undefined || args.fields === null || args.fields === ''
      ? []
      : Array.isArray(args.fields)
        ? args.fields
        : String(args.fields)
            .split(',')
            .map((item) => item.trim())
            .filter(Boolean);
  const includeAll = fieldTokens.length === 0 || fieldTokens.includes('*');

  return rows.map((row) => {
    const out: Record<string, any> = {};
    const rootFields = includeAll
      ? [
          ...(table?.columns || []).map((column: any) => column.name),
          ...(table?.relations || []).map(
            (relation: any) => relation.propertyName,
          ),
        ]
      : fieldTokens;

    for (const field of rootFields) {
      if (field && Object.prototype.hasOwnProperty.call(row, field)) {
        out[field] = row[field];
      }
    }

    for (const [relationName, entry] of Object.entries(args.deep || {})) {
      const relation = table?.relations?.find(
        (item: any) => item.propertyName === relationName,
      );
      const targetTable = relation?.targetTableName;
      const relationValue = row[relationName];
      if (!targetTable || relationValue == null) continue;
      const childRows = Array.isArray(relationValue)
        ? relationValue
        : [relationValue];
      const projected = projectRows(targetTable, childRows, {
        fields: (entry as any)?.fields,
        deep: (entry as any)?.deep,
      });
      out[relationName] = Array.isArray(relationValue)
        ? projected
        : projected[0];
    }

    return out;
  });
}

function makeRepo({
  fields,
  deep,
  tableName = 'enfyra_flow_step',
  enforceFieldPermission = false,
  runtimeRegistryService,
}: {
  fields?: string | string[];
  deep?: Record<string, any>;
  tableName?: string;
  enforceFieldPermission?: boolean;
  runtimeRegistryService?: any;
} = {}) {
  const queryBuilderService = {
    getPkField: vi.fn(() => (tableName === 'mongo_doc' ? '_id' : 'id')),
    find: vi.fn().mockImplementation(async (args: any) => {
      const rows = seededRows[args.table] || [];
      return { data: projectRows(args.table, rows, args), count: rows.length };
    }),
  };
  const settingCacheService = {
    getMaxQueryDepth: vi.fn().mockResolvedValue(10),
  };
  const fieldPermissionCacheService = {
    getPoliciesFor: vi.fn().mockResolvedValue([]),
  };
  const activeRuntimeRegistryService = runtimeRegistryService ?? {
    requireMetadata: vi.fn(() => metadata),
    lookupTableByName: vi.fn((name: string) => metadata.tables.get(name)),
  };
  const repo = new DynamicRepository({
    context: { $query: { fields, deep } } as any,
    tableName,
    queryBuilderService: queryBuilderService as any,
    tableHandlerService: {} as any,
    policyService: {} as any,
    tableValidationService: {} as any,
    settingCacheService: settingCacheService as any,
    fieldPermissionCacheService: fieldPermissionCacheService as any,
    runtimeRegistryService: activeRuntimeRegistryService,
    eventEmitter: {} as any,
    enforceFieldPermission,
  });
  return { repo, queryBuilderService };
}

describe('dynamic read field selection', () => {
  it('keeps normal include mode unchanged', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: 'id,sourceCode,owner.email',
        deep: undefined,
        metadata,
      }),
    ).toEqual({
      fields: ['id', 'sourceCode', 'owner'],
      deep: { owner: { fields: ['email'] } },
    });
  });

  it('turns mixed wildcard and dotted relation includes into deep projections', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'secure_storage_config',
        fields: '*,updatedBy.*',
        metadata,
      }),
    ).toEqual({
      fields: '*',
      deep: { updatedBy: { fields: ['*'] } },
    });
  });

  it('leaves unknown dotted include tokens to the query layer', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_setting',
        fields: '*,methods.*',
        metadata,
      }),
    ).toEqual({
      fields: '*,methods.*',
      deep: undefined,
    });
  });

  it('does not reject mixed relation and non-relation dotted includes', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_menu',
        fields: '*,parent.*,children.*,sidebar.*,extension.*',
        metadata,
      }),
    ).toEqual({
      fields: '*',
      deep: {
        parent: { fields: ['*'] },
        children: { fields: ['*'] },
        extension: { fields: ['*'] },
      },
    });
  });

  it('treats any negative token as exclude mode and ignores positive tokens', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: 'id,-compiledCode',
        metadata,
      }).fields,
    ).toEqual(['id', 'key', 'sourceCode', 'scriptLanguage', 'owner', 'flow']);
  });

  it('supports root excludes without dropping the primary key unless requested', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-compiledCode,-key',
        metadata,
      }).fields,
    ).toEqual(['id', 'sourceCode', 'scriptLanguage', 'owner', 'flow']);
  });

  it('allows excluding the primary key explicitly', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-id,-compiledCode',
        metadata,
      }).fields,
    ).toEqual(['key', 'sourceCode', 'scriptLanguage', 'owner', 'flow']);
  });

  it('turns dotted relation excludes into deep all-minus projections', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-owner.avatar',
        metadata,
      }),
    ).toEqual({
      fields: [
        'id',
        'key',
        'sourceCode',
        'compiledCode',
        'scriptLanguage',
        'owner',
        'flow',
      ],
      deep: {
        owner: {
          fields: ['id', 'email', 'password', 'displayName', 'role'],
        },
      },
    });
  });

  it('lets top-level dotted excludes win over nested include fields', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-owner.avatar',
        deep: { owner: { fields: 'avatar' } },
        metadata,
      }).deep,
    ).toEqual({
      owner: {
        fields: ['id', 'email', 'password', 'displayName', 'role'],
      },
    });
  });

  it('supports nested deep exclude mode independently', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: 'id,owner',
        deep: {
          owner: {
            fields: '-avatar',
            deep: {
              role: {
                fields: '-permissionsBlob',
              },
            },
          },
        },
        metadata,
      }).deep,
    ).toEqual({
      owner: {
        fields: ['id', 'email', 'password', 'displayName', 'role'],
        deep: {
          role: {
            fields: ['id', 'name'],
          },
        },
      },
    });
  });

  it('removes deep entries when the owning relation is excluded', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-owner',
        deep: { owner: { fields: '-avatar' } },
        metadata,
      }),
    ).toEqual({
      fields: [
        'id',
        'key',
        'sourceCode',
        'compiledCode',
        'scriptLanguage',
        'flow',
      ],
      deep: {},
    });
  });

  it('uses _id as the primary key for Mongo-shaped metadata', () => {
    expect(
      normalizeDynamicReadProjection({
        tableName: 'mongo_doc',
        fields: '-payload',
        metadata,
      }).fields,
    ).toEqual(['_id', 'title']);
  });

  it('rejects unknown root fields in exclude mode', () => {
    expect(() =>
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-compildCode',
        metadata,
      }),
    ).toThrow("Unknown excluded field 'compildCode'");
  });

  it('rejects unknown dotted relations in exclude mode', () => {
    expect(() =>
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-missing.avatar',
        metadata,
      }),
    ).toThrow("Unknown excluded relation 'missing'");
  });

  it('rejects invalid wildcard exclusion', () => {
    expect(() =>
      normalizeDynamicReadProjection({
        tableName: 'enfyra_flow_step',
        fields: '-*',
        metadata,
      }),
    ).toThrow("Invalid excluded field '-*'");
  });

  it('passes rewritten exclude projection to QueryBuilderService.find', async () => {
    const { repo, queryBuilderService } = makeRepo({
      fields: 'id,-compiledCode',
    });

    await repo.find();

    expect(queryBuilderService.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_flow_step',
        fields: ['id', 'key', 'sourceCode', 'scriptLanguage', 'owner', 'flow'],
      }),
    );
  });

  it('passes rewritten dotted excludes and deep options to QueryBuilderService.find', async () => {
    const { repo, queryBuilderService } = makeRepo({
      fields: '-owner.avatar',
      deep: {
        owner: { fields: 'avatar', filter: { email: { _contains: '@' } } },
      },
    });

    await repo.find();

    expect(queryBuilderService.find).toHaveBeenCalledWith(
      expect.objectContaining({
        fields: [
          'id',
          'key',
          'sourceCode',
          'compiledCode',
          'scriptLanguage',
          'owner',
          'flow',
        ],
        deep: {
          owner: {
            fields: ['id', 'email', 'password', 'displayName', 'role'],
            filter: { email: { _contains: '@' } },
          },
        },
      }),
    );
  });

  it('does not request unpublished root or relation fields from a seeded secure row', async () => {
    const { repo, queryBuilderService } = makeRepo({
      tableName: 'secure_storage_config',
      fields: '*,updatedBy.*',
      enforceFieldPermission: true,
    });

    const result = await repo.find();

    expect(queryBuilderService.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'secure_storage_config',
        fields: 'id,name,bucket,updatedBy',
        deep: {
          updatedBy: {
            fields: 'id,email,avatar,displayName,role',
          },
        },
      }),
    );
    expect(result.data[0]).toEqual({
      id: 2,
      name: 'Cloud R2',
      bucket: 'private-bucket',
      updatedBy: {
        id: 'user-1',
        email: 'root@example.com',
        avatar: 'avatar.png',
        displayName: 'Root Admin',
      },
    });
    expect(result.data[0]).not.toHaveProperty('accessKeyId');
    expect(result.data[0]).not.toHaveProperty('secretAccessKey');
    expect(result.data[0].updatedBy).not.toHaveProperty('password');
  });

  it('reads repository metadata from the active runtime registry when available', async () => {
    const runtimeRegistryService = {
      requireMetadata: vi.fn(() => metadata),
      lookupTableByName: vi.fn((name: string) => metadata.tables.get(name)),
    };
    const { repo } = makeRepo({
      tableName: 'enfyra_flow_step',
      fields: 'id,key',
      runtimeRegistryService,
    });

    await repo.find();

    expect(runtimeRegistryService.requireMetadata).toHaveBeenCalled();
    expect(runtimeRegistryService.lookupTableByName).toHaveBeenCalledWith(
      'enfyra_flow_step',
    );
  });
});
