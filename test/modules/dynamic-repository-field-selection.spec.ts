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
  ]),
};

function makeRepo({
  fields,
  deep,
  tableName = 'enfyra_flow_step',
}: {
  fields?: string | string[];
  deep?: Record<string, any>;
  tableName?: string;
} = {}) {
  const queryBuilderService = {
    getPkField: vi.fn(() => (tableName === 'mongo_doc' ? '_id' : 'id')),
    find: vi.fn().mockResolvedValue({ data: [], count: 0 }),
  };
  const metadataCacheService = {
    lookupTableByName: vi.fn().mockResolvedValue(metadata.tables.get(tableName)),
    getMetadata: vi.fn().mockResolvedValue(metadata),
  };
  const settingCacheService = {
    getMaxQueryDepth: vi.fn().mockResolvedValue(10),
  };
  const repo = new DynamicRepository({
    context: { $query: { fields, deep } } as any,
    tableName,
    queryBuilderService: queryBuilderService as any,
    tableHandlerService: {} as any,
    policyService: {} as any,
    tableValidationService: {} as any,
    metadataCacheService: metadataCacheService as any,
    settingCacheService: settingCacheService as any,
    eventEmitter: {} as any,
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
      fields: 'id,sourceCode,owner.email',
      deep: undefined,
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
          fields: ['id', 'email', 'displayName', 'role'],
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
        fields: ['id', 'email', 'displayName', 'role'],
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
        fields: ['id', 'email', 'displayName', 'role'],
        deep: {
          role: {
            fields: ['id', 'name'],
            deep: undefined,
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
      fields: ['id', 'key', 'sourceCode', 'compiledCode', 'scriptLanguage', 'flow'],
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
      deep: { owner: { fields: 'avatar', filter: { email: { _contains: '@' } } } },
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
            fields: ['id', 'email', 'displayName', 'role'],
            filter: { email: { _contains: '@' } },
          },
        },
      }),
    );
  });
});
