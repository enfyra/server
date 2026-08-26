import { describe, it, expect } from 'vitest';
import { SchemaMigrationValidatorService } from '../../src/domain/policy';
import { RuntimeSchemaContractCompilerService } from '../../src/modules/table-management';

const runtimeRegistryStub: any = {
  getMetadata() {
    return { tables: new Map() };
  },
};

const databaseConfigStub: any = {
  getDbType: () => 'postgres',
};

const makeValidator = () => {
  const runtimeSchemaContractCompilerService =
    new RuntimeSchemaContractCompilerService({
      databaseConfigService: databaseConfigStub,
      runtimeRegistryService: runtimeRegistryStub,
      runtimeSchemaPhysicalPlannerService: {
        plan: async () => null,
      } as any,
    });
  return new SchemaMigrationValidatorService({
    runtimeRegistryService: runtimeRegistryStub,
    runtimeSchemaContractCompilerService,
  });
};

const baseBefore = {
  name: 'post',
  columns: [
    {
      id: 1,
      name: 'id',
      type: 'int',
      isNullable: false,
      isPrimary: true,
      isGenerated: true,
      defaultValue: null,
    },
    {
      id: 2,
      name: 'title',
      type: 'varchar',
      isNullable: false,
      isPrimary: false,
      isGenerated: false,
      defaultValue: null,
    },
  ],
  relations: [],
  uniques: null,
  indexes: null,
};

const columnWithId = (id: any, name: string) => ({
  id,
  name,
  type: 'varchar',
  isNullable: true,
  isPrimary: false,
  isGenerated: false,
  defaultValue: null,
});

describe('SchemaMigrationValidatorService — hash stability', () => {
  it('returns requiredConfirmHash on preview when adding new column', async () => {
    const v = makeValidator();
    const after = {
      ...baseBefore,
      columns: [...baseBefore.columns, columnWithId(99, 'slug')],
    };
    const decision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: after,
      requestContext: { $query: {} },
    });
    expect(decision.allow).toBe(false);
    expect(decision.preview).toBe(true);
    expect(typeof decision.details.requiredConfirmHash).toBe('string');
    expect(decision.details.requiredConfirmHash.length).toBe(64);
  });

  it('produces identical hash when new column id differs between preview and confirm (PG reload case)', async () => {
    const v = makeValidator();
    // Simulate PG path: preview reloaded metadata with tmp id 100,
    // confirm reloaded metadata with tmp id 250 (rows re-inserted after rollback).
    const previewAfter = {
      ...baseBefore,
      columns: [...baseBefore.columns, columnWithId(100, 'slug')],
    };
    const confirmAfter = {
      ...baseBefore,
      columns: [...baseBefore.columns, columnWithId(250, 'slug')],
    };
    const previewDecision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: previewAfter,
      requestContext: { $query: {} },
    });
    const previewHash = previewDecision.details.requiredConfirmHash;

    const confirmDecision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: confirmAfter,
      requestContext: { $query: { schemaConfirmHash: previewHash } },
    });
    expect(confirmDecision.allow).toBe(true);
    expect(confirmDecision.details.requiredConfirmHash).toBe(previewHash);
    expect(confirmDecision.details.contractHash).toBe(
      previewDecision.details.contractHash,
    );
  });

  it('rejects when client hash is wrong', async () => {
    const v = makeValidator();
    const after = {
      ...baseBefore,
      columns: [...baseBefore.columns, columnWithId(100, 'slug')],
    };
    const decision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: after,
      requestContext: { $query: { schemaConfirmHash: 'deadbeef' } },
    });
    expect(decision.allow).toBe(false);
    expect(decision.statusCode).toBe(422);
    expect(decision.code).toBe('SCHEMA_CONFIRM_HASH_MISMATCH');
  });

  it('canonicalizes an exact index duplicate of a unique field before compiling a create schema', async () => {
    const v = makeValidator();
    const decision = await v.checkSchemaMigration({
      operation: 'create',
      tableName: 'enfyra_table',
      data: {
        name: 'runtime_versions',
        uniques: [{ value: ['version'] }],
        indexes: [
          { value: ['is_active', 'sort_order'] },
          { value: ['version'] },
        ],
      },
      requestContext: { $query: {} },
    });

    expect(decision.allow).toBe(true);
    expect(decision.details.schemaMutationContract.context.target.indexes).toEqual([
      ['createdAt'],
      ['is_active', 'sort_order'],
      ['updatedAt'],
    ]);
  });

  it('allows an index that overlaps a unique field without duplicating its lookup', async () => {
    const v = makeValidator();
    const after = {
      ...baseBefore,
      uniques: [['version'], ['docker_image']],
      indexes: [
        ['is_active', 'sort_order'],
        ['version', 'is_active'],
      ],
    };

    const decision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'runtime_versions',
      beforeMetadata: baseBefore,
      afterMetadata: after,
      requestContext: { $query: {} },
    });

    expect(decision.code).not.toBe('SCHEMA_INDEX_OVER_UNIQUE_FIELD');
    expect(decision.details.requiredConfirmHash).toHaveLength(64);
  });

  it('allows an update when a persisted automatic id-suffix index duplicates a unique field', async () => {
    const v = makeValidator();
    const before = {
      ...baseBefore,
      name: 'payment_order',
      columns: [
        ...baseBefore.columns,
        columnWithId(3, 'providerOrderId'),
      ],
      uniques: [['providerOrderId']],
      indexes: [['providerOrderId']],
    };
    const after = {
      ...before,
      columns: [...before.columns, columnWithId(4, 'note')],
    };

    const decision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'payment_order',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: {} },
    });

    expect(decision.allow).toBe(false);
    expect(decision.preview).toBe(true);
    expect(decision.code).not.toBe('SCHEMA_INDEX_OVER_UNIQUE_FIELD');
  });

  it('allows an update when a persisted owning relation index duplicates its unique relation', async () => {
    const v = makeValidator();
    const before = {
      ...baseBefore,
      name: 'member_profile',
      relations: [
        {
          propertyName: 'user',
          type: 'one-to-one',
          targetTableName: 'enfyra_user',
          foreignKeyColumn: 'userId',
        },
      ],
      uniques: [['user']],
      indexes: [['user']],
    };
    const after = {
      ...before,
      columns: [...before.columns, columnWithId(3, 'nickname')],
    };

    const decision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'member_profile',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: {} },
    });

    expect(decision.allow).toBe(false);
    expect(decision.preview).toBe(true);
    expect(decision.code).not.toBe('SCHEMA_INDEX_OVER_UNIQUE_FIELD');
  });

  it('allows a narrow id-suffix lookup alongside a composite unique constraint', async () => {
    const v = makeValidator();
    const before = {
      ...baseBefore,
      name: 'payment_order',
      columns: [
        ...baseBefore.columns,
        columnWithId(3, 'providerOrderId'),
        columnWithId(4, 'status'),
      ],
      uniques: [['paymentProvider', 'providerOrderId']],
      indexes: [['providerOrderId']],
    };
    const after = {
      ...before,
      columns: [...before.columns, columnWithId(5, 'note')],
    };

    const decision = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'payment_order',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: {} },
    });

    expect(decision.code).not.toBe('SCHEMA_INDEX_OVER_UNIQUE_FIELD');
    expect(decision.details.requiredConfirmHash).toHaveLength(64);
  });

  it('allows a shared lookup index for multiple composite unique constraints', async () => {
    const v = makeValidator();
    const decision = await v.checkSchemaMigration({
      operation: 'create',
      tableName: 'cloud_host_databases',
      data: {
        uniques: [
          ['host', 'db_name'],
          ['host', 'db_user'],
        ],
        indexes: [['host']],
      },
      requestContext: { $query: {} },
    });

    expect(decision.code).not.toBe('SCHEMA_INDEX_OVER_UNIQUE_FIELD');
    expect(decision.details.schemaMutationContract.context.target.indexes).toContainEqual([
      'host',
    ]);
  });

  it('hash differs when adding different column (not just id)', async () => {
    const v = makeValidator();
    const afterSlug = {
      ...baseBefore,
      columns: [...baseBefore.columns, columnWithId(100, 'slug')],
    };
    const afterBody = {
      ...baseBefore,
      columns: [...baseBefore.columns, columnWithId(100, 'body')],
    };
    const d1 = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: afterSlug,
      requestContext: { $query: {} },
    });
    const d2 = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: afterBody,
      requestContext: { $query: {} },
    });
    expect(d1.details.requiredConfirmHash).not.toBe(
      d2.details.requiredConfirmHash,
    );
  });

  it('MySQL/Mongo path — new column without id produces stable hash across calls', async () => {
    const v = makeValidator();
    const newColNoId = {
      name: 'slug',
      type: 'varchar',
      isNullable: true,
      isPrimary: false,
      isGenerated: false,
      defaultValue: null,
    };
    const after = {
      ...baseBefore,
      columns: [...baseBefore.columns, newColNoId],
    };
    const preview = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: after,
      requestContext: { $query: {} },
    });
    const confirm = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: after,
      requestContext: {
        $query: { schemaConfirmHash: preview.details.requiredConfirmHash },
      },
    });
    expect(confirm.allow).toBe(true);
  });

  it('name swap between two columns produces hash that differs from no-op', async () => {
    const v = makeValidator();
    const swapped = {
      ...baseBefore,
      columns: [
        { ...baseBefore.columns[0], name: 'title' },
        { ...baseBefore.columns[1], name: 'id' },
      ],
    };
    const d = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: swapped,
      requestContext: { $query: {} },
    });
    expect(d.details.renamedColumns).toEqual(
      expect.arrayContaining([
        { from: 'id', to: 'title' },
        { from: 'title', to: 'id' },
      ]),
    );
    expect(d.preview).toBe(true);
  });

  it('relations with new id on PG reload — hash stays stable', async () => {
    const v = makeValidator();
    const before = {
      ...baseBefore,
      relations: [],
    };
    const previewAfter = {
      ...baseBefore,
      relations: [
        {
          id: 500,
          propertyName: 'author',
          type: 'many-to-one',
          targetTableName: 'user',
          foreignKeyColumn: 'authorId',
          isNullable: true,
        },
      ],
    };
    const confirmAfter = {
      ...baseBefore,
      relations: [
        {
          id: 777,
          propertyName: 'author',
          type: 'many-to-one',
          targetTableName: 'user',
          foreignKeyColumn: 'authorId',
          isNullable: true,
        },
      ],
    };
    const preview = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: before,
      afterMetadata: previewAfter,
      requestContext: { $query: {} },
    });
    const confirm = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: before,
      afterMetadata: confirmAfter,
      requestContext: {
        $query: { schemaConfirmHash: preview.details.requiredConfirmHash },
      },
    });
    expect(confirm.allow).toBe(true);
  });

  it('Mongo path — ObjectId string in before vs id string in after for same column — hash stable', async () => {
    const v = makeValidator();
    const objId = '507f1f77bcf86cd799439011';
    const before = {
      name: 'post',
      columns: [
        {
          _id: objId,
          name: 'id',
          type: 'uuid',
          isNullable: false,
          isPrimary: true,
          isGenerated: true,
          defaultValue: null,
        },
      ],
      relations: [],
      uniques: null,
      indexes: null,
    };
    const after = {
      name: 'post',
      columns: [
        {
          id: objId,
          name: 'id',
          type: 'uuid',
          isNullable: false,
          isPrimary: true,
          isGenerated: true,
          defaultValue: null,
        },
        {
          name: 'slug',
          type: 'varchar',
          isNullable: true,
          isPrimary: false,
          isGenerated: false,
          defaultValue: null,
        },
      ],
      relations: [],
      uniques: null,
      indexes: null,
    };
    const preview = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: { $query: {} },
    });
    const confirm = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: before,
      afterMetadata: after,
      requestContext: {
        $query: { schemaConfirmHash: preview.details.requiredConfirmHash },
      },
    });
    expect(confirm.allow).toBe(true);
  });

  it('detects rename even though column id matches', async () => {
    const v = makeValidator();
    const after = {
      ...baseBefore,
      columns: [
        baseBefore.columns[0],
        { ...baseBefore.columns[1], name: 'heading' },
      ],
    };
    const preview = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: after,
      requestContext: { $query: {} },
    });
    expect(preview.details.renamedColumns).toEqual([
      { from: 'title', to: 'heading' },
    ]);
    const confirm = await v.checkSchemaMigration({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseBefore,
      afterMetadata: after,
      requestContext: {
        $query: { schemaConfirmHash: preview.details.requiredConfirmHash },
      },
    });
    expect(confirm.allow).toBe(true);
  });
});
