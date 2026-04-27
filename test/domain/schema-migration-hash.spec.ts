import { describe, it, expect } from 'vitest';
import { SchemaMigrationValidatorService } from 'src/domain/policy';

const metadataCacheStub: any = {
  async getMetadata() {
    return { tables: new Map() };
  },
};

const makeValidator = () =>
  new SchemaMigrationValidatorService({
    metadataCacheService: metadataCacheStub,
  });

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
