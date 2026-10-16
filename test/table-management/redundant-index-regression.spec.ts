import { describe, expect, it, vi } from 'vitest';
import { RuntimeSchemaContractCompilerService } from '../../src/modules/table-management/services/runtime-schema-contract-compiler.service';
import { RuntimeMetadataSchemaRouterService } from '../../src/modules/table-management/services/runtime-metadata-schema-router.service';
import { TableManagementValidationService } from '../../src/modules/table-management/services/table-validation.service';
import { buildSqlIndexContracts } from '../../src/engines/knex/utils/sql-physical-schema-contract';
import { getMongoCanonicalConstraintGroups } from '../../src/engines/mongo/utils/mongo-physical-schema-contract';
import { SqlSchemaDiffService } from '../../src/engines/knex/services/sql-schema-diff.service';
import { MongoSchemaDiffService } from '../../src/engines/mongo/services/mongo-schema-diff.service';

const schema = {
  id: 42,
  name: 'user_discount',
  columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
  relations: [
    { id: 2, propertyName: 'user', type: 'many-to-one', targetTableName: 'enfyra_user', foreignKeyColumn: 'userId' },
    { id: 3, propertyName: 'discount', type: 'many-to-one', targetTableName: 'discount', foreignKeyColumn: 'discountId' },
  ],
  uniques: [['user', 'discount']],
  indexes: [['createdAt'], ['updatedAt'], ['discount']],
};

function compiler(database: string) {
  return new RuntimeSchemaContractCompilerService({
    databaseConfigService: { getDbType: () => database } as any,
    runtimeRegistryService: { getMetadata: () => ({ tables: new Map() }) } as any,
    runtimeSchemaPhysicalPlannerService: { plan: vi.fn(async () => null) } as any,
  });
}

describe('redundant relation indexes', () => {
  it.each(['postgres', 'mysql', 'mongodb'])('rejects a newly added unique-prefix index before planning on %s', async (database) => {
    await expect(compiler(database).compile({
      operation: 'update', tableName: schema.name, beforeMetadata: schema,
      afterMetadata: { ...schema, indexes: [...schema.indexes, ['user']] },
    })).rejects.toMatchObject({ details: { code: 'SCHEMA_INDEX_OVER_UNIQUE_FIELD' } });
  });

  it.each(['postgres', 'mysql', 'mongodb'])('plans removal of a persisted redundant index on %s instead of a successful no-op', async (database) => {
    const { contract } = await compiler(database).compile({
      operation: 'update', tableName: schema.name,
      beforeMetadata: { ...schema, indexes: [...schema.indexes, ['user']] },
      afterMetadata: schema,
    });
    expect(contract.context.diff.schemaChanged).toBe(true);
    expect(contract.context.diff.removedIndexes).toContain(JSON.stringify(['user']));
    expect(contract.phases.length).toBeGreaterThan(0);
    expect(contract.context.target?.indexes).not.toContainEqual(['user']);
  });

  it('rejects a manually declared owning relation index on create because the relation already owns its automatic lookup', async () => {
    const router = new RuntimeMetadataSchemaRouterService({
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
    } as any);
    await expect(router.createTable({ body: { ...schema, uniques: [], indexes: [['user']] } as any }))
      .rejects.toMatchObject({ details: { code: 'SCHEMA_INDEX_AUTO_MANAGED' } });
  });

  it('rejects a unique prefix expressed through the persisted physical FK alias', async () => {
    await expect(compiler('postgres').compile({
      operation: 'update', tableName: schema.name, beforeMetadata: schema,
      afterMetadata: { ...schema, indexes: [...schema.indexes, ['userId']] },
    })).rejects.toMatchObject({ details: { code: 'SCHEMA_INDEX_OVER_UNIQUE_FIELD' } });
  });

  it('does not regenerate a SQL owning FK index covered by a compound unique prefix', () => {
    const indexes = buildSqlIndexContracts(schema.name, schema as any);
    expect(indexes.map(index => index.logicalColumns)).not.toContainEqual(['user']);
    expect(indexes.map(index => index.logicalColumns)).toContainEqual(['discount']);
    expect(buildSqlIndexContracts(schema.name, { ...schema, indexes: [['discount', 'user']] } as any)
      .map(index => index.logicalColumns)).toContainEqual(['discount', 'user']);
  });

  it('does not regenerate a Mongo owning relation index covered by a compound unique prefix', () => {
    const metadata = { ...schema, relations: schema.relations.map(({ foreignKeyColumn: _fk, ...relation }) => relation) };
    expect(getMongoCanonicalConstraintGroups({ ...metadata, collectionName: schema.name } as any).indexes).not.toContainEqual(['user']);
  });

  it('plans the existing SQL redundant physical index as a drop candidate', async () => {
    const diff = await new SqlSchemaDiffService({ knexService: { getKnex: () => ({}) }, metadataCacheService: {}, queryBuilderService: {} } as any).generateSchemaDiff(
      { ...schema, indexes: [...schema.indexes, ['user']] }, schema,
    );
    expect(diff.constraints.indexes.delete).toContainEqual(['userId', 'id']);
    expect(diff.constraints.indexes.create).not.toContainEqual(['userId', 'id']);
  });

  it('plans the existing Mongo redundant physical index as a drop candidate', () => {
    const metadata = { ...schema, relations: schema.relations.map(({ foreignKeyColumn: _fk, ...relation }) => relation) };
    const diff = new MongoSchemaDiffService({} as any).analyzeMongoIndexChanges(
      { ...metadata, indexes: [...metadata.indexes, ['user']] }, metadata,
    );
    expect(diff.delete).toContain('user_discount_user_fk_idx');
    expect(diff.create.map(index => index.logicalFields)).not.toContainEqual(['user']);
  });

  it('rejects through the aggregate router before normalization can hide the caller intent', async () => {
    const compile = vi.fn();
    const execute = vi.fn();
    const router = new RuntimeMetadataSchemaRouterService({
      queryBuilderService: { findOne: vi.fn(async () => schema) } as any,
      tableHandlerService: {} as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
      tableManagementValidationService: new TableManagementValidationService(),
      runtimeSchemaContractCompilerService: { compile } as any,
      runtimeSchemaExecutorService: { execute } as any,
    });
    await expect(router.updateTable({ tableId: 42, body: { indexes: [...schema.indexes, ['user']] } as any }))
      .rejects.toMatchObject({ details: { code: 'SCHEMA_INDEX_OVER_UNIQUE_FIELD' } });
    expect(compile).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
  });
});
