import { describe, expect, it, vi } from 'vitest';
import { RuntimeSchemaContractCompilerService } from '../../src/modules/table-management/services/runtime-schema-contract-compiler.service';
import { RuntimeMetadataSchemaRouterService } from '../../src/modules/table-management/services/runtime-metadata-schema-router.service';
import { TableManagementValidationService } from '../../src/modules/table-management/services/table-validation.service';
import { buildSqlIndexContracts } from '../../src/engines/knex/utils/sql-physical-schema-contract';
import { normalizeRuntimeTableSchema } from '../../src/modules/table-management/utils/runtime-schema-normalization.util';
import { stripOwningRelationIndexes } from '../../src/modules/table-management/utils/schema-index-validation.util';
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
  it.each(['postgres', 'mysql', 'mongodb'])('drops a caller-added unique-prefix index on an owning relation on %s instead of rejecting it', async (database) => {
    const { contract } = await compiler(database).compile({
      operation: 'update', tableName: schema.name, beforeMetadata: schema,
      afterMetadata: { ...schema, indexes: [...schema.indexes, ['user']] },
    });
    expect(contract.context.target?.indexes).not.toContainEqual(['user']);
  });

  it.each(['postgres', 'mysql', 'mongodb'])('still rejects a unique-prefix index over scalar columns on %s', async (database) => {
    const scalarSchema = {
      ...schema,
      relations: [],
      uniques: [['status', 'createdAt']],
      indexes: [],
    };
    await expect(compiler(database).compile({
      operation: 'update', tableName: scalarSchema.name, beforeMetadata: scalarSchema,
      afterMetadata: { ...scalarSchema, indexes: [['status']] },
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

  it('strips a manually declared owning relation index on create because the relation already owns its automatic lookup', async () => {
    const body = { ...schema, uniques: [], indexes: [['user'], ['createdAt']] };
    expect(stripOwningRelationIndexes(body, body.indexes)).toEqual([['createdAt']]);
    expect(stripOwningRelationIndexes({ ...body, relations: [] }, [['user']])).toEqual([['user']]);
    expect(stripOwningRelationIndexes(body, [['user', 'discount']])).toEqual([['user', 'discount']]);
  });

  it('keeps a caller-declared owning relation index out of the persisted constraint set on create', async () => {
    const router = new RuntimeMetadataSchemaRouterService({
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
    } as any);
    const normalized = (router as any).normalizeCompleteTableConstraints({
      ...schema,
      uniques: [],
      indexes: stripOwningRelationIndexes(schema, [['user']]),
    });
    expect(normalized.indexes).toContainEqual(['user']);
    expect(normalized.indexes.filter((group: string[]) => group.length === 1 && group[0] === 'user')).toHaveLength(1);
  });

  it.each([
    ['postgres', 'postgresql', 'user'],
    ['mysql', 'mysql', 'user'],
    ['mongodb', 'mongodb', 'userId'],
  ])('accepts the canonical relation index normalization injects when a relation is added on %s', async (database, backend, autoIndex) => {
    const before = {
      id: 208,
      name: 'ai_internal_tool_history',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
      relations: [],
      uniques: [],
      indexes: [],
    };
    const normalized = normalizeRuntimeTableSchema({
      ...before,
      relations: [{ id: 2, propertyName: 'user', type: 'many-to-one', targetTableName: 'enfyra_user', foreignKeyColumn: 'userId' }],
    }, { backend: backend as any, mode: 'persisted' });
    expect(normalized?.contract.indexes).toContainEqual([autoIndex]);
    await expect(compiler(database).compile({
      operation: 'update',
      tableName: before.name,
      beforeMetadata: before,
      afterMetadata: normalized?.contract,
    })).resolves.toBeTruthy();
  });

  it('adds a relation through the aggregate router without a false manual-index rejection', async () => {
    const existing = {
      id: 208,
      name: 'ai_internal_tool_history',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
      relations: [],
      uniques: [],
      indexes: [['createdAt'], ['updatedAt']],
    };
    const compile = vi.fn(async () => ({ contract: { context: { diff: { schemaChanged: true } } }, requiredConfirmHash: 'hash' }));
    const router = new RuntimeMetadataSchemaRouterService({
      queryBuilderService: {
        findOne: vi.fn(async ({ table }: any) => table === 'enfyra_table' ? existing : { id: 4, name: 'enfyra_user' }),
      } as any,
      tableHandlerService: {} as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
      tableManagementValidationService: new TableManagementValidationService(),
      runtimeSchemaContractCompilerService: { compile } as any,
      runtimeSchemaExecutorService: { execute: vi.fn() } as any,
    });
    await expect(router.updateTable({
      tableId: 208,
      body: { relations: [{ propertyName: 'user', type: 'many-to-one', targetTable: 4, isNullable: false }] } as any,
    })).resolves.toBeTruthy();
    expect(compile).toHaveBeenCalledTimes(1);
  });

  it('drops a caller-declared owning relation index through the aggregate router and keeps the automatic one', async () => {
    const existing = {
      id: 208,
      name: 'ai_internal_tool_history',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
      relations: [{ id: 2, propertyName: 'user', type: 'many-to-one', targetTableName: 'enfyra_user', foreignKeyColumn: 'userId' }],
      uniques: [],
      indexes: [],
    };
    const compile = vi.fn(async () => ({ contract: { context: { diff: { schemaChanged: true } } }, requiredConfirmHash: 'hash' }));
    const execute = vi.fn(async () => ({ recordId: 208, affectedTables: [] }));
    const router = new RuntimeMetadataSchemaRouterService({
      queryBuilderService: { findOne: vi.fn(async () => existing) } as any,
      tableHandlerService: {} as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
      tableManagementValidationService: new TableManagementValidationService(),
      runtimeSchemaContractCompilerService: { compile } as any,
      runtimeSchemaExecutorService: { execute } as any,
    });
    await router.updateTable({ tableId: 208, body: { indexes: [['user']] } as any });
    const sent = (compile.mock.calls[0] as any[])[0].afterMetadata;
    expect(sent.indexes.filter((group: string[]) => group.length === 1 && group[0] === 'user')).toHaveLength(1);
  });

  it('drops a unique prefix expressed through the persisted physical FK alias', async () => {
    const { contract } = await compiler('postgres').compile({
      operation: 'update', tableName: schema.name, beforeMetadata: schema,
      afterMetadata: { ...schema, indexes: [...schema.indexes, ['userId']] },
    });
    expect(contract.context.target?.indexes).not.toContainEqual(['userId']);
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
