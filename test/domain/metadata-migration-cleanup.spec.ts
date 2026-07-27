import { describe, expect, it, vi } from 'vitest';
import { MetadataColumnMigrationService } from '../../src/engines/bootstrap/services/metadata-migration/metadata-column-migration.service';
import { MetadataRelationMigrationService } from '../../src/engines/bootstrap/services/metadata-migration/metadata-relation-migration.service';
import { MetadataTableMigrationService } from '../../src/engines/bootstrap/services/metadata-migration/metadata-table-migration.service';
import { MetadataTableRenameService } from '../../src/engines/bootstrap/services/metadata-migration/metadata-table-rename.service';
import { MetadataPhysicalMigrationHelper } from '../../src/engines/bootstrap/utils/metadata-physical-migration.util';

function makeMigrationDeps(
  queryBuilderService: any,
  systemCoreTableResolver: any,
) {
  return {
    queryBuilderService,
    systemCoreTableResolver,
    physicalMigration: new MetadataPhysicalMigrationHelper({
      queryBuilderService,
    }),
    verbose: () => undefined,
  };
}

function makeMigrationService<T>(
  Service: new (deps: ReturnType<typeof makeMigrationDeps>) => T,
  deps: {
    queryBuilderService: any;
    systemCoreTableResolver: any;
  },
): T {
  return new Service(
    makeMigrationDeps(deps.queryBuilderService, deps.systemCoreTableResolver),
  );
}

function matches(row: any, filter: Record<string, any>): boolean {
  return Object.entries(filter).every(([key, value]) => {
    if (value && typeof value === 'object' && '$in' in value) {
      return value.$in.some((item: any) => String(item) === String(row[key]));
    }
    return String(row[key]) === String(value);
  });
}

function makeMongoDb(collections: Record<string, any[]>) {
  const db = {
    listCollections: vi.fn(({ name }: { name: string }) => ({
      toArray: vi.fn(async () => (name in collections ? [{ name }] : [])),
    })),
    collection: vi.fn((name: string) => ({
      find: vi.fn((filter: Record<string, any> = {}) => ({
        toArray: vi.fn(async () =>
          (collections[name] ?? []).filter((row) => matches(row, filter)),
        ),
      })),
      findOne: vi.fn(
        async (filter: Record<string, any>) =>
          (collections[name] ?? []).find((row) => matches(row, filter)) ?? null,
      ),
      deleteOne: vi.fn(async (filter: Record<string, any>) => {
        const before = collections[name]?.length ?? 0;
        collections[name] = (collections[name] ?? []).filter(
          (row) => !matches(row, filter),
        );
        return { deletedCount: before - collections[name].length };
      }),
      deleteMany: vi.fn(async (filter: Record<string, any>) => {
        const before = collections[name]?.length ?? 0;
        collections[name] = (collections[name] ?? []).filter(
          (row) => !matches(row, filter),
        );
        return { deletedCount: before - collections[name].length };
      }),
      updateOne: vi.fn(
        async (filter: Record<string, any>, update: Record<string, any>) => {
          collections[name] = (collections[name] ?? []).map((row) =>
            matches(row, filter) ? { ...row, ...(update.$set ?? update) } : row,
          );
          return { modifiedCount: 1 };
        },
      ),
      updateMany: vi.fn(async () => ({ modifiedCount: 0 })),
    })),
  } as any;
  return { db, collections };
}

function makeSqlKnex(tables: Record<string, any[]>) {
  const knex: any = (tableName: string) => {
    const filters: Array<(row: any) => boolean> = [];
    const query: any = {
      where(field: string | Record<string, any>, value?: any) {
        if (typeof field === 'string') {
          filters.push((row) => String(row[field]) === String(value));
        } else {
          filters.push((row) => matches(row, field));
        }
        return query;
      },
      async first() {
        return (tables[tableName] ?? []).find((row) =>
          filters.every((filter) => filter(row)),
        );
      },
      async select() {
        return (tables[tableName] ?? []).filter((row) =>
          filters.every((filter) => filter(row)),
        );
      },
      async update(payload: Record<string, any>) {
        let updated = 0;
        tables[tableName] = (tables[tableName] ?? []).map((row) => {
          if (!filters.every((filter) => filter(row))) return row;
          updated++;
          return { ...row, ...payload };
        });
        return updated;
      },
      async delete() {
        const before = tables[tableName]?.length ?? 0;
        tables[tableName] = (tables[tableName] ?? []).filter(
          (row) => !filters.every((filter) => filter(row)),
        );
        return before - tables[tableName].length;
      },
    };
    return query;
  };
  knex.schema = {
    hasTable: vi.fn(async (tableName: string) => tableName in tables),
  };
  return knex;
}

describe('MetadataMigrationService destructive cleanup', () => {
  it('reconciles an already-created SQL relation rename target on retry', async () => {
    const tables = {
      enfyra_relation: [
        {
          id: 1,
          sourceTableId: 10,
          targetTableId: 20,
          propertyName: 'preHook',
          type: 'one-to-many',
        },
        {
          id: 2,
          sourceTableId: 10,
          targetTableId: 20,
          propertyName: 'preHooks',
          type: 'one-to-many',
        },
      ],
      enfyra_field_permission: [{ id: 3, relationId: 1 }],
    };
    const knex = makeSqlKnex(tables);
    const service = makeMigrationService(MetadataRelationMigrationService, {
      queryBuilderService: {
        isMongoDb: vi.fn(() => false),
        getKnex: vi.fn(() => knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: vi.fn(async () => 'enfyra_table'),
        getNames: vi.fn(async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        })),
      } as any,
    });

    await service.modifyRelationMetadata(10, false, [
      {
        from: { propertyName: 'preHook' },
        to: { propertyName: 'preHooks' },
      },
    ]);

    expect(tables.enfyra_relation).toEqual([
      {
        id: 2,
        sourceTableId: 10,
        targetTableId: 20,
        propertyName: 'preHooks',
        type: 'one-to-many',
      },
    ]);
    expect(tables.enfyra_field_permission).toEqual([{ id: 3, relationId: 2 }]);
  });

  it('reconciles and removes an overlapping legacy Mongo table metadata row', async () => {
    const mongo = makeMongoDb({
      enfyra_table: [
        { _id: 'legacy-table-id', name: 'post_definition' },
        { _id: 'target-table-id', name: 'enfyra_post' },
      ],
      enfyra_column: [
        {
          _id: 'custom-column-id',
          table: 'legacy-table-id',
          name: 'operatorNote',
        },
      ],
      enfyra_relation: [
        {
          _id: 'custom-relation-id',
          sourceTable: 'legacy-table-id',
          targetTable: 'target-table-id',
          propertyName: 'operatorOwner',
        },
      ],
    });
    const service = makeMigrationService(MetadataTableRenameService, {
      queryBuilderService: {
        isMongoDb: vi.fn(() => true),
        getMongoDb: vi.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getTableName: vi.fn(async () => 'enfyra_table'),
        getNames: vi.fn(async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        })),
      } as any,
    });

    await service.renameMongoTable({
      from: 'post_definition',
      to: 'enfyra_post',
    });

    expect(mongo.collections.enfyra_table).toEqual([
      { _id: 'target-table-id', name: 'enfyra_post' },
    ]);
    expect(mongo.collections.enfyra_column).toEqual([
      {
        _id: 'custom-column-id',
        table: 'target-table-id',
        name: 'operatorNote',
        updatedAt: expect.any(Date),
      },
    ]);
    expect(mongo.collections.enfyra_relation).toEqual([
      {
        _id: 'custom-relation-id',
        sourceTable: 'target-table-id',
        targetTable: 'target-table-id',
        propertyName: 'operatorOwner',
        updatedAt: expect.any(Date),
      },
    ]);
  });

  it('removes the owning relation, mapped inverse, and dependent field permissions on Mongo', async () => {
    const mongo = makeMongoDb({
      enfyra_table: [
        { _id: 'posts-id', name: 'posts' },
        { _id: 'tags-id', name: 'tags' },
      ],
      enfyra_relation: [
        {
          _id: 'owning-id',
          sourceTable: 'posts-id',
          targetTable: 'tags-id',
          propertyName: 'tags',
          type: 'many-to-many',
          mappedBy: null,
        },
        {
          _id: 'inverse-id',
          sourceTable: 'tags-id',
          targetTable: 'posts-id',
          propertyName: 'posts',
          type: 'many-to-many',
          mappedBy: 'owning-id',
        },
      ],
      enfyra_field_permission: [
        { _id: 'permission-1', relation: 'owning-id' },
        { _id: 'permission-2', relation: 'inverse-id' },
      ],
    });
    const service = makeMigrationService(MetadataRelationMigrationService, {
      queryBuilderService: {
        isMongoDb: vi.fn(() => true),
        getMongoDb: vi.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getNames: vi.fn(async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        })),
      } as any,
    });

    await service.removeRelationMetadata('posts-id', true, ['tags']);

    expect(mongo.collections.enfyra_relation).toEqual([]);
    expect(mongo.collections.enfyra_field_permission).toEqual([]);
  });

  it('removes Mongo column rules and field permissions with deleted column metadata', async () => {
    const mongo = makeMongoDb({
      enfyra_table: [{ _id: 'posts-id', name: 'posts' }],
      enfyra_column: [
        {
          _id: 'column-id',
          table: 'posts-id',
          name: 'legacy',
        },
      ],
      enfyra_relation: [],
      enfyra_column_rule: [{ _id: 'rule-id', column: 'column-id' }],
      enfyra_field_permission: [{ _id: 'permission-id', column: 'column-id' }],
      posts: [{ _id: 'post-id', legacy: 'value' }],
    });
    const service = makeMigrationService(MetadataColumnMigrationService, {
      queryBuilderService: {
        isMongoDb: vi.fn(() => true),
        getMongoDb: vi.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getNames: vi.fn(async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        })),
      } as any,
    });

    await service.removeColumnMetadata(
      'posts',
      'posts-id',
      'table',
      ['legacy'],
      true,
    );

    expect(mongo.collections.enfyra_column).toEqual([]);
    expect(mongo.collections.enfyra_column_rule).toEqual([]);
    expect(mongo.collections.enfyra_field_permission).toEqual([]);
  });

  it('removes all Mongo metadata that depends on a dropped table', async () => {
    const mongo = makeMongoDb({
      enfyra_table: [
        { _id: 'authors-id', name: 'authors' },
        { _id: 'posts-id', name: 'posts' },
      ],
      enfyra_column: [{ _id: 'column-id', table: 'authors-id', name: 'name' }],
      enfyra_relation: [
        {
          _id: 'relation-id',
          sourceTable: 'posts-id',
          targetTable: 'authors-id',
          propertyName: 'author',
        },
      ],
      enfyra_column_rule: [{ _id: 'rule-id', column: 'column-id' }],
      enfyra_field_permission: [
        { _id: 'column-permission', column: 'column-id' },
        { _id: 'relation-permission', relation: 'relation-id' },
      ],
      enfyra_route: [{ _id: 'route-id', mainTable: 'authors-id' }],
      enfyra_graphql: [{ _id: 'graphql-id', table: 'authors-id' }],
    });
    const service = makeMigrationService(MetadataTableMigrationService, {
      queryBuilderService: {
        isMongoDb: vi.fn(() => true),
        getMongoDb: vi.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getNames: vi.fn(async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        })),
      } as any,
    });

    await service.dropTableMetadata(['authors'], true);

    expect(mongo.collections.enfyra_table).toEqual([
      { _id: 'posts-id', name: 'posts' },
    ]);
    expect(mongo.collections.enfyra_column).toEqual([]);
    expect(mongo.collections.enfyra_relation).toEqual([]);
    expect(mongo.collections.enfyra_column_rule).toEqual([]);
    expect(mongo.collections.enfyra_field_permission).toEqual([]);
    expect(mongo.collections.enfyra_route).toEqual([]);
    expect(mongo.collections.enfyra_graphql).toEqual([]);
  });

  it('updates both owning and inverse relation metadata on Mongo', async () => {
    const mongo = makeMongoDb({
      enfyra_relation: [
        {
          _id: 'owning-id',
          sourceTable: 'posts-id',
          targetTable: 'tags-id',
          propertyName: 'tags',
          type: 'many-to-many',
          mappedBy: null,
        },
        {
          _id: 'inverse-id',
          sourceTable: 'tags-id',
          targetTable: 'posts-id',
          propertyName: 'posts',
          type: 'many-to-many',
          mappedBy: 'owning-id',
        },
      ],
    });
    const service = makeMigrationService(MetadataRelationMigrationService, {
      queryBuilderService: {
        isMongoDb: vi.fn(() => true),
        getMongoDb: vi.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getNames: vi.fn(async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        })),
      } as any,
    });

    await service.modifyRelationMetadata('posts-id', true, [
      {
        from: {
          propertyName: 'tags',
          inversePropertyName: 'posts',
        },
        to: {
          propertyName: 'labels',
          inversePropertyName: 'labeledPosts',
          type: 'many-to-many',
        },
      },
    ]);

    expect(mongo.collections.enfyra_relation).toEqual([
      expect.objectContaining({
        _id: 'owning-id',
        propertyName: 'labels',
        type: 'many-to-many',
      }),
      expect.objectContaining({
        _id: 'inverse-id',
        propertyName: 'labeledPosts',
      }),
    ]);
  });
});
