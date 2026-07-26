import { describe, expect, it, vi } from 'vitest';
import { MetadataMigrationService } from '../../src/engines/bootstrap/services/metadata-migration.service';

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

describe('MetadataMigrationService destructive cleanup', () => {
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
    const service = new MetadataMigrationService({
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

    await (service as any).removeRelationMetadata('posts-id', true, ['tags']);

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
    const service = new MetadataMigrationService({
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

    await (service as any).removeColumnMetadata(
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
    const service = new MetadataMigrationService({
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

    await (service as any).dropTableMetadata(['authors'], true);

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
    const service = new MetadataMigrationService({
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

    await (service as any).modifyRelationMetadata('posts-id', true, [
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
