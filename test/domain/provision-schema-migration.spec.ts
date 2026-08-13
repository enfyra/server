import { describe, expect, it, vi } from 'vitest';
import {
  applyMongoSchemaMigrations,
  applySqlSchemaMigrations,
} from '../../src/shared/utils/provision-schema-migration';

function makeMongoDb(input: {
  collections: Record<string, any[]>;
  indexes?: Record<string, any[]>;
  updateError?: Error;
}) {
  const collections = new Map(
    Object.entries(input.collections).map(([name, rows]) => [name, [...rows]]),
  );
  const indexes = new Map(
    Object.entries(input.indexes ?? {}).map(([name, rows]) => [
      name,
      [...rows],
    ]),
  );
  const updateMany = vi.fn(async () => ({ modifiedCount: 0 }));
  const dropIndex = vi.fn(async () => undefined);
  const createIndex = vi.fn(async () => undefined);
  const dropCollection = vi.fn(async (name: string) => {
    collections.delete(name);
    return true;
  });
  const matchesFilter = (
    row: Record<string, any>,
    filter: Record<string, any>,
  ) =>
    Object.entries(filter).every(([key, value]: [string, any]) => {
      if (key === '$expr' && value?.$ne) {
        const [left, right] = value.$ne.map((item: string) =>
          item.startsWith('$') ? row[item.slice(1)] : item,
        );
        return left !== right;
      }
      if (value?.$exists !== undefined) {
        return value.$exists ? key in row : !(key in row);
      }
      return String(row[key]) === String(value);
    });

  const db = {
    listCollections: vi.fn(({ name }: { name: string }) => ({
      toArray: vi.fn(async () => (collections.has(name) ? [{ name }] : [])),
    })),
    dropCollection,
    collection: vi.fn((name: string) => ({
      find: vi.fn((filter: Record<string, any> = {}) => ({
        toArray: vi.fn(async () =>
          (collections.get(name) ?? []).filter((row) =>
            matchesFilter(row, filter),
          ),
        ),
      })),
      findOne: vi.fn(
        async (filter: Record<string, any> = {}) =>
          (collections.get(name) ?? []).find((row) =>
            matchesFilter(row, filter),
          ) ?? null,
      ),
      countDocuments: vi.fn(
        async (filter: Record<string, any>) =>
          (collections.get(name) ?? []).filter((row) =>
            matchesFilter(row, filter),
          ).length,
      ),
      updateMany: async (
        filter: Record<string, any>,
        update: Record<string, any> | Record<string, any>[],
      ) => {
        if (input.updateError) throw input.updateError;
        await updateMany(filter, update);
        let modifiedCount = 0;
        collections.set(
          name,
          (collections.get(name) ?? []).map((row) => {
            if (!matchesFilter(row, filter)) return row;
            const next = { ...row };
            const pipelineSet = Array.isArray(update)
              ? (update[0]?.$set ?? {})
              : {};
            for (const [field, value] of Object.entries(pipelineSet)) {
              next[field] =
                typeof value === 'string' && value.startsWith('$')
                  ? row[value.slice(1)]
                  : value;
            }
            const directUpdate = Array.isArray(update) ? {} : update;
            for (const field of Object.keys(directUpdate.$unset ?? {})) {
              delete next[field];
            }
            for (const [oldField, newField] of Object.entries(
              directUpdate.$rename ?? {},
            )) {
              next[String(newField)] = next[oldField];
              delete next[oldField];
            }
            modifiedCount++;
            return next;
          }),
        );
        return { modifiedCount };
      },
      updateOne: vi.fn(async () => ({ modifiedCount: 0 })),
      listIndexes: vi.fn(() => ({
        toArray: vi.fn(async () => indexes.get(name) ?? [{ name: '_id_' }]),
      })),
      dropIndex: async (indexName: string) => {
        await dropIndex(indexName);
        indexes.set(
          name,
          (indexes.get(name) ?? []).filter((index) => index.name !== indexName),
        );
      },
      createIndex: async (
        key: Record<string, any>,
        options: Record<string, any>,
      ) => {
        await createIndex(key, options);
        indexes.set(name, [
          ...(indexes.get(name) ?? []),
          { name: options.name, key, ...options },
        ]);
        return options.name;
      },
    })),
  } as any;

  return {
    db,
    collections,
    indexes,
    dropCollection,
    dropIndex,
    createIndex,
    updateMany,
  };
}

function makeSqlKnex(input: {
  tables: Record<string, any[]>;
  columns: Record<string, string[]>;
}) {
  const physicalTables = new Set([
    ...Object.keys(input.columns),
    ...Object.keys(input.tables),
  ]);
  const droppedColumns: Array<{ table: string; column: string }> = [];
  const droppedTables: string[] = [];
  const raw = vi.fn(async (sql: string) => {
    if (sql.includes('SELECT') || sql.includes('pg_constraint')) {
      return { rows: [] };
    }
    return { rows: [] };
  });
  const knex = vi.fn((name: string) => ({
    select: vi.fn(async () => input.tables[name] ?? []),
    where: vi.fn((condition: Record<string, any>) => ({
      first: vi.fn(
        async () =>
          (input.tables[name] ?? []).find((row) =>
            Object.entries(condition).every(
              ([key, value]) => String(row[key]) === String(value),
            ),
          ) ?? null,
      ),
    })),
  })) as any;
  knex.client = { config: { client: 'pg' } };
  knex.raw = raw;
  knex.transaction = vi.fn(async (callback: any) => callback({ raw }));
  knex.schema = {
    hasTable: vi.fn(async (name: string) => physicalTables.has(name)),
    hasColumn: vi.fn(async (table: string, column: string) =>
      (input.columns[table] ?? []).includes(column),
    ),
    alterTable: vi.fn(async (table: string, callback: any) => {
      callback({
        dropColumn: (column: string) => {
          droppedColumns.push({ table, column });
          input.columns[table] = (input.columns[table] ?? []).filter(
            (item) => item !== column,
          );
        },
      });
    }),
    dropTableIfExists: vi.fn(async (name: string) => {
      droppedTables.push(name);
      physicalTables.delete(name);
    }),
  };

  return { knex, droppedColumns, droppedTables };
}

describe('provision schema migration physical cleanup', () => {
  it('migrates legacy user roles through a hook-decorated Knex builder', async () => {
    const raw = vi.fn(async () => ({ rows: [] }));
    const decoratedInsert = vi.fn(async () => []);
    const knex = vi.fn((tableName: string) => {
      if (tableName === 'enfyra_user') {
        return {
          select: vi.fn(() => ({
            whereNotNull: vi.fn(async () => [
              { id: 'user-1', roleId: 'role-1' },
            ]),
          })),
        };
      }
      if (tableName === 'enfyra_user_roles') {
        return { insert: decoratedInsert };
      }
      return { select: vi.fn(async () => []) };
    }) as any;
    knex.client = { config: { client: 'pg' } };
    knex.raw = raw;
    knex.schema = {
      hasTable: vi.fn(async (tableName: string) =>
        [
          'enfyra_table',
          'enfyra_relation',
          'enfyra_user',
          'enfyra_user_roles',
        ].includes(tableName),
      ),
      hasColumn: vi.fn(
        async (tableName: string, columnName: string) =>
          tableName === 'enfyra_user' && columnName === 'roleId',
      ),
      alterTable: vi.fn(async (_tableName: string, callback: any) => {
        callback({ dropColumn: vi.fn() });
      }),
    };

    await applySqlSchemaMigrations(knex, {
      tables: [
        {
          _unique: { name: { _eq: 'enfyra_user' } },
          relationsToRemove: ['role'],
        },
      ],
    });

    expect(decoratedInsert).not.toHaveBeenCalled();
    expect(raw).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO "enfyra_user_roles"'),
      ['user-1', 'role-1'],
    );
    expect(raw).toHaveBeenCalledWith(
      expect.stringContaining('ON CONFLICT ("userId", "roleId") DO NOTHING'),
      ['user-1', 'role-1'],
    );
  });

  it('removes incoming SQL foreign-key columns and junction tables before dropping a target table', async () => {
    const sql = makeSqlKnex({
      tables: {
        enfyra_table: [
          { id: 1, name: 'authors' },
          { id: 2, name: 'posts' },
          { id: 3, name: 'tags' },
        ],
        enfyra_relation: [
          {
            id: 10,
            sourceTableId: 2,
            targetTableId: 1,
            propertyName: 'author',
            foreignKeyColumn: 'author_id',
            type: 'many-to-one',
            mappedById: null,
          },
          {
            id: 11,
            sourceTableId: 1,
            targetTableId: 3,
            propertyName: 'tags',
            type: 'many-to-many',
            mappedById: null,
            junctionTableName: 'j_authors_tags',
          },
        ],
      },
      columns: {
        authors: ['id'],
        posts: ['id', 'author_id'],
        tags: ['id'],
        j_authors_tags: ['sourceId', 'targetId'],
      },
    });

    await applySqlSchemaMigrations(sql.knex, {
      tables: [],
      tablesToDrop: ['authors'],
    });

    expect(sql.droppedColumns).toContainEqual({
      table: 'posts',
      column: 'author_id',
    });
    expect(sql.droppedTables).toContain('j_authors_tags');
  });

  it('drops the persisted Mongo junction collection when an owning many-to-many relation is removed', async () => {
    const mongo = makeMongoDb({
      collections: {
        enfyra_table: [
          { _id: 'posts-id', name: 'posts' },
          { _id: 'tags-id', name: 'tags' },
        ],
        enfyra_relation: [
          {
            _id: 'relation-id',
            sourceTable: 'posts-id',
            targetTable: 'tags-id',
            propertyName: 'tags',
            type: 'many-to-many',
            mappedBy: null,
            junctionTableName: 'j_posts_tags',
          },
        ],
        posts: [],
        tags: [],
        j_posts_tags: [{ sourceId: 'post-1', targetId: 'tag-1' }],
      },
    });

    await applyMongoSchemaMigrations(mongo.db, {
      tables: [
        {
          _unique: { name: { _eq: 'posts' } },
          relationsToRemove: ['tags'],
        },
      ],
    });

    expect(mongo.dropCollection).toHaveBeenCalledWith('j_posts_tags');
  });

  it('preserves a removed Mongo column field when the current snapshot reuses it as a relation', async () => {
    const mongo = makeMongoDb({
      collections: {
        enfyra_relation: [
          {
            _id: 'inverse-id',
            propertyName: 'children',
            mappedBy: 'owning-id',
          },
        ],
      },
    });

    await applyMongoSchemaMigrations(
      mongo.db,
      {
        tables: [
          {
            _unique: { name: { _eq: 'enfyra_relation' } },
            columnsToRemove: ['mappedBy'],
          },
        ],
      },
      {
        preserveFieldsByCollection: {
          enfyra_relation: ['mappedBy'],
        },
      },
    );

    expect(mongo.collections.get('enfyra_relation')).toEqual([
      {
        _id: 'inverse-id',
        propertyName: 'children',
        mappedBy: 'owning-id',
      },
    ]);
    expect(mongo.updateMany).not.toHaveBeenCalled();
  });

  it('removes incoming Mongo relation fields and indexes before dropping a target collection', async () => {
    const mongo = makeMongoDb({
      collections: {
        enfyra_table: [
          { _id: 'authors-id', name: 'authors' },
          { _id: 'posts-id', name: 'posts' },
        ],
        enfyra_relation: [
          {
            _id: 'relation-id',
            sourceTable: 'posts-id',
            targetTable: 'authors-id',
            propertyName: 'author',
            foreignKeyColumn: 'author',
            type: 'many-to-one',
            mappedBy: null,
          },
        ],
        authors: [{ _id: 'author-1' }],
        posts: [{ _id: 'post-1', author: 'author-1' }],
      },
      indexes: {
        posts: [
          { name: '_id_', key: { _id: 1 } },
          { name: 'idx_posts_author', key: { author: 1, _id: 1 } },
        ],
      },
    });

    await applyMongoSchemaMigrations(mongo.db, {
      tables: [],
      tablesToDrop: ['authors'],
    });

    expect(mongo.updateMany).toHaveBeenCalledWith(
      { author: { $exists: true } },
      { $unset: { author: '' } },
    );
    expect(mongo.dropIndex).toHaveBeenCalledWith('idx_posts_author');
    expect(mongo.dropCollection).toHaveBeenCalledWith('authors');
  });

  it('renames Mongo field data and every compound index contract idempotently', async () => {
    const mongo = makeMongoDb({
      collections: {
        posts: [
          {
            _id: 'post-1',
            legacyTitle: 'preserved',
            status: 'draft',
          },
        ],
      },
      indexes: {
        posts: [
          { name: '_id_', key: { _id: 1 } },
          {
            name: 'idx_legacyTitle_status',
            key: { legacyTitle: 1, status: -1 },
            unique: true,
            partialFilterExpression: {
              legacyTitle: { $exists: true },
            },
          },
        ],
      },
    });
    const migration = {
      tables: [
        {
          _unique: { name: { _eq: 'posts' } },
          columnsToModify: [
            {
              from: { name: 'legacyTitle' },
              to: { name: 'title' },
            },
          ],
        },
      ],
    };

    await applyMongoSchemaMigrations(mongo.db, migration);
    await applyMongoSchemaMigrations(mongo.db, migration);

    expect(mongo.collections.get('posts')).toEqual([
      { _id: 'post-1', title: 'preserved', status: 'draft' },
    ]);
    expect(mongo.dropIndex).toHaveBeenCalledTimes(1);
    expect(mongo.createIndex).toHaveBeenCalledWith(
      { title: 1, status: -1 },
      expect.objectContaining({
        name: 'idx_title_status',
        unique: true,
        partialFilterExpression: { title: { $exists: true } },
      }),
    );
    expect(mongo.indexes.get('posts')).toEqual([
      { name: '_id_', key: { _id: 1 } },
      expect.objectContaining({
        name: 'idx_title_status',
        key: { title: 1, status: -1 },
      }),
    ]);
  });

  it('removes Mongo column data and all indexes containing that field', async () => {
    const mongo = makeMongoDb({
      collections: {
        posts: [{ _id: 'post-1', obsolete: 'remove', status: 'draft' }],
      },
      indexes: {
        posts: [
          { name: '_id_', key: { _id: 1 } },
          {
            name: 'idx_obsolete_status',
            key: { obsolete: 1, status: 1 },
          },
        ],
      },
    });

    await applyMongoSchemaMigrations(mongo.db, {
      tables: [
        {
          _unique: { name: { _eq: 'posts' } },
          columnsToRemove: ['obsolete'],
        },
      ],
    });

    expect(mongo.collections.get('posts')).toEqual([
      { _id: 'post-1', status: 'draft' },
    ]);
    expect(mongo.dropIndex).toHaveBeenCalledWith('idx_obsolete_status');
    expect(mongo.indexes.get('posts')).toEqual([
      { name: '_id_', key: { _id: 1 } },
    ]);
  });

  it('propagates Mongo physical write failures without touching indexes', async () => {
    const mongo = makeMongoDb({
      collections: {
        posts: [{ _id: 'post-1', legacyTitle: 'preserved' }],
      },
      indexes: {
        posts: [
          { name: '_id_', key: { _id: 1 } },
          { name: 'idx_legacyTitle', key: { legacyTitle: 1 } },
        ],
      },
      updateError: new Error('forced Mongo write failure'),
    });

    await expect(
      applyMongoSchemaMigrations(mongo.db, {
        tables: [
          {
            _unique: { name: { _eq: 'posts' } },
            columnsToModify: [
              {
                from: { name: 'legacyTitle' },
                to: { name: 'title' },
              },
            ],
          },
        ],
      }),
    ).rejects.toThrow('forced Mongo write failure');
    expect(mongo.dropIndex).not.toHaveBeenCalled();
  });
});
