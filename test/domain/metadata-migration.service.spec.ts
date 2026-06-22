import { MetadataMigrationService } from '../../src/engines/bootstrap/services/metadata-migration.service';

function makeSqlKnex({
  tables,
  schemas = {},
}: {
  tables: Record<string, any[]>;
  schemas?: Record<string, string[]>;
}) {
  const inserts: Array<{ table: string; rows: any[] }> = [];
  const updates: Array<{ table: string; where: any; data: any }> = [];
  const deletes: Array<{ table: string; where: any }> = [];

  const matchRow = (row: any, condition: any) =>
    Object.entries(condition).every(([key, value]) => row[key] === value);

  const knex = jest.fn((table: string) => ({
    select: jest.fn(async () => tables[table] ?? []),
    columnInfo: jest.fn(async () => {
      const rows = tables[table] ?? [];
      const columns = new Set<string>(schemas[table] ?? []);
      rows.forEach((row) =>
        Object.keys(row).forEach((column) => columns.add(column)),
      );
      return Object.fromEntries(
        [...columns].map((column) => [column, { type: 'varchar' }]),
      );
    }),
    insert: jest.fn(async (input: any[] | any) => {
      const rows = Array.isArray(input) ? input : [input];
      const normalizedRows = rows.map((row) => {
        if (row.id !== undefined && row.id !== null) return row;
        const maxId = Math.max(0, ...(tables[table] ?? []).map((r) => r.id));
        return { ...row, id: maxId + 1 };
      });
      inserts.push({ table, rows: normalizedRows });
      tables[table] = [...(tables[table] ?? []), ...normalizedRows];
      return normalizedRows.length;
    }),
    where: jest.fn((condition: any) => ({
      update: jest.fn(async (data: any) => {
        updates.push({ table, where: condition, data });
        tables[table] = (tables[table] ?? []).map((row) =>
          matchRow(row, condition) ? { ...row, ...data } : row,
        );
        return 1;
      }),
      andWhere: jest.fn((extraCondition: any) => ({
        update: jest.fn(async (data: any) => {
          updates.push({
            table,
            where: { ...condition, ...extraCondition },
            data,
          });
          return 1;
        }),
      })),
      whereNot: jest.fn((negativeCondition: any) => ({
        delete: jest.fn(async () => {
          deletes.push({
            table,
            where: { ...condition, not: negativeCondition },
          });
          tables[table] = (tables[table] ?? []).filter(
            (row) =>
              !matchRow(row, condition) || matchRow(row, negativeCondition),
          );
          return 1;
        }),
      })),
      first: jest.fn(
        async () =>
          (tables[table] ?? []).find((row) => matchRow(row, condition)) ?? null,
      ),
    })),
  })) as any;

  knex.schema = {
    hasTable: jest.fn(async (table: string) => table in tables),
    hasColumn: jest.fn(async (table: string, column: string) =>
      (schemas[table] ?? Object.keys(tables[table]?.[0] ?? {})).includes(
        column,
      ),
    ),
    alterTable: jest.fn(async (tableName: string, callback: any) => {
      const builder = {
        specificType: jest.fn((column: string) => {
          schemas[tableName] = [...(schemas[tableName] ?? []), column];
        }),
        text: jest.fn((column: string) => {
          schemas[tableName] = [...(schemas[tableName] ?? []), column];
        }),
        dropColumn: jest.fn((column: string) => {
          schemas[tableName] = (schemas[tableName] ?? []).filter(
            (name) => name !== column,
          );
          tables[tableName] = (tables[tableName] ?? []).map((row) => {
            const next = { ...row };
            delete next[column];
            return next;
          });
        }),
      };
      callback(builder);
    }),
  };

  return { knex, inserts, updates, deletes, tables };
}

function makeMongoDb({ collections }: { collections: Record<string, any[]> }) {
  const inserts: Array<{ collection: string; rows: any[] }> = [];
  const updates: Array<{ collection: string; filter: any; data: any }> = [];

  const matchRow = (row: any, condition: any) =>
    Object.entries(condition).every(([key, value]) => row[key] === value);

  const db = {
    collection: jest.fn((name: string) => ({
      find: jest.fn((filter = {}) => ({
        toArray: jest.fn(async () =>
          (collections[name] ?? []).filter((row) => matchRow(row, filter)),
        ),
      })),
      findOne: jest.fn(
        async (filter = {}) =>
          (collections[name] ?? []).find((row) => matchRow(row, filter)) ??
          null,
      ),
      insertMany: jest.fn(async (rows: any[]) => {
        const normalizedRows = rows.map((row, index) =>
          row._id !== undefined && row._id !== null
            ? row
            : { ...row, _id: `${name}-generated-${index + 1}` },
        );
        inserts.push({ collection: name, rows: normalizedRows });
        collections[name] = [...(collections[name] ?? []), ...normalizedRows];
        return { insertedCount: normalizedRows.length };
      }),
      updateOne: jest.fn(async (filter: any, data: any) => {
        updates.push({ collection: name, filter, data });
        collections[name] = (collections[name] ?? []).map((row) =>
          matchRow(row, filter) ? { ...row, ...(data.$set ?? data) } : row,
        );
        return { modifiedCount: 1 };
      }),
      updateMany: jest.fn(async (filter: any, data: any) => {
        updates.push({ collection: name, filter, data });
        collections[name] = (collections[name] ?? []).map((row) =>
          matchRow(row, filter) ? { ...row, ...(data.$set ?? data) } : row,
        );
        return { modifiedCount: 1 };
      }),
      rename: jest.fn(async (target: string) => {
        collections[target] = collections[name] ?? [];
        delete collections[name];
      }),
    })),
    listCollections: jest.fn(({ name }: { name: string }) => ({
      toArray: jest.fn(async () => (name in collections ? [{ name }] : [])),
    })),
  } as any;

  return { db, collections, inserts, updates };
}

describe('MetadataMigrationService core table overlap', () => {
  it('does not keep legacy core table names when canonical SQL metadata already exists', async () => {
    const sql = makeSqlKnex({
      tables: {
        table_definition: [
          { id: 1, name: 'table_definition' },
          { id: 4, name: 'route_definition' },
        ],
        enfyra_table: [
          { id: 2, name: 'enfyra_table' },
          { id: 3, name: 'route_definition' },
        ],
        route_definition: [
          { id: 10, path: '/table_definition', mainTableId: 1 },
        ],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runSqlCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
    ]);

    expect(sql.inserts).toEqual([]);
    expect(sql.tables.enfyra_table).toEqual(
      expect.arrayContaining([
        { id: 2, name: 'enfyra_table' },
        { id: 3, name: 'route_definition' },
      ]),
    );
    expect(sql.deletes).toEqual([]);
  });

  it('copies missing non-core legacy rows into canonical SQL metadata without keeping legacy core names', async () => {
    const sql = makeSqlKnex({
      tables: {
        table_definition: [
          { id: 1, name: 'table_definition' },
          { id: 20, name: 'custom_post' },
        ],
        enfyra_table: [{ id: 2, name: 'enfyra_table' }],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runSqlCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
    ]);

    expect(sql.inserts).toContainEqual({
      table: 'enfyra_table',
      rows: [{ id: 20, name: 'custom_post' }],
    });
    expect(sql.tables.enfyra_table).not.toEqual(
      expect.arrayContaining([{ id: 1, name: 'table_definition' }]),
    );
  });

  it('remaps SQL child metadata when a legacy table id conflicts with an existing canonical row', async () => {
    const sql = makeSqlKnex({
      tables: {
        table_definition: [
          { id: 1, name: 'table_definition' },
          { id: 20, name: 'custom_post' },
        ],
        enfyra_table: [{ id: 20, name: 'enfyra_table' }],
        column_definition: [{ id: 100, tableId: 20, name: 'title' }],
        enfyra_column: [{ id: 999, tableId: 20, name: 'id' }],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runSqlCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
      { from: 'column_definition', to: 'enfyra_column' },
    ]);

    const insertedTable = sql.tables.enfyra_table.find(
      (row) => row.name === 'custom_post',
    );
    expect(insertedTable).toMatchObject({ name: 'custom_post' });
    expect(insertedTable.id).not.toBe(20);
    expect(sql.inserts).toContainEqual({
      table: 'enfyra_column',
      rows: [{ id: 100, tableId: insertedTable.id, name: 'title' }],
    });
  });

  it('remaps SQL relation metadata through conflicting legacy source and target table ids', async () => {
    const sql = makeSqlKnex({
      tables: {
        table_definition: [
          { id: 30, name: 'author' },
          { id: 31, name: 'post' },
        ],
        enfyra_table: [
          { id: 30, name: 'enfyra_table' },
          { id: 31, name: 'enfyra_column' },
        ],
        relation_definition: [
          {
            id: 200,
            sourceTableId: 31,
            targetTableId: 30,
            propertyName: 'author',
          },
        ],
        enfyra_relation: [
          {
            id: 999,
            sourceTableId: 900,
            targetTableId: 901,
            propertyName: 'existing',
          },
        ],
      },
      schemas: {
        relation_definition: [
          'id',
          'sourceTableId',
          'targetTableId',
          'propertyName',
        ],
        enfyra_relation: [
          'id',
          'sourceTableId',
          'targetTableId',
          'propertyName',
        ],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runSqlCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
      { from: 'relation_definition', to: 'enfyra_relation' },
    ]);

    const author = sql.tables.enfyra_table.find((row) => row.name === 'author');
    const post = sql.tables.enfyra_table.find((row) => row.name === 'post');
    expect(author.id).not.toBe(30);
    expect(post.id).not.toBe(31);
    expect(sql.inserts).toContainEqual({
      table: 'enfyra_relation',
      rows: [
        {
          id: 200,
          sourceTableId: post.id,
          targetTableId: author.id,
          propertyName: 'author',
        },
      ],
    });
  });

  it('keeps SQL core overlap healing idempotent across repeated runs', async () => {
    const tables = {
      table_definition: [
        { id: 40, name: 'table_definition' },
        { id: 41, name: 'post' },
        { id: 42, name: 'comment' },
      ],
      enfyra_table: [{ id: 40, name: 'enfyra_table' }],
      column_definition: [
        { id: 300, tableId: 41, name: 'title' },
        { id: 301, tableId: 42, name: 'body' },
      ],
      enfyra_column: [{ id: 302, tableId: 40, name: 'id' }],
      relation_definition: [
        {
          id: 400,
          sourceTableId: 41,
          targetTableId: 42,
          propertyName: 'comments',
        },
      ],
      enfyra_relation: [
        {
          id: 401,
          sourceTableId: 40,
          targetTableId: 40,
          propertyName: 'self',
        },
      ],
    };
    const sql = makeSqlKnex({
      tables,
      schemas: {
        table_definition: ['id', 'name'],
        enfyra_table: ['id', 'name'],
        column_definition: ['id', 'tableId', 'name'],
        enfyra_column: ['id', 'tableId', 'name'],
        relation_definition: [
          'id',
          'sourceTableId',
          'targetTableId',
          'propertyName',
        ],
        enfyra_relation: [
          'id',
          'sourceTableId',
          'targetTableId',
          'propertyName',
        ],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    const renames = [
      { from: 'table_definition', to: 'enfyra_table' },
      { from: 'column_definition', to: 'enfyra_column' },
      { from: 'relation_definition', to: 'enfyra_relation' },
    ];

    await (service as any).runSqlCoreTableRenames(renames);
    const afterFirstRun = {
      tables: [...sql.tables.enfyra_table],
      columns: [...sql.tables.enfyra_column],
      relations: [...sql.tables.enfyra_relation],
      insertCount: sql.inserts.length,
    };

    await (service as any).runSqlCoreTableRenames(renames);

    expect(sql.tables.enfyra_table).toEqual(afterFirstRun.tables);
    expect(sql.tables.enfyra_column).toEqual(afterFirstRun.columns);
    expect(sql.tables.enfyra_relation).toEqual(afterFirstRun.relations);
    expect(sql.inserts).toHaveLength(afterFirstRun.insertCount);
    expect(
      sql.tables.enfyra_table.filter((row) => row.name === 'post'),
    ).toHaveLength(1);
    expect(
      sql.tables.enfyra_relation.filter(
        (row) => row.propertyName === 'comments',
      ),
    ).toHaveLength(1);
  });

  it('does not duplicate SQL relation metadata when remapped logical relation already exists', async () => {
    const sql = makeSqlKnex({
      tables: {
        table_definition: [{ id: 10, name: 'post' }],
        enfyra_table: [{ id: 99, name: 'post' }],
        relation_definition: [
          { id: 100, sourceTableId: 10, propertyName: 'comments' },
        ],
        enfyra_relation: [
          { id: 101, sourceTableId: 99, propertyName: 'comments' },
        ],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runSqlCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
      { from: 'relation_definition', to: 'enfyra_relation' },
    ]);

    expect(sql.inserts).toEqual([]);
    expect(sql.tables.enfyra_relation).toEqual([
      { id: 101, sourceTableId: 99, propertyName: 'comments' },
    ]);
  });

  it('does not duplicate canonical SQL core rows when logical names already match', async () => {
    const sql = makeSqlKnex({
      tables: {
        table_definition: [{ id: 1, name: 'enfyra_table' }],
        enfyra_table: [{ id: 2, name: 'enfyra_table' }],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runSqlCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
    ]);

    expect(sql.inserts).toEqual([]);
    expect(sql.tables.enfyra_table).toEqual([{ id: 2, name: 'enfyra_table' }]);
    expect(sql.deletes).toEqual([]);
  });

  it('copies SQL non-core overlap rows and preserves custom columns added by users', async () => {
    const sql = makeSqlKnex({
      tables: {
        user_definition: [
          {
            id: 1,
            email: 'old@example.com',
            displayName: 'Old User',
            favoriteColor: 'green',
          },
        ],
        enfyra_user: [],
        enfyra_table: [],
      },
      schemas: {
        user_definition: ['id', 'email', 'displayName', 'favoriteColor'],
        enfyra_user: ['id', 'email', 'displayName'],
        enfyra_table: ['id', 'name'],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).renameSqlTable({
      from: 'user_definition',
      to: 'enfyra_user',
      mergeKeys: ['email'],
    });

    expect(sql.tables.enfyra_user).toEqual([
      {
        id: 1,
        email: 'old@example.com',
        displayName: 'Old User',
        favoriteColor: 'green',
      },
    ]);
  });

  it('keeps canonical SQL non-core overlap rows when legacy data conflicts', async () => {
    const sql = makeSqlKnex({
      tables: {
        user_definition: [
          { id: 1, email: 'same@example.com', displayName: 'Legacy' },
        ],
        enfyra_user: [
          { id: 1, email: 'same@example.com', displayName: 'Canonical' },
        ],
        enfyra_table: [],
      },
      schemas: {
        user_definition: ['id', 'email', 'displayName'],
        enfyra_user: ['id', 'email', 'displayName'],
        enfyra_table: ['id', 'name'],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).renameSqlTable({
      from: 'user_definition',
      to: 'enfyra_user',
      mergeKeys: ['email'],
    });

    expect(sql.tables.enfyra_user).toEqual([
      { id: 1, email: 'same@example.com', displayName: 'Canonical' },
    ]);
    expect(sql.inserts).toEqual([]);
  });

  it('backfills missing custom values into existing SQL non-core canonical rows', async () => {
    const sql = makeSqlKnex({
      tables: {
        user_definition: [
          {
            id: 1,
            email: 'same@example.com',
            displayName: 'Canonical',
            favoriteColor: 'green',
          },
        ],
        enfyra_user: [
          { id: 1, email: 'same@example.com', displayName: 'Canonical' },
        ],
        enfyra_table: [],
      },
      schemas: {
        user_definition: ['id', 'email', 'displayName', 'favoriteColor'],
        enfyra_user: ['id', 'email', 'displayName'],
        enfyra_table: ['id', 'name'],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => false),
        getKnex: jest.fn(() => sql.knex),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).renameSqlTable({
      from: 'user_definition',
      to: 'enfyra_user',
      mergeKeys: ['email'],
    });

    expect(sql.tables.enfyra_user).toEqual([
      {
        id: 1,
        email: 'same@example.com',
        displayName: 'Canonical',
        favoriteColor: 'green',
      },
    ]);
    expect(sql.inserts).toEqual([]);
  });

  it('normalizes legacy core table names when reconciling Mongo core overlap', async () => {
    const mongo = makeMongoDb({
      collections: {
        table_definition: [
          { _id: 'legacy-table', name: 'table_definition' },
          { _id: 'custom-post', name: 'custom_post' },
        ],
        enfyra_table: [{ _id: 'canonical-table', name: 'enfyra_table' }],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => true),
        getMongoDb: jest.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runMongoCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
    ]);

    expect(mongo.inserts).toContainEqual({
      collection: 'enfyra_table',
      rows: [{ _id: 'custom-post', name: 'custom_post' }],
    });
    expect(mongo.collections.enfyra_table).not.toEqual(
      expect.arrayContaining([
        { _id: 'legacy-table', name: 'table_definition' },
      ]),
    );
  });

  it('remaps Mongo child metadata when a legacy table id conflicts with an existing canonical document', async () => {
    const mongo = makeMongoDb({
      collections: {
        table_definition: [
          { _id: 'legacy-core', name: 'table_definition' },
          { _id: 'conflicting-id', name: 'custom_post' },
        ],
        enfyra_table: [{ _id: 'conflicting-id', name: 'enfyra_table' }],
        column_definition: [
          { _id: 'legacy-column', table: 'conflicting-id', name: 'title' },
        ],
        enfyra_column: [],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => true),
        getMongoDb: jest.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runMongoCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
      { from: 'column_definition', to: 'enfyra_column' },
    ]);

    const insertedTable = mongo.collections.enfyra_table.find(
      (row) => row.name === 'custom_post',
    );
    expect(insertedTable).toMatchObject({ name: 'custom_post' });
    expect(insertedTable._id).not.toBe('conflicting-id');
    expect(mongo.inserts).toContainEqual({
      collection: 'enfyra_column',
      rows: [{ _id: 'legacy-column', table: insertedTable._id, name: 'title' }],
    });
  });

  it('backfills missing custom values into existing Mongo non-core canonical documents', async () => {
    const mongo = makeMongoDb({
      collections: {
        user_definition: [
          {
            _id: 'user-1',
            email: 'same@example.com',
            displayName: 'Canonical',
            favoriteColor: 'green',
          },
        ],
        enfyra_user: [
          {
            _id: 'user-1',
            email: 'same@example.com',
            displayName: 'Canonical',
          },
        ],
        enfyra_table: [],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => true),
        getMongoDb: jest.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).renameMongoTable({
      from: 'user_definition',
      to: 'enfyra_user',
      mergeKeys: ['email'],
    });

    expect(mongo.collections.enfyra_user).toEqual([
      {
        _id: 'user-1',
        email: 'same@example.com',
        displayName: 'Canonical',
        favoriteColor: 'green',
      },
    ]);
    expect(mongo.inserts).toEqual([]);
  });

  it('remaps Mongo relation metadata through conflicting legacy source and target table ids', async () => {
    const mongo = makeMongoDb({
      collections: {
        table_definition: [
          { _id: 'author-id', name: 'author' },
          { _id: 'post-id', name: 'post' },
        ],
        enfyra_table: [
          { _id: 'author-id', name: 'enfyra_table' },
          { _id: 'post-id', name: 'enfyra_column' },
        ],
        relation_definition: [
          {
            _id: 'legacy-relation',
            sourceTable: 'post-id',
            targetTable: 'author-id',
            propertyName: 'author',
          },
        ],
        enfyra_relation: [],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => true),
        getMongoDb: jest.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runMongoCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
      { from: 'relation_definition', to: 'enfyra_relation' },
    ]);

    const author = mongo.collections.enfyra_table.find(
      (row) => row.name === 'author',
    );
    const post = mongo.collections.enfyra_table.find(
      (row) => row.name === 'post',
    );
    expect(author._id).not.toBe('author-id');
    expect(post._id).not.toBe('post-id');
    expect(mongo.inserts).toContainEqual({
      collection: 'enfyra_relation',
      rows: [
        {
          _id: 'legacy-relation',
          sourceTable: post._id,
          targetTable: author._id,
          propertyName: 'author',
        },
      ],
    });
  });

  it('does not duplicate Mongo relation metadata when remapped logical relation already exists', async () => {
    const mongo = makeMongoDb({
      collections: {
        table_definition: [{ _id: 'legacy-post', name: 'post' }],
        enfyra_table: [{ _id: 'canonical-post', name: 'post' }],
        relation_definition: [
          {
            _id: 'legacy-relation',
            sourceTable: 'legacy-post',
            propertyName: 'comments',
          },
        ],
        enfyra_relation: [
          {
            _id: 'canonical-relation',
            sourceTable: 'canonical-post',
            propertyName: 'comments',
          },
        ],
      },
    });

    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: jest.fn(() => true),
        getMongoDb: jest.fn(() => mongo.db),
      } as any,
      systemCoreTableResolver: {
        getTableName: jest.fn(async () => 'enfyra_table'),
      } as any,
    });

    await (service as any).runMongoCoreTableRenames([
      { from: 'table_definition', to: 'enfyra_table' },
      { from: 'relation_definition', to: 'enfyra_relation' },
    ]);

    expect(mongo.inserts).toEqual([]);
    expect(mongo.collections.enfyra_relation).toEqual([
      {
        _id: 'canonical-relation',
        sourceTable: 'canonical-post',
        propertyName: 'comments',
      },
    ]);
  });
});
