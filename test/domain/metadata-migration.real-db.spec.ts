import knex, { type Knex } from 'knex';
import { MongoClient, type Db } from 'mongodb';
import { MetadataMigrationService } from '../../src/engines/bootstrap/services/metadata-migration.service';

const SQL_DBS = [
  {
    name: 'postgres',
    client: 'pg',
    connection:
      process.env.PG_TEST_URI ||
      'postgresql://root:1234@localhost:5432/postgres',
  },
  {
    name: 'mysql',
    client: 'mysql2',
    connection:
      process.env.MYSQL_TEST_URI || 'mysql://root:1234@localhost:3306/enfyra',
  },
];

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';

async function probeSql(config: (typeof SQL_DBS)[number]): Promise<boolean> {
  const db = knex({ client: config.client, connection: config.connection });
  try {
    await db.raw('select 1');
    return true;
  } catch {
    return false;
  } finally {
    await db.destroy();
  }
}

async function probeMongo(): Promise<boolean> {
  const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2000 });
  try {
    await client.connect();
    return true;
  } catch {
    return false;
  } finally {
    await client.close().catch(() => undefined);
  }
}

function makeService(queryBuilderService: any) {
  return new MetadataMigrationService({
    queryBuilderService,
    systemCoreTableResolver: {
      getTableName: async (key: string) => `enfyra_${key}`,
    } as any,
  });
}

async function dropSqlTables(db: Knex, names: string[]) {
  for (const name of names) {
    await db.schema.dropTableIfExists(name);
  }
}

async function makeIsolatedSqlDb(config: (typeof SQL_DBS)[number]) {
  const suffix = `${Date.now()}_${Math.random().toString(16).slice(2)}`.replace(
    /[^a-zA-Z0-9_]/g,
    '_',
  );
  const admin = knex({ client: config.client, connection: config.connection });

  if (config.client === 'pg') {
    const schema = `metadata_migration_stress_${suffix}`.toLowerCase();
    await admin.raw('create schema ??', [schema]);
    const db = knex({
      client: config.client,
      connection: config.connection,
      searchPath: [schema],
    });
    return {
      db,
      cleanup: async () => {
        await db.destroy();
        await admin.raw('drop schema if exists ?? cascade', [schema]);
        await admin.destroy();
      },
    };
  }

  const database = `metadata_migration_stress_${suffix}`.toLowerCase();
  await admin.raw('create database ??', [database]);
  const url = new URL(config.connection);
  url.pathname = `/${database}`;
  const db = knex({ client: config.client, connection: url.toString() });
  return {
    db,
    cleanup: async () => {
      await db.destroy();
      await admin.raw('drop database if exists ??', [database]);
      await admin.destroy();
    },
  };
}

async function createSqlCoreStore(db: Knex, name: string) {
  await db.schema.createTable(name, (table) => {
    table.increments('id').primary();
    table.string('name');
  });
}

async function createSqlColumnStore(db: Knex, name: string) {
  await db.schema.createTable(name, (table) => {
    table.increments('id').primary();
    table.integer('tableId');
    table.string('name');
  });
}

async function createSqlRelationStore(db: Knex, name: string) {
  await db.schema.createTable(name, (table) => {
    table.increments('id').primary();
    table.integer('sourceTableId');
    table.integer('targetTableId');
    table.string('propertyName');
  });
}

describe('MetadataMigrationService real DB self-healing stress', () => {
  for (const config of SQL_DBS) {
    test(`heals repeated core table overlap on ${config.name}`, async () => {
      const available = await probeSql(config);
      if (!available) {
        console.warn(`${config.name} not available, skipping SQL stress test`);
        return;
      }

      const names = {
        tableOld: 'table_definition',
        tableNew: 'enfyra_table',
        columnOld: 'column_definition',
        columnNew: 'enfyra_column',
        relationOld: 'relation_definition',
        relationNew: 'enfyra_relation',
      };
      const { db, cleanup } = await makeIsolatedSqlDb(config);

      try {
        await dropSqlTables(db, Object.values(names));
        await createSqlCoreStore(db, names.tableOld);
        await createSqlCoreStore(db, names.tableNew);
        await createSqlColumnStore(db, names.columnOld);
        await createSqlColumnStore(db, names.columnNew);
        await createSqlRelationStore(db, names.relationOld);
        await createSqlRelationStore(db, names.relationNew);

        await db(names.tableOld).insert([
          { id: 10, name: 'table_definition' },
          { id: 11, name: 'post' },
          { id: 12, name: 'comment' },
        ]);
        await db(names.tableNew).insert([{ id: 10, name: 'enfyra_table' }]);
        await db(names.columnOld).insert([
          { id: 20, tableId: 11, name: 'title' },
          { id: 21, tableId: 12, name: 'body' },
        ]);
        await db(names.columnNew).insert([{ id: 20, tableId: 10, name: 'id' }]);
        await db(names.relationOld).insert([
          {
            id: 30,
            sourceTableId: 11,
            targetTableId: 12,
            propertyName: 'comments',
          },
        ]);
        await db(names.relationNew).insert([
          {
            id: 30,
            sourceTableId: 10,
            targetTableId: 10,
            propertyName: 'self',
          },
        ]);

        const service = makeService({
          isMongoDb: () => false,
          getKnex: () => db,
        });
        const renames = [
          { from: names.tableOld, to: names.tableNew },
          { from: names.columnOld, to: names.columnNew },
          { from: names.relationOld, to: names.relationNew },
        ];

        await (service as any).runSqlCoreTableRenames(renames);
        await (service as any).runSqlCoreTableRenames(renames);

        const tables = await db(names.tableNew).select('*').orderBy('name');
        const post = tables.find((row) => row.name === 'post');
        const comment = tables.find((row) => row.name === 'comment');
        expect(tables.filter((row) => row.name === 'post')).toHaveLength(1);
        expect(tables.filter((row) => row.name === 'comment')).toHaveLength(1);
        expect(tables.some((row) => row.name === 'table_definition')).toBe(
          false,
        );

        const columns = await db(names.columnNew).select('*');
        expect(
          columns.filter((row) => row.tableId === post.id && row.name === 'title'),
        ).toHaveLength(1);
        expect(
          columns.filter(
            (row) => row.tableId === comment.id && row.name === 'body',
          ),
        ).toHaveLength(1);

        const relations = await db(names.relationNew).select('*');
        expect(
          relations.filter(
            (row) =>
              row.sourceTableId === post.id &&
              row.targetTableId === comment.id &&
              row.propertyName === 'comments',
          ),
        ).toHaveLength(1);
      } finally {
        await cleanup();
      }
    });
  }

  test('heals repeated core collection overlap on MongoDB', async () => {
    const available = await probeMongo();
    if (!available) {
      console.warn('MongoDB not available, skipping Mongo stress test');
      return;
    }

    const suffix = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const dbName = `metadata_migration_stress_${suffix}`;
    const names = {
      tableOld: 'table_definition',
      tableNew: 'enfyra_table',
      columnOld: 'column_definition',
      columnNew: 'enfyra_column',
      relationOld: 'relation_definition',
      relationNew: 'enfyra_relation',
    };
    const client = new MongoClient(MONGO_URI);
    let db: Db | undefined;

    try {
      await client.connect();
      db = client.db(dbName);
      await db.collection(names.tableOld).insertMany([
        { _id: 'table-id', name: 'table_definition' },
        { _id: 'post-id', name: 'post' },
        { _id: 'comment-id', name: 'comment' },
      ]);
      await db
        .collection(names.tableNew)
        .insertOne({ _id: 'table-id', name: 'enfyra_table' });
      await db.collection(names.columnOld).insertMany([
        { _id: 'title-column', table: 'post-id', name: 'title' },
        { _id: 'body-column', table: 'comment-id', name: 'body' },
      ]);
      await db
        .collection(names.columnNew)
        .insertOne({ _id: 'title-column', table: 'table-id', name: 'id' });
      await db.collection(names.relationOld).insertOne({
        _id: 'comments-relation',
        sourceTable: 'post-id',
        targetTable: 'comment-id',
        propertyName: 'comments',
      });
      await db.collection(names.relationNew).insertOne({
        _id: 'comments-relation',
        sourceTable: 'table-id',
        targetTable: 'table-id',
        propertyName: 'self',
      });

      const service = makeService({
        isMongoDb: () => true,
        getMongoDb: () => db,
      });
      const renames = [
        { from: names.tableOld, to: names.tableNew },
        { from: names.columnOld, to: names.columnNew },
        { from: names.relationOld, to: names.relationNew },
      ];

      await (service as any).runMongoCoreTableRenames(renames);
      await (service as any).runMongoCoreTableRenames(renames);

      const tables = await db.collection(names.tableNew).find({}).toArray();
      const post = tables.find((row) => row.name === 'post');
      const comment = tables.find((row) => row.name === 'comment');
      expect(tables.filter((row) => row.name === 'post')).toHaveLength(1);
      expect(tables.filter((row) => row.name === 'comment')).toHaveLength(1);
      expect(tables.some((row) => row.name === 'table_definition')).toBe(false);

      const columns = await db.collection(names.columnNew).find({}).toArray();
      expect(
        columns.filter((row) => row.table === post?._id && row.name === 'title'),
      ).toHaveLength(1);
      expect(
        columns.filter(
          (row) => row.table === comment?._id && row.name === 'body',
        ),
      ).toHaveLength(1);

      const relations = await db
        .collection(names.relationNew)
        .find({})
        .toArray();
      expect(
        relations.filter(
          (row) =>
            row.sourceTable === post?._id &&
            row.targetTable === comment?._id &&
            row.propertyName === 'comments',
        ),
      ).toHaveLength(1);
    } finally {
      if (db) await db.dropDatabase();
      await client.close();
    }
  });
});
