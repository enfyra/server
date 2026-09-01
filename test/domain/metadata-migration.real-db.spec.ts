import knex, { type Knex } from 'knex';
import { MongoClient, type Db } from 'mongodb';
import { MetadataTableRenameService } from '../../src/engines/bootstrap/services/metadata-migration/metadata-table-rename.service';
import { MetadataPhysicalMigrationHelper } from '../../src/engines/bootstrap/utils/metadata-physical-migration.util';
import { repairSqlSystemPhysicalTarget } from '../../src/engines/bootstrap/utils/sql-system-physical-healing.util';
import { getCurrentDatabaseSchema } from '../../src/engines/knex/utils/provision/schema-comparison';
import { applySqlSchemaMigrations } from '../../src/shared/utils/provision-schema-migration';
import { parseSnapshotToSchema } from '../../src/engines/knex/utils/provision/schema-parser';
import { syncTable } from '../../src/engines/knex/utils/provision/sync-table';

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
  const systemCoreTableResolver = {
    getTableName: async (key: string) => `enfyra_${key}`,
    getNames: async () => ({
      table: 'enfyra_table',
      column: 'enfyra_column',
      relation: 'enfyra_relation',
    }),
  } as any;
  return new MetadataTableRenameService({
    queryBuilderService,
    systemCoreTableResolver,
    physicalMigration: new MetadataPhysicalMigrationHelper({
      queryBuilderService,
    }),
    verbose: () => undefined,
  });
}

function normalizeSqlBooleans<T extends Record<string, any>>(rows: T[]): T[] {
  return rows.map((row) => ({
    ...row,
    ...(Object.prototype.hasOwnProperty.call(row, 'isPublished')
      ? {
          isPublished:
            row.isPublished === null ? null : Boolean(row.isPublished),
        }
      : {}),
    ...(Object.prototype.hasOwnProperty.call(row, 'isPublic')
      ? {
          isPublic: row.isPublic === null ? null : Boolean(row.isPublic),
        }
      : {}),
  }));
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
      searchPath: [schema, 'public'],
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
  test('replaces every legacy PostgreSQL enum CHECK before applying new options', async () => {
    const config = SQL_DBS[0];
    const available = await probeSql(config);
    if (!available) {
      console.warn('postgres not available, skipping enum CHECK regression');
      return;
    }

    const { db, cleanup } = await makeIsolatedSqlDb(config);
    const tableName = 'payment_order_enum_regression';
    try {
      await db.schema.createTable(tableName, (table) => {
        table.increments('id').primary();
        table.string('paymentProvider').notNullable();
      });
      await db.raw(
        "ALTER TABLE ?? ADD CONSTRAINT ?? CHECK (?? IN ('sepay', 'paypal'))",
        [tableName, 'payment_provider_old_check', 'paymentProvider'],
      );
      await db.raw(
        "ALTER TABLE ?? ADD CONSTRAINT ?? CHECK (?? IN ('sepay', 'paypal', 'apipay'))",
        [tableName, 'payment_provider_new_check', 'paymentProvider'],
      );
      await db(tableName).insert({ paymentProvider: 'sepay' });

      await applySqlSchemaMigrations(db, {
        tables: [
          {
            _unique: { name: { _eq: tableName } },
            columnsToModify: [
              {
                from: {
                  name: 'paymentProvider',
                  type: 'enum',
                  options: ['sepay', 'paypal'],
                },
                to: {
                  name: 'paymentProvider',
                  type: 'enum',
                  options: ['sepay', 'paypal', 'apipay'],
                },
              },
            ],
          },
        ],
      });

      const checks = await db.raw(
        `
          SELECT constraint_def.conname
          FROM pg_constraint constraint_def
          JOIN pg_class relation ON relation.oid = constraint_def.conrelid
          JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
          WHERE constraint_def.contype = 'c'
            AND namespace.nspname = current_schema()
            AND relation.relname = ?
        `,
        [tableName],
      );
      expect(checks.rows).toEqual([]);
      await expect(
        db(tableName).insert({ paymentProvider: 'apipay' }),
      ).resolves.toBeDefined();
    } finally {
      await cleanup();
    }
  });

  test('syncTable upgrades a varchar-backed PostgreSQL enum with duplicate CHECKs', async () => {
    const config = SQL_DBS[0];
    const available = await probeSql(config);
    if (!available) {
      console.warn(
        'postgres not available, skipping syncTable enum regression',
      );
      return;
    }

    const { db, cleanup } = await makeIsolatedSqlDb(config);
    const tableName = 'sync_table_enum_regression';
    try {
      await db.schema.createTable('enfyra_table', (table) => {
        table.increments('id').primary();
        table.string('name').notNullable();
      });
      await db.schema.createTable('enfyra_relation', (table) => {
        table.increments('id').primary();
        table.integer('sourceTableId').notNullable();
        table.string('propertyName').notNullable();
        table.string('type').notNullable();
        table.string('onDelete').notNullable().defaultTo('SET NULL');
      });
      await db.schema.createTable(tableName, (table) => {
        table.increments('id').primary();
        table.string('paymentProvider').notNullable();
        table.timestamp('createdAt').notNullable().defaultTo(db.fn.now());
        table.timestamp('updatedAt').notNullable().defaultTo(db.fn.now());
      });
      await db('enfyra_table').insert({ name: tableName });
      await db.raw(
        "ALTER TABLE ?? ADD CONSTRAINT ?? CHECK (?? IN ('sepay', 'paypal'))",
        [tableName, 'sync_payment_provider_old_check', 'paymentProvider'],
      );
      await db.raw(
        "ALTER TABLE ?? ADD CONSTRAINT ?? CHECK (?? IN ('sepay', 'paypal', 'apipay'))",
        [tableName, 'sync_payment_provider_new_check', 'paymentProvider'],
      );
      await db(tableName).insert({ paymentProvider: 'sepay' });

      const schemas = parseSnapshotToSchema({
        [tableName]: {
          name: tableName,
          isSystem: true,
          columns: [
            {
              name: 'id',
              type: 'int',
              isPrimary: true,
              isGenerated: true,
              isNullable: false,
              isSystem: true,
            },
            {
              name: 'paymentProvider',
              type: 'enum',
              options: ['sepay', 'paypal', 'apipay'],
              isNullable: false,
              isSystem: true,
            },
          ],
          relations: [],
          indexes: [],
          uniques: [],
        },
      });
      const schema = schemas[0];
      if (!schema) throw new Error('enum regression schema was not generated');
      await syncTable(db, schema, schemas);

      const column = await db.raw(
        `
          SELECT data_type, udt_name
          FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name = ?
            AND column_name = 'paymentProvider'
        `,
        [tableName],
      );
      expect(column.rows[0]?.data_type).toBe('USER-DEFINED');
      expect(column.rows[0]?.udt_name).toBe(
        `${tableName}_paymentProvider_enum`,
      );
      await expect(
        db(tableName).insert({ paymentProvider: 'apipay' }),
      ).resolves.toBeDefined();
    } finally {
      await cleanup();
    }
  });

  test('syncTable upgrades a varchar-backed MySQL enum without reverting it to text', async () => {
    const config = SQL_DBS[1];
    const available = await probeSql(config);
    if (!available) {
      console.warn('mysql not available, skipping syncTable enum regression');
      return;
    }

    const { db, cleanup } = await makeIsolatedSqlDb(config);
    const tableName = 'sync_table_enum_regression';
    try {
      await db.schema.createTable('enfyra_table', (table) => {
        table.increments('id').primary();
        table.string('name').notNullable();
      });
      await db.schema.createTable('enfyra_relation', (table) => {
        table.increments('id').primary();
        table.integer('sourceTableId').notNullable();
        table.string('propertyName').notNullable();
        table.string('type').notNullable();
        table.string('onDelete').notNullable().defaultTo('SET NULL');
      });
      await db.schema.createTable(tableName, (table) => {
        table.increments('id').primary();
        table.string('paymentProvider').notNullable();
        table.timestamp('createdAt').notNullable().defaultTo(db.fn.now());
        table.timestamp('updatedAt').notNullable().defaultTo(db.fn.now());
      });
      await db('enfyra_table').insert({ name: tableName });
      await db(tableName).insert({ paymentProvider: 'sepay' });

      const schemas = parseSnapshotToSchema({
        [tableName]: {
          name: tableName,
          isSystem: true,
          columns: [
            {
              name: 'id',
              type: 'int',
              isPrimary: true,
              isGenerated: true,
              isNullable: false,
              isSystem: true,
            },
            {
              name: 'paymentProvider',
              type: 'enum',
              options: ['sepay', 'paypal', 'apipay'],
              isNullable: false,
              isSystem: true,
            },
          ],
          relations: [],
          indexes: [],
          uniques: [],
        },
      });
      const schema = schemas[0];
      if (!schema) throw new Error('enum regression schema was not generated');
      await syncTable(db, schema, schemas);

      const [columns] = await db.raw(
        `
          SELECT COLUMN_TYPE
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            AND COLUMN_NAME = 'paymentProvider'
        `,
        [tableName],
      );
      expect(String(columns[0]?.COLUMN_TYPE)).toBe(
        "enum('sepay','paypal','apipay')",
      );
      await expect(
        db(tableName).insert({ paymentProvider: 'apipay' }),
      ).resolves.toBeDefined();
    } finally {
      await cleanup();
    }
  });

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
        await db.schema.alterTable(names.tableOld, (table) => {
          table.string('operatorTag');
        });
        await db.schema.alterTable(names.columnOld, (table) => {
          table.string('operatorTag');
        });
        await db.schema.alterTable(names.relationOld, (table) => {
          table.string('operatorTag');
        });

        await db(names.tableOld).insert([
          { id: 10, name: 'table_definition', operatorTag: 'legacy-core' },
          { id: 11, name: 'post', operatorTag: 'custom-post' },
          { id: 12, name: 'comment', operatorTag: 'custom-comment' },
        ]);
        await db(names.tableNew).insert([{ id: 10, name: 'enfyra_table' }]);
        await db(names.columnOld).insert([
          { id: 20, tableId: 11, name: 'title', operatorTag: 'title-field' },
          { id: 21, tableId: 12, name: 'body', operatorTag: 'body-field' },
        ]);
        await db(names.columnNew).insert([{ id: 20, tableId: 10, name: 'id' }]);
        await db(names.relationOld).insert([
          {
            id: 30,
            sourceTableId: 11,
            targetTableId: 12,
            propertyName: 'comments',
            operatorTag: 'comments-relation',
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

        await service.runSqlCoreTableRenames(renames);
        await service.runSqlCoreTableRenames(renames);
        await service.cleanupRenamedTables(renames, false);
        await service.cleanupRenamedTables(renames, false);

        const tables = await db(names.tableNew).select('*').orderBy('name');
        const post = tables.find((row) => row.name === 'post');
        const comment = tables.find((row) => row.name === 'comment');
        expect(tables.filter((row) => row.name === 'post')).toHaveLength(1);
        expect(tables.filter((row) => row.name === 'comment')).toHaveLength(1);
        expect(post.operatorTag).toBe('custom-post');
        expect(comment.operatorTag).toBe('custom-comment');
        expect(tables.some((row) => row.name === 'table_definition')).toBe(
          false,
        );

        const columns = await db(names.columnNew).select('*');
        expect(
          columns.filter(
            (row) =>
              row.tableId === post.id &&
              row.name === 'title' &&
              row.operatorTag === 'title-field',
          ),
        ).toHaveLength(1);
        expect(
          columns.filter(
            (row) =>
              row.tableId === comment.id &&
              row.name === 'body' &&
              row.operatorTag === 'body-field',
          ),
        ).toHaveLength(1);

        const relations = await db(names.relationNew).select('*');
        expect(
          relations.filter(
            (row) =>
              row.sourceTableId === post.id &&
              row.targetTableId === comment.id &&
              row.propertyName === 'comments' &&
              row.operatorTag === 'comments-relation',
          ),
        ).toHaveLength(1);
        for (const legacyName of [
          names.tableOld,
          names.columnOld,
          names.relationOld,
        ]) {
          expect(await db.schema.hasTable(legacyName)).toBe(false);
        }
      } finally {
        await cleanup();
      }
    });

    test(`repairs system physical target contracts on ${config.name}`, async () => {
      const available = await probeSql(config);
      if (!available) {
        console.warn(`${config.name} not available, skipping SQL stress test`);
        return;
      }

      const { db, cleanup } = await makeIsolatedSqlDb(config);
      try {
        await db.schema.createTable('target_parent', (table) => {
          table.increments('id').primary();
          table.timestamp('createdAt').defaultTo(db.fn.now());
          table.timestamp('updatedAt').defaultTo(db.fn.now());
        });
        await db.schema.createTable('target_child', (table) => {
          table.increments('id').primary();
          table.boolean('requiredFlag').nullable().defaultTo(false);
          table.string('status').notNullable().defaultTo('active');
          table.integer('parentId').unsigned().nullable();
          table
            .foreign('parentId')
            .references('id')
            .inTable('target_parent')
            .onDelete('CASCADE');
        });

        await repairSqlSystemPhysicalTarget(db, {
          target_parent: {
            name: 'target_parent',
            isSystem: true,
            columns: [
              {
                name: 'id',
                type: 'int',
                isPrimary: true,
                isGenerated: true,
                isNullable: false,
              },
            ],
            relations: [],
          },
          target_child: {
            name: 'target_child',
            isSystem: true,
            columns: [
              {
                name: 'id',
                type: 'int',
                isPrimary: true,
                isGenerated: true,
                isNullable: false,
              },
              {
                name: 'requiredFlag',
                type: 'boolean',
                isNullable: false,
                defaultValue: false,
              },
              {
                name: 'status',
                type: 'enum',
                options: ['active', 'paused'],
                isNullable: false,
                defaultValue: 'active',
              },
            ],
            relations: [
              {
                propertyName: 'parent',
                type: 'many-to-one',
                targetTable: 'target_parent',
                foreignKeyColumn: 'parentId',
                isNullable: false,
                onDelete: 'CASCADE',
              },
            ],
          },
        });
        await repairSqlSystemPhysicalTarget(db, {
          target_parent: {
            name: 'target_parent',
            isSystem: true,
            columns: [],
            relations: [],
          },
          target_child: {
            name: 'target_child',
            isSystem: true,
            columns: [
              {
                name: 'requiredFlag',
                type: 'boolean',
                isNullable: false,
              },
              {
                name: 'status',
                type: 'enum',
                options: ['active', 'paused'],
                isNullable: false,
                defaultValue: 'active',
              },
            ],
            relations: [
              {
                propertyName: 'parent',
                type: 'many-to-one',
                targetTable: 'target_parent',
                foreignKeyColumn: 'parentId',
                isNullable: false,
                onDelete: 'CASCADE',
              },
            ],
          },
        });

        expect(await db.schema.hasColumn('target_child', 'createdAt')).toBe(
          true,
        );
        expect(await db.schema.hasColumn('target_child', 'updatedAt')).toBe(
          true,
        );
        const current = await getCurrentDatabaseSchema(db, 'target_child');
        expect(
          current.columns.find((column) => column.name === 'requiredFlag')
            ?.isNullable,
        ).toBe(false);
        expect(
          current.columns.find((column) => column.name === 'parentId')
            ?.isNullable,
        ).toBe(false);
        expect(
          current.columns.find((column) => column.name === 'status')?.type,
        ).toBe('enum');
        await db('target_parent').insert({});
        await expect(
          db('target_child').insert({ status: 'paused', parentId: 1 }),
        ).resolves.toBeDefined();
        expect(
          current.indexes.some(
            (index) => index.columns.join('|') === 'createdAt|id',
          ),
        ).toBe(true);
        expect(
          current.indexes.some(
            (index) => index.columns.join('|') === 'updatedAt|id',
          ),
        ).toBe(true);
      } finally {
        await cleanup();
      }
    });

    test(`heals non-core table overlap with custom fields on ${config.name}`, async () => {
      const available = await probeSql(config);
      if (!available) {
        console.warn(`${config.name} not available, skipping SQL stress test`);
        return;
      }

      const { db, cleanup } = await makeIsolatedSqlDb(config);
      try {
        await dropSqlTables(db, [
          'user_definition',
          'enfyra_user',
          'enfyra_table',
        ]);
        await db.schema.createTable('user_definition', (table) => {
          table.integer('id').primary();
          table.string('email');
          table.string('displayName');
          table.string('favoriteColor');
        });
        await db.schema.createTable('enfyra_user', (table) => {
          table.integer('id').primary();
          table.string('email');
          table.string('displayName');
        });
        await createSqlCoreStore(db, 'enfyra_table');

        await db('user_definition').insert([
          {
            id: 1,
            email: 'same@example.com',
            displayName: 'Canonical',
            favoriteColor: 'green',
          },
          {
            id: 2,
            email: 'new@example.com',
            displayName: 'New User',
            favoriteColor: 'blue',
          },
        ]);
        await db('enfyra_user').insert({
          id: 1,
          email: 'same@example.com',
          displayName: 'Canonical',
        });

        const service = makeService({
          isMongoDb: () => false,
          getKnex: () => db,
        });
        const rename = {
          from: 'user_definition',
          to: 'enfyra_user',
          mergeKeys: ['email'],
        };
        await service.renameSqlTable(rename);
        await service.renameSqlTable(rename);

        const users = await db('enfyra_user').select('*').orderBy('id');
        expect(users).toEqual([
          {
            id: 1,
            email: 'same@example.com',
            displayName: 'Canonical',
            favoriteColor: 'green',
          },
          {
            id: 2,
            email: 'new@example.com',
            displayName: 'New User',
            favoriteColor: 'blue',
          },
        ]);
      } finally {
        await cleanup();
      }
    });

    test(`rejects conflicting physical rename atomically on ${config.name}`, async () => {
      const available = await probeSql(config);
      if (!available) {
        console.warn(`${config.name} not available, skipping SQL stress test`);
        return;
      }

      const { db, cleanup } = await makeIsolatedSqlDb(config);
      try {
        await dropSqlTables(db, ['enfyra_file']);
        await db.schema.createTable('enfyra_file', (table) => {
          table.integer('id').primary();
          table.boolean('isPublished');
          table.boolean('isPublic');
        });
        await db('enfyra_file').insert([
          { id: 1, isPublished: true, isPublic: false },
          { id: 2, isPublished: true, isPublic: null },
        ]);

        const helper = new MetadataPhysicalMigrationHelper({
          queryBuilderService: {
            getKnex: () => db,
          } as any,
          verbose: () => undefined,
        });
        await expect(
          helper.renameSqlPhysicalColumnIfNeeded(
            'enfyra_file',
            'isPublished',
            'isPublic',
          ),
        ).rejects.toThrow(/conflicting row/);

        const rows = normalizeSqlBooleans(
          await db('enfyra_file').select('*').orderBy('id'),
        );
        expect(rows).toEqual([
          { id: 1, isPublished: true, isPublic: false },
          { id: 2, isPublished: true, isPublic: null },
        ]);
        expect(await db.schema.hasColumn('enfyra_file', 'isPublished')).toBe(
          true,
        );
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
        {
          _id: 'table-id',
          name: 'table_definition',
          operatorTag: 'legacy-core',
        },
        { _id: 'post-id', name: 'post', operatorTag: 'custom-post' },
        { _id: 'comment-id', name: 'comment', operatorTag: 'custom-comment' },
      ]);
      await db
        .collection(names.tableNew)
        .insertOne({ _id: 'table-id', name: 'enfyra_table' });
      await db.collection(names.columnOld).insertMany([
        {
          _id: 'title-column',
          table: 'post-id',
          name: 'title',
          operatorTag: 'title-field',
        },
        {
          _id: 'body-column',
          table: 'comment-id',
          name: 'body',
          operatorTag: 'body-field',
        },
      ]);
      await db
        .collection(names.columnNew)
        .insertOne({ _id: 'title-column', table: 'table-id', name: 'id' });
      await db.collection(names.relationOld).insertOne({
        _id: 'comments-relation',
        sourceTable: 'post-id',
        targetTable: 'comment-id',
        propertyName: 'comments',
        operatorTag: 'comments-relation',
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

      await service.runMongoCoreTableRenames(renames);
      await service.runMongoCoreTableRenames(renames);
      await service.cleanupRenamedTables(renames, true);
      await service.cleanupRenamedTables(renames, true);

      const tables = await db.collection(names.tableNew).find({}).toArray();
      const post = tables.find((row) => row.name === 'post');
      const comment = tables.find((row) => row.name === 'comment');
      expect(tables.filter((row) => row.name === 'post')).toHaveLength(1);
      expect(tables.filter((row) => row.name === 'comment')).toHaveLength(1);
      expect(post?.operatorTag).toBe('custom-post');
      expect(comment?.operatorTag).toBe('custom-comment');
      expect(tables.some((row) => row.name === 'table_definition')).toBe(false);

      const columns = await db.collection(names.columnNew).find({}).toArray();
      expect(
        columns.filter(
          (row) =>
            row.table === post?._id &&
            row.name === 'title' &&
            row.operatorTag === 'title-field',
        ),
      ).toHaveLength(1);
      expect(
        columns.filter(
          (row) =>
            row.table === comment?._id &&
            row.name === 'body' &&
            row.operatorTag === 'body-field',
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
            row.propertyName === 'comments' &&
            row.operatorTag === 'comments-relation',
        ),
      ).toHaveLength(1);
      for (const legacyName of [
        names.tableOld,
        names.columnOld,
        names.relationOld,
      ]) {
        expect(await db.listCollections({ name: legacyName }).hasNext()).toBe(
          false,
        );
      }
    } finally {
      if (db) await db.dropDatabase();
      await client.close();
    }
  });

  test('heals non-core collection overlap with custom fields on MongoDB', async () => {
    const available = await probeMongo();
    if (!available) {
      console.warn('MongoDB not available, skipping Mongo stress test');
      return;
    }

    const suffix = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const dbName = `metadata_migration_stress_${suffix}`;
    const client = new MongoClient(MONGO_URI);
    let db: Db | undefined;

    try {
      await client.connect();
      db = client.db(dbName);
      await db.collection('user_definition').insertMany([
        {
          _id: 'user-1',
          email: 'same@example.com',
          displayName: 'Canonical',
          favoriteColor: 'green',
        },
        {
          _id: 'user-2',
          email: 'new@example.com',
          displayName: 'New User',
          favoriteColor: 'blue',
        },
      ]);
      await db.collection('enfyra_user').insertOne({
        _id: 'user-1',
        email: 'same@example.com',
        displayName: 'Canonical',
      });

      const service = makeService({
        isMongoDb: () => true,
        getMongoDb: () => db,
      });
      const rename = {
        from: 'user_definition',
        to: 'enfyra_user',
        mergeKeys: ['email'],
      };
      await service.renameMongoTable(rename);
      await service.renameMongoTable(rename);

      const users = await db
        .collection('enfyra_user')
        .find({})
        .sort({ _id: 1 })
        .toArray();
      expect(users).toEqual([
        {
          _id: 'user-1',
          email: 'same@example.com',
          displayName: 'Canonical',
          favoriteColor: 'green',
        },
        {
          _id: 'user-2',
          email: 'new@example.com',
          displayName: 'New User',
          favoriteColor: 'blue',
        },
      ]);
    } finally {
      if (db) await db.dropDatabase();
      await client.close();
    }
  });

  test('rejects conflicting physical rename atomically on MongoDB', async () => {
    const available = await probeMongo();
    if (!available) {
      console.warn('MongoDB not available, skipping Mongo stress test');
      return;
    }

    const suffix = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const dbName = `metadata_migration_stress_${suffix}`;
    const client = new MongoClient(MONGO_URI);
    let db: Db | undefined;

    try {
      await client.connect();
      db = client.db(dbName);
      await db.collection('enfyra_file').insertMany([
        { _id: 'file-1', isPublished: true, isPublic: false },
        { _id: 'file-2', isPublished: true },
      ]);

      const helper = new MetadataPhysicalMigrationHelper({
        queryBuilderService: {
          isMongoDb: () => true,
          getMongoDb: () => db,
        } as any,
        verbose: () => undefined,
      });
      await expect(
        helper.renameMongoDocumentFieldIfNeeded(
          'enfyra_file',
          'isPublished',
          'isPublic',
        ),
      ).rejects.toThrow(/conflicting document/);

      const rows = await db
        .collection('enfyra_file')
        .find({})
        .sort({ _id: 1 })
        .toArray();
      expect(rows).toEqual([
        { _id: 'file-1', isPublished: true, isPublic: false },
        { _id: 'file-2', isPublished: true },
      ]);
    } finally {
      if (db) await db.dropDatabase();
      await client.close();
    }
  });
});
