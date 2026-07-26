import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { knex, type Knex } from 'knex';
import { MongoClient } from 'mongodb';
import {
  getForeignKeyColumnName,
  getJunctionColumnNames,
  getJunctionTableName,
} from '@enfyra/kernel';
import {
  applyMongoSchemaMigrations,
  applySqlSchemaMigrations,
} from '../../src/shared/utils/provision-schema-migration';
import type { SchemaMigrationDef } from '../../src/shared/types/schema-migration.types';
import { MetadataMigrationService } from '../../src/engines/bootstrap/services/metadata-migration.service';

type SqlDatabase = 'postgres' | 'mysql';

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function sqlConnection(database: SqlDatabase, databaseName?: string) {
  const prefix = database === 'postgres' ? 'POSTGRES' : 'MYSQL';
  return {
    host: process.env[`MATRIX_${prefix}_HOST`] || '127.0.0.1',
    port: Number(
      process.env[`MATRIX_${prefix}_PORT`] ||
        (database === 'postgres' ? 5432 : 3306),
    ),
    user: required(`MATRIX_${prefix}_USER`),
    password: required(`MATRIX_${prefix}_PASSWORD`),
    database: databaseName || required(`MATRIX_${prefix}_DATABASE`),
  };
}

function createKnex(database: SqlDatabase, databaseName?: string): Knex {
  return knex({
    client: database === 'postgres' ? 'pg' : 'mysql2',
    connection: sqlConnection(database, databaseName),
  });
}

function buildDatabaseUri(
  protocol: 'postgresql' | 'mysql' | 'mongodb',
  connection: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
  },
  authSource?: string,
): string {
  const url = new URL(`${protocol}://${connection.host}`);
  url.port = String(connection.port);
  url.username = connection.user;
  url.password = connection.password;
  url.pathname = `/${connection.database}`;
  if (authSource) url.searchParams.set('authSource', authSource);
  return url.toString();
}

async function stopServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill('SIGTERM');
  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => {
      if (child.exitCode === null) child.kill('SIGKILL');
    }, 10_000);
    child.once('exit', () => {
      clearTimeout(timeout);
      resolve();
    });
  });
}

async function bootServer(dbUri: string, port: number): Promise<ChildProcess> {
  const child = spawn('yarn', ['tsx', 'src/main.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      DB_URI: dbUri,
      REDIS_URI: process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/14',
      PORT: String(port),
      SECRET_KEY: `snapshot-migration-e2e-${randomUUID()}`,
      ADMIN_EMAIL: 'snapshot-migration-e2e@localhost.test',
      ADMIN_PASSWORD: `e2e-${randomUUID()}`,
      NODE_ENV: 'test',
      NODE_NAME: `snapshot-migration-e2e-${port}`,
      BOOTSTRAP_VERBOSE: '0',
      MONGO_FORCE_APP_TRANSACTION: '0',
      ISOLATED_EXECUTOR_FILE_LOG: '0',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL');
      reject(
        new Error(
          `Server boot timed out on port ${port}: ${output.slice(-4000)}`,
        ),
      );
    }, 90_000);
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      if (!output.includes(`HTTP listening on port ${port}`)) return;
      clearTimeout(timeout);
      resolve();
    };
    child.stdout?.on('data', onData);
    child.stderr?.on('data', onData);
    child.once('exit', (code) => {
      clearTimeout(timeout);
      reject(
        new Error(
          `Server exited before listening with code ${code}: ${output.slice(-4000)}`,
        ),
      );
    });
  });

  return child;
}

function migrationDefinition(): SchemaMigrationDef {
  return {
    tablesToDrop: ['authors'],
    tables: [
      {
        _unique: { name: { _eq: 'posts' } },
        columnsToModify: [
          {
            from: { name: 'legacy_title' },
            to: { name: 'title' },
          },
        ],
        columnsToRemove: ['obsolete'],
        relationsToRemove: ['tags'],
      },
    ],
  };
}

async function createSqlFixture(db: Knex): Promise<void> {
  await db.schema.createTable('authors', (table) => {
    table.integer('id').primary();
  });
  await db.schema.createTable('tags', (table) => {
    table.integer('id').primary();
  });
  await db.schema.createTable('posts', (table) => {
    table.integer('id').primary();
    table.string('legacy_title');
    table.string('obsolete');
    table.integer('author_id');
    table
      .foreign('author_id')
      .references('id')
      .inTable('authors')
      .onDelete('CASCADE');
    table.index(['obsolete'], 'idx_posts_obsolete');
  });
  await db.schema.createTable('j_posts_tags', (table) => {
    table.integer('sourceId').references('id').inTable('posts');
    table.integer('targetId').references('id').inTable('tags');
  });
  await db.schema.createTable('enfyra_table', (table) => {
    table.integer('id').primary();
    table.string('name').notNullable();
  });
  await db.schema.createTable('enfyra_relation', (table) => {
    table.integer('id').primary();
    table.integer('sourceTableId').notNullable();
    table.integer('targetTableId');
    table.integer('mappedById');
    table.string('propertyName').notNullable();
    table.string('type').notNullable();
    table.string('foreignKeyColumn');
    table.string('junctionTableName');
  });
  await db.schema.createTable('enfyra_column', (table) => {
    table.integer('id').primary();
    table.integer('tableId').notNullable();
    table.string('name').notNullable();
  });
  await db.schema.createTable('enfyra_column_rule', (table) => {
    table.integer('id').primary();
    table.integer('columnId').notNullable();
  });
  await db.schema.createTable('enfyra_field_permission', (table) => {
    table.integer('id').primary();
    table.integer('columnId');
    table.integer('relationId');
  });
  await db.schema.createTable('enfyra_route', (table) => {
    table.integer('id').primary();
    table.integer('mainTableId').notNullable();
  });
  await db.schema.createTable('enfyra_graphql', (table) => {
    table.integer('id').primary();
    table.integer('tableId').notNullable();
  });
  await db('enfyra_table').insert([
    { id: 1, name: 'authors' },
    { id: 2, name: 'posts' },
    { id: 3, name: 'tags' },
  ]);
  await db('enfyra_relation').insert([
    {
      id: 1,
      sourceTableId: 2,
      targetTableId: 1,
      propertyName: 'author',
      type: 'many-to-one',
      foreignKeyColumn: 'author_id',
    },
    {
      id: 2,
      sourceTableId: 2,
      targetTableId: 3,
      propertyName: 'tags',
      type: 'many-to-many',
      junctionTableName: 'j_posts_tags',
    },
  ]);
  await db('enfyra_column').insert([
    { id: 1, tableId: 1, name: 'name' },
    { id: 2, tableId: 2, name: 'title' },
  ]);
  await db('enfyra_column_rule').insert([
    { id: 1, columnId: 1 },
    { id: 2, columnId: 2 },
  ]);
  await db('enfyra_field_permission').insert([
    { id: 1, columnId: 1, relationId: null },
    { id: 2, columnId: null, relationId: 1 },
    { id: 3, columnId: 2, relationId: 2 },
  ]);
  await db('enfyra_route').insert([
    { id: 1, mainTableId: 1 },
    { id: 2, mainTableId: 2 },
  ]);
  await db('enfyra_graphql').insert([
    { id: 1, tableId: 1 },
    { id: 2, tableId: 2 },
  ]);
  await db('authors').insert({ id: 1 });
  await db('tags').insert({ id: 1 });
  await db('posts').insert({
    id: 1,
    legacy_title: 'preserved',
    obsolete: 'remove',
    author_id: 1,
  });
  await db('j_posts_tags').insert({ sourceId: 1, targetId: 1 });
}

async function runSql(database: SqlDatabase): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_migration_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;

  try {
    if (database === 'postgres') {
      await admin.raw('CREATE DATABASE ??', [databaseName]);
    } else {
      await admin.raw('CREATE DATABASE ??', [databaseName]);
    }
    target = createKnex(database, databaseName);
    await createSqlFixture(target);
    await applySqlSchemaMigrations(target, migrationDefinition());

    assert.equal(await target.schema.hasTable('authors'), false);
    assert.equal(await target.schema.hasTable('j_posts_tags'), false);
    assert.equal(await target.schema.hasColumn('posts', 'legacy_title'), false);
    assert.equal(await target.schema.hasColumn('posts', 'title'), true);
    assert.equal(await target.schema.hasColumn('posts', 'obsolete'), false);
    assert.equal(await target.schema.hasColumn('posts', 'author_id'), false);
    assert.equal(
      (await target('posts').where({ id: 1 }).first()).title,
      'preserved',
    );
    const metadataMigration = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: () => false,
        getKnex: () => target,
      } as any,
      systemCoreTableResolver: {
        getNames: async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        }),
      } as any,
    });
    await (metadataMigration as any).dropTableMetadata(['authors'], false);
    assert.deepEqual(
      (await target('enfyra_table').orderBy('id')).map((row) => row.name),
      ['posts', 'tags'],
    );
    assert.deepEqual(
      (await target('enfyra_column').orderBy('id')).map((row) => row.id),
      [2],
    );
    assert.deepEqual(
      (await target('enfyra_relation').orderBy('id')).map((row) => row.id),
      [2],
    );
    assert.deepEqual(
      (await target('enfyra_column_rule').orderBy('id')).map((row) => row.id),
      [2],
    );
    assert.deepEqual(
      (await target('enfyra_field_permission').orderBy('id')).map(
        (row) => row.id,
      ),
      [3],
    );
    assert.deepEqual(
      (await target('enfyra_route').orderBy('id')).map((row) => row.id),
      [2],
    );
    assert.deepEqual(
      (await target('enfyra_graphql').orderBy('id')).map((row) => row.id),
      [2],
    );
  } finally {
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function runSqlPartialJunctionResume(
  database: SqlDatabase,
): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_junction_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;
  const tableName = 'enfyra_file_permission';
  const targetTable = 'enfyra_user';
  const propertyName = 'allowedUsers';
  const oldColumn = getForeignKeyColumnName(propertyName);
  const junctionTable = getJunctionTableName(
    tableName,
    propertyName,
    targetTable,
  );
  const { sourceColumn, targetColumn } = getJunctionColumnNames(
    tableName,
    propertyName,
    targetTable,
  );
  const migration: SchemaMigrationDef = {
    tables: [
      {
        _unique: { name: { _eq: tableName } },
        relationsToRemove: [propertyName],
      },
    ],
  };

  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    target = createKnex(database, databaseName);
    await target.schema.createTable(targetTable, (table) => {
      table.integer('id').primary();
    });
    await target.schema.createTable(tableName, (table) => {
      table.integer('id').primary();
      table.integer(oldColumn).references('id').inTable(targetTable);
    });
    await target.schema.createTable(junctionTable, (table) => {
      table.integer(sourceColumn).notNullable();
      table.integer(targetColumn).notNullable();
      table.primary([sourceColumn, targetColumn]);
      table.foreign(sourceColumn).references('id').inTable(tableName);
      table.foreign(targetColumn).references('id').inTable(targetTable);
    });
    await target(targetTable).insert([{ id: 1 }, { id: 2 }]);
    await target(tableName).insert([
      { id: 10, [oldColumn]: 1 },
      { id: 20, [oldColumn]: 2 },
    ]);
    await target(junctionTable).insert({
      [sourceColumn]: 10,
      [targetColumn]: 1,
    });

    await applySqlSchemaMigrations(target, migration);
    await applySqlSchemaMigrations(target, migration);

    assert.equal(await target.schema.hasColumn(tableName, oldColumn), false);
    assert.deepEqual(await target(junctionTable).orderBy(sourceColumn), [
      { [sourceColumn]: 10, [targetColumn]: 1 },
      { [sourceColumn]: 20, [targetColumn]: 2 },
    ]);
  } finally {
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function runSqlRenameConflict(database: SqlDatabase): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_conflict_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;
  const migration: SchemaMigrationDef = {
    tables: [
      {
        _unique: { name: { _eq: 'posts' } },
        columnsToModify: [
          {
            from: { name: 'legacy_title' },
            to: { name: 'title' },
          },
        ],
      },
    ],
  };

  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    target = createKnex(database, databaseName);
    await target.schema.createTable('posts', (table) => {
      table.integer('id').primary();
      table.string('legacy_title');
      table.string('title');
    });
    await target('posts').insert([
      {
        id: 1,
        legacy_title: 'legacy-value',
        title: 'new-value',
      },
      {
        id: 2,
        legacy_title: 'copy-later',
        title: null,
      },
    ]);

    await assert.rejects(
      applySqlSchemaMigrations(target, migration),
      /conflict/i,
    );
    assert.deepEqual(await target('posts').orderBy('id'), [
      {
        id: 1,
        legacy_title: 'legacy-value',
        title: 'new-value',
      },
      {
        id: 2,
        legacy_title: 'copy-later',
        title: null,
      },
    ]);

    await target('posts').where({ id: 1 }).update({ title: 'legacy-value' });
    await applySqlSchemaMigrations(target, migration);
    assert.equal(await target.schema.hasColumn('posts', 'legacy_title'), false);
    assert.deepEqual(await target('posts').orderBy('id'), [
      { id: 1, title: 'legacy-value' },
      { id: 2, title: 'copy-later' },
    ]);
  } finally {
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function runSqlColumnContract(database: SqlDatabase): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_column_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;
  const migration: SchemaMigrationDef = {
    tables: [
      {
        _unique: { name: { _eq: 'records' } },
        columnsToModify: [
          {
            from: {
              name: 'score',
              type: 'varchar',
              isNullable: true,
              defaultValue: '7',
            },
            to: {
              name: 'score',
              type: 'int',
              isNullable: false,
              defaultValue: 0,
            },
          },
          {
            from: {
              name: 'label',
              type: 'varchar',
              isNullable: true,
              defaultValue: 'legacy',
            },
            to: {
              name: 'label',
              type: 'varchar',
              isNullable: false,
              defaultValue: 'current',
            },
          },
          {
            from: {
              name: 'optional',
              type: 'varchar',
              isNullable: false,
              defaultValue: 'fallback',
            },
            to: {
              name: 'optional',
              type: 'varchar',
              isNullable: true,
              defaultValue: null,
            },
          },
        ],
      },
    ],
  };

  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    target = createKnex(database, databaseName);
    await target.schema.createTable('records', (table) => {
      table.integer('id').primary();
      table.string('score').nullable().defaultTo('7');
      table.string('label').nullable().defaultTo('legacy');
      table.string('optional').notNullable().defaultTo('fallback');
    });
    await target('records').insert({
      id: 1,
      score: '42',
      label: 'kept',
      optional: 'kept',
    });
    await target('records').insert({
      id: 3,
      score: null,
      label: null,
      optional: 'kept',
    });

    await applySqlSchemaMigrations(target, migration);
    await applySqlSchemaMigrations(target, migration);

    const info = await target('records').columnInfo();
    assert.match(String(info.score.type), /int/i);
    assert.equal(info.score.nullable, false);
    assert.equal(Number(info.score.defaultValue), 0);
    assert.equal(info.label.nullable, false);
    assert.match(String(info.label.defaultValue), /current/);
    assert.equal(info.optional.nullable, true);
    assert.equal(info.optional.defaultValue, null);
    assert.deepEqual(await target('records').where({ id: 1 }).first(), {
      id: 1,
      score: 42,
      label: 'kept',
      optional: 'kept',
    });
    assert.deepEqual(await target('records').where({ id: 3 }).first(), {
      id: 3,
      score: 0,
      label: 'current',
      optional: 'kept',
    });
    await target('records').insert({ id: 2 });
    const defaulted = await target('records').where({ id: 2 }).first();
    assert.equal(defaulted.score, 0);
    assert.equal(defaulted.label, 'current');
    assert.equal(defaulted.optional, null);
  } finally {
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function readSqlForeignKey(
  db: Knex,
  database: SqlDatabase,
  tableName: string,
  columnName: string,
): Promise<any> {
  if (database === 'postgres') {
    const result = await db.raw(
      `
        SELECT ccu.table_name AS target_table,
               ccu.column_name AS target_column,
               rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.constraint_schema = kcu.constraint_schema
        JOIN information_schema.referential_constraints rc
          ON tc.constraint_name = rc.constraint_name
         AND tc.constraint_schema = rc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu
          ON rc.unique_constraint_name = ccu.constraint_name
         AND rc.unique_constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = current_schema()
          AND tc.table_name = ?
          AND kcu.column_name = ?
      `,
      [tableName, columnName],
    );
    return result.rows[0];
  }

  const result = await db.raw(
    `
      SELECT kcu.REFERENCED_TABLE_NAME AS target_table,
             kcu.REFERENCED_COLUMN_NAME AS target_column,
             rc.DELETE_RULE AS delete_rule
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
       AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
      WHERE kcu.TABLE_SCHEMA = DATABASE()
        AND kcu.TABLE_NAME = ?
        AND kcu.COLUMN_NAME = ?
        AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
    `,
    [tableName, columnName],
  );
  return result[0][0];
}

async function runSqlRelationContract(database: SqlDatabase): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_relation_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;
  const migration: SchemaMigrationDef = {
    tables: [
      {
        _unique: { name: { _eq: 'posts' } },
        relationsToModify: [
          {
            from: {
              propertyName: 'owner',
              type: 'many-to-one',
              targetTable: 'authors',
              isNullable: true,
              onDelete: 'SET NULL',
              foreignKeyColumn: 'ownerId',
              referencedColumn: 'id',
            },
            to: {
              propertyName: 'publisher',
              type: 'many-to-one',
              targetTable: 'users',
              isNullable: false,
              onDelete: 'CASCADE',
              foreignKeyColumn: 'publisherId',
              referencedColumn: 'id',
            },
          },
        ],
      },
    ],
  };

  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    target = createKnex(database, databaseName);
    await target.schema.createTable('authors', (table) => {
      table.integer('id').primary();
    });
    await target.schema.createTable('users', (table) => {
      table.integer('id').primary();
    });
    await target.schema.createTable('posts', (table) => {
      table.integer('id').primary();
      table.integer('ownerId').nullable();
      table
        .foreign('ownerId')
        .references('id')
        .inTable('authors')
        .onDelete('SET NULL');
    });
    await target('authors').insert({ id: 1 });
    await target('users').insert({ id: 1 });
    await target('posts').insert({ id: 1, ownerId: 1 });

    await applySqlSchemaMigrations(target, migration);
    await applySqlSchemaMigrations(target, migration);

    assert.equal(await target.schema.hasColumn('posts', 'ownerId'), false);
    assert.equal(await target.schema.hasColumn('posts', 'publisherId'), true);
    const info = await target('posts').columnInfo('publisherId');
    assert.equal(info.nullable, false);
    const foreignKey = await readSqlForeignKey(
      target,
      database,
      'posts',
      'publisherId',
    );
    assert.equal(foreignKey.target_table, 'users');
    assert.equal(foreignKey.target_column, 'id');
    assert.equal(String(foreignKey.delete_rule).toUpperCase(), 'CASCADE');
    assert.equal(
      (await target('posts').where({ id: 1 }).first()).publisherId,
      1,
    );
    await target('users').where({ id: 1 }).delete();
    const remaining = await target('posts').count('* as count').first();
    assert.equal(Number(remaining?.count ?? 0), 0);
  } finally {
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function runSqlFailureGuards(database: SqlDatabase): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_failure_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;
  const relationMigration: SchemaMigrationDef = {
    tables: [
      {
        _unique: { name: { _eq: 'posts' } },
        relationsToModify: [
          {
            from: {
              propertyName: 'owner',
              type: 'many-to-one',
              targetTable: 'authors',
              isNullable: true,
              onDelete: 'SET NULL',
              foreignKeyColumn: 'ownerId',
              referencedColumn: 'id',
            },
            to: {
              propertyName: 'publisher',
              type: 'many-to-one',
              targetTable: 'users',
              isNullable: false,
              onDelete: 'CASCADE',
              foreignKeyColumn: 'publisherId',
              referencedColumn: 'id',
            },
          },
        ],
      },
    ],
  };
  const columnMigration: SchemaMigrationDef = {
    tables: [
      {
        _unique: { name: { _eq: 'records' } },
        columnsToModify: [
          {
            from: {
              name: 'score',
              type: 'varchar',
              isNullable: true,
              defaultValue: '7',
            },
            to: {
              name: 'score',
              type: 'int',
              isNullable: false,
              defaultValue: null,
            },
          },
        ],
      },
    ],
  };

  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    target = createKnex(database, databaseName);
    await target.schema.createTable('authors', (table) => {
      table.integer('id').primary();
    });
    await target.schema.createTable('users', (table) => {
      table.integer('id').primary();
    });
    await target.schema.createTable('posts', (table) => {
      table.integer('id').primary();
      table.integer('ownerId').nullable();
      table
        .foreign('ownerId')
        .references('id')
        .inTable('authors')
        .onDelete('SET NULL');
    });
    await target('authors').insert({ id: 1 });
    await target('posts').insert({ id: 1, ownerId: 1 });

    await assert.rejects(
      applySqlSchemaMigrations(target, relationMigration),
      /orphan values exist/,
    );
    assert.equal(await target.schema.hasColumn('posts', 'ownerId'), true);
    assert.equal(await target.schema.hasColumn('posts', 'publisherId'), false);
    const originalForeignKey = await readSqlForeignKey(
      target,
      database,
      'posts',
      'ownerId',
    );
    assert.equal(originalForeignKey.target_table, 'authors');
    assert.equal(
      String(originalForeignKey.delete_rule).toUpperCase(),
      'SET NULL',
    );
    await target('users').insert({ id: 1 });
    await applySqlSchemaMigrations(target, relationMigration);
    assert.equal(await target.schema.hasColumn('posts', 'ownerId'), false);
    assert.equal(await target.schema.hasColumn('posts', 'publisherId'), true);
    const recoveredForeignKey = await readSqlForeignKey(
      target,
      database,
      'posts',
      'publisherId',
    );
    assert.equal(recoveredForeignKey.target_table, 'users');
    assert.equal(
      String(recoveredForeignKey.delete_rule).toUpperCase(),
      'CASCADE',
    );

    await target.schema.createTable('records', (table) => {
      table.integer('id').primary();
      table.string('score').nullable().defaultTo('7');
    });
    await target('records').insert({ id: 1, score: null });
    await assert.rejects(applySqlSchemaMigrations(target, columnMigration));
    const scoreInfo = await target('records').columnInfo('score');
    assert.match(String(scoreInfo.type), /char|varchar/i);
    assert.equal(scoreInfo.nullable, true);
    assert.match(String(scoreInfo.defaultValue), /7/);
    await target('records').where({ id: 1 }).update({ score: '13' });
    await applySqlSchemaMigrations(target, columnMigration);
    const recoveredScoreInfo = await target('records').columnInfo('score');
    assert.match(String(recoveredScoreInfo.type), /int/i);
    assert.equal(recoveredScoreInfo.nullable, false);
    assert.equal(recoveredScoreInfo.defaultValue, null);
    assert.equal(
      Number((await target('records').where({ id: 1 }).first()).score),
      13,
    );
  } finally {
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function runSqlBoot(database: SqlDatabase, port: number): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_boot_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;
  let server: ChildProcess | null = null;

  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    const connection = sqlConnection(database, databaseName);
    target = createKnex(database, databaseName);
    server = await bootServer(
      buildDatabaseUri(
        database === 'postgres' ? 'postgresql' : 'mysql',
        connection,
      ),
      port,
    );
    const setting = await target('enfyra_setting').first();
    assert.equal(
      setting.isInit === true || setting.isInit === 1,
      true,
      `${database} server did not finish initialization`,
    );
    await stopServer(server);
    server = null;

    const settingTable = await target('enfyra_table')
      .where({ name: 'enfyra_setting' })
      .first();
    const columnTable = await target('enfyra_table')
      .where({ name: 'enfyra_column' })
      .first();
    await target.schema.alterTable('enfyra_setting', (table) => {
      table.text('userExtensionField').nullable();
    });
    await target.schema.alterTable('enfyra_column', (table) => {
      table.boolean('isHidden').nullable();
    });
    await target('enfyra_column').insert([
      {
        tableId: settingTable.id,
        name: 'userExtensionField',
        type: 'text',
        isSystem: false,
      },
      {
        tableId: columnTable.id,
        name: 'isHidden',
        type: 'boolean',
        isSystem: true,
      },
    ]);
    await target('enfyra_column').update({ isHidden: true });
    await target('enfyra_setting').update({
      isInit: false,
      userExtensionField: 'preserved',
    });

    server = await bootServer(
      buildDatabaseUri(
        database === 'postgres' ? 'postgresql' : 'mysql',
        connection,
      ),
      port,
    );
    const retriedSetting = await target('enfyra_setting').first();
    assert.equal(
      retriedSetting.isInit === true || retriedSetting.isInit === 1,
      true,
    );
    assert.equal(retriedSetting.userExtensionField, 'preserved');
    assert.equal(
      await target.schema.hasColumn('enfyra_setting', 'userExtensionField'),
      true,
    );
    assert.equal(
      Number(
        (
          await target('enfyra_column')
            .where({
              tableId: settingTable.id,
              name: 'userExtensionField',
              isSystem: false,
            })
            .count({ count: '*' })
        )[0].count,
      ),
      1,
    );
    assert.equal(
      await target.schema.hasColumn('enfyra_column', 'isHidden'),
      false,
    );
    assert.equal(
      Number(
        (
          await target('enfyra_column')
            .where({ tableId: columnTable.id, name: 'isHidden' })
            .count({ count: '*' })
        )[0].count,
      ),
      0,
    );
    await stopServer(server);
    server = null;

    await target('enfyra_file')
      .whereNull('description')
      .update({ description: '' });
    await target.schema.alterTable('enfyra_file', (table) => {
      table.text('description').notNullable().alter();
    });
    await target('enfyra_setting').update({ isInit: false });
    await assert.rejects(
      bootServer(
        buildDatabaseUri(
          database === 'postgres' ? 'postgresql' : 'mysql',
          connection,
        ),
        port,
      ),
      /physical column enfyra_file\.description differs on nullable|target attestation/i,
    );
    const failedSetting = await target('enfyra_setting').first();
    assert.equal(
      failedSetting.isInit === false || failedSetting.isInit === 0,
      true,
    );
  } finally {
    if (server) await stopServer(server);
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function runMongo(): Promise<void> {
  const mongoUser = encodeURIComponent(required('MATRIX_MONGO_USER'));
  const mongoPassword = encodeURIComponent(required('MATRIX_MONGO_PASSWORD'));
  const mongoHost = process.env.MATRIX_MONGO_HOST || '127.0.0.1';
  const mongoPort = Number(process.env.MATRIX_MONGO_PORT || 27017);
  const mongoAuthDatabase = encodeURIComponent(
    process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin',
  );
  const client = new MongoClient(
    `mongodb://${mongoUser}:${mongoPassword}@${mongoHost}:${mongoPort}/?authSource=${mongoAuthDatabase}`,
  );
  const databaseName = `enfyra_snapshot_migration_${randomUUID()
    .replaceAll('-', '')
    .slice(0, 12)}`;
  let connected = false;

  try {
    await client.connect();
    connected = true;
    const db = client.db(databaseName);
    await db.collection('enfyra_table').insertMany([
      { _id: 'authors-id' as any, name: 'authors' },
      { _id: 'posts-id' as any, name: 'posts' },
      { _id: 'tags-id' as any, name: 'tags' },
    ]);
    await db.collection('enfyra_relation').insertMany([
      {
        _id: 'author-relation' as any,
        sourceTable: 'posts-id',
        targetTable: 'authors-id',
        propertyName: 'author',
        type: 'many-to-one',
        foreignKeyColumn: 'author',
      },
      {
        _id: 'tags-relation' as any,
        sourceTable: 'posts-id',
        targetTable: 'tags-id',
        propertyName: 'tags',
        type: 'many-to-many',
        junctionTableName: 'j_posts_tags',
      },
    ]);
    await db.collection('enfyra_column').insertMany([
      { _id: 'author-column' as any, table: 'authors-id', name: 'name' },
      { _id: 'post-column' as any, table: 'posts-id', name: 'title' },
    ]);
    await db.collection('enfyra_column_rule').insertMany([
      { _id: 'author-rule' as any, column: 'author-column' },
      { _id: 'post-rule' as any, column: 'post-column' },
    ]);
    await db.collection('enfyra_field_permission').insertMany([
      { _id: 'author-column-permission' as any, column: 'author-column' },
      {
        _id: 'author-relation-permission' as any,
        relation: 'author-relation',
      },
      {
        _id: 'post-relation-permission' as any,
        column: 'post-column',
        relation: 'tags-relation',
      },
    ]);
    await db.collection('enfyra_route').insertMany([
      { _id: 'author-route' as any, mainTable: 'authors-id' },
      { _id: 'post-route' as any, mainTable: 'posts-id' },
    ]);
    await db.collection('enfyra_graphql').insertMany([
      { _id: 'author-graphql' as any, table: 'authors-id' },
      { _id: 'post-graphql' as any, table: 'posts-id' },
    ]);
    await db.collection('authors').insertOne({ _id: 1 });
    await db.collection('tags').insertOne({ _id: 1 });
    await db.collection('posts').insertOne({
      _id: 1,
      legacy_title: 'preserved',
      obsolete: 'remove',
      author: 1,
      tags: [1],
    });
    await db
      .collection('posts')
      .createIndex({ author: 1 }, { name: 'idx_author' });
    await db
      .collection('posts')
      .createIndex({ legacy_title: 1 }, { name: 'idx_legacy_title' });
    await db
      .collection('posts')
      .createIndex({ obsolete: 1 }, { name: 'idx_obsolete' });
    await db.collection('posts').createIndex({ tags: 1 }, { name: 'idx_tags' });
    await db.collection('j_posts_tags').insertOne({
      sourceId: 1,
      targetId: 1,
    });

    await applyMongoSchemaMigrations(db, migrationDefinition());
    await applyMongoSchemaMigrations(db, migrationDefinition());

    assert.equal(
      (await db.listCollections({ name: 'authors' }).toArray()).length,
      0,
    );
    assert.equal(
      (await db.listCollections({ name: 'j_posts_tags' }).toArray()).length,
      0,
    );
    const post = await db.collection('posts').findOne({ _id: 1 });
    assert.equal(post?.title, 'preserved');
    assert.equal('legacy_title' in (post ?? {}), false);
    assert.equal('obsolete' in (post ?? {}), false);
    assert.equal('author' in (post ?? {}), false);
    assert.equal('tags' in (post ?? {}), false);
    assert.deepEqual(
      (await db.collection('posts').listIndexes().toArray()).map(
        (index) => index.name,
      ),
      ['_id_', 'idx_title'],
    );
    const metadataMigration = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: () => true,
        getMongoDb: () => db,
      } as any,
      systemCoreTableResolver: {
        getNames: async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        }),
      } as any,
    });
    await (metadataMigration as any).dropTableMetadata(['authors'], true);
    assert.deepEqual(
      (await db.collection('enfyra_table').find({}).sort({ _id: 1 }).toArray())
        .map((row) => row.name)
        .sort(),
      ['posts', 'tags'],
    );
    assert.deepEqual(
      (
        await db.collection('enfyra_column').find({}).sort({ _id: 1 }).toArray()
      ).map((row) => row._id),
      ['post-column'],
    );
    assert.deepEqual(
      (
        await db
          .collection('enfyra_relation')
          .find({})
          .sort({ _id: 1 })
          .toArray()
      ).map((row) => row._id),
      ['tags-relation'],
    );
    assert.deepEqual(
      (
        await db
          .collection('enfyra_column_rule')
          .find({})
          .sort({ _id: 1 })
          .toArray()
      ).map((row) => row._id),
      ['post-rule'],
    );
    assert.deepEqual(
      (
        await db
          .collection('enfyra_field_permission')
          .find({})
          .sort({ _id: 1 })
          .toArray()
      ).map((row) => row._id),
      ['post-relation-permission'],
    );
    assert.deepEqual(
      (
        await db.collection('enfyra_route').find({}).sort({ _id: 1 }).toArray()
      ).map((row) => row._id),
      ['post-route'],
    );
    assert.deepEqual(
      (
        await db
          .collection('enfyra_graphql')
          .find({})
          .sort({ _id: 1 })
          .toArray()
      ).map((row) => row._id),
      ['post-graphql'],
    );
  } finally {
    if (connected) {
      await client.db(databaseName).dropDatabase();
    }
    await client.close();
  }
}

async function runMongoRenameConflict(): Promise<void> {
  const mongoUser = encodeURIComponent(required('MATRIX_MONGO_USER'));
  const mongoPassword = encodeURIComponent(required('MATRIX_MONGO_PASSWORD'));
  const mongoHost = process.env.MATRIX_MONGO_HOST || '127.0.0.1';
  const mongoPort = Number(process.env.MATRIX_MONGO_PORT || 27017);
  const mongoAuthDatabase = encodeURIComponent(
    process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin',
  );
  const client = new MongoClient(
    `mongodb://${mongoUser}:${mongoPassword}@${mongoHost}:${mongoPort}/?authSource=${mongoAuthDatabase}`,
  );
  const databaseName = `enfyra_snapshot_conflict_${randomUUID()
    .replaceAll('-', '')
    .slice(0, 12)}`;
  const migration: SchemaMigrationDef = {
    tables: [
      {
        _unique: { name: { _eq: 'posts' } },
        columnsToModify: [
          {
            from: { name: 'legacy_title' },
            to: { name: 'title' },
          },
        ],
      },
    ],
  };

  try {
    await client.connect();
    const db = client.db(databaseName);
    await db.collection('posts').insertMany([
      {
        _id: 1,
        legacy_title: 'legacy-value',
        title: 'new-value',
      },
      {
        _id: 2,
        legacy_title: 'copy-later',
      },
    ]);
    await db
      .collection('posts')
      .createIndex({ legacy_title: 1 }, { name: 'idx_legacy_title' });

    await assert.rejects(
      applyMongoSchemaMigrations(db, migration),
      /conflict/i,
    );
    assert.deepEqual(
      await db.collection('posts').find({}).sort({ _id: 1 }).toArray(),
      [
        {
          _id: 1,
          legacy_title: 'legacy-value',
          title: 'new-value',
        },
        {
          _id: 2,
          legacy_title: 'copy-later',
        },
      ],
    );

    await db
      .collection('posts')
      .updateOne({ _id: 1 }, { $set: { title: 'legacy-value' } });
    await applyMongoSchemaMigrations(db, migration);
    await applyMongoSchemaMigrations(db, migration);
    assert.deepEqual(
      await db.collection('posts').find({}).sort({ _id: 1 }).toArray(),
      [
        { _id: 1, title: 'legacy-value' },
        { _id: 2, title: 'copy-later' },
      ],
    );
    assert.deepEqual(
      (await db.collection('posts').listIndexes().toArray()).map(
        (index) => index.name,
      ),
      ['_id_', 'idx_title'],
    );
  } finally {
    await client.db(databaseName).dropDatabase();
    await client.close();
  }
}

async function runMongoBoot(port: number): Promise<void> {
  const mongoUser = required('MATRIX_MONGO_USER');
  const mongoPassword = required('MATRIX_MONGO_PASSWORD');
  const mongoHost = process.env.MATRIX_MONGO_HOST || '127.0.0.1';
  const mongoPort = Number(process.env.MATRIX_MONGO_PORT || 27017);
  const mongoAuthDatabase = process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin';
  const databaseName = `enfyra_snapshot_boot_${randomUUID()
    .replaceAll('-', '')
    .slice(0, 12)}`;
  const client = new MongoClient(
    buildDatabaseUri(
      'mongodb',
      {
        host: mongoHost,
        port: mongoPort,
        user: mongoUser,
        password: mongoPassword,
        database: databaseName,
      },
      mongoAuthDatabase,
    ),
  );
  let server: ChildProcess | null = null;

  try {
    await client.connect();
    const db = client.db(databaseName);
    server = await bootServer(
      buildDatabaseUri(
        'mongodb',
        {
          host: mongoHost,
          port: mongoPort,
          user: mongoUser,
          password: mongoPassword,
          database: databaseName,
        },
        mongoAuthDatabase,
      ),
      port,
    );
    const setting = await db.collection('enfyra_setting').findOne({});
    assert.equal(setting?.isInit, true);
    await stopServer(server);
    server = null;

    const settingTable = await db
      .collection('enfyra_table')
      .findOne({ name: 'enfyra_setting' });
    const columnTable = await db
      .collection('enfyra_table')
      .findOne({ name: 'enfyra_column' });
    assert.ok(settingTable?._id);
    assert.ok(columnTable?._id);
    const now = new Date();
    const baseColumn = {
      isPrimary: false,
      isGenerated: false,
      isNullable: true,
      isUpdatable: true,
      isPublished: true,
      isEncrypted: false,
      defaultValue: null,
      options: null,
      description: null,
      placeholder: null,
      createdAt: now,
      updatedAt: now,
    };
    await db.collection('enfyra_column').insertMany([
      {
        ...baseColumn,
        table: settingTable._id,
        name: 'userExtensionField',
        type: 'text',
        isSystem: false,
      },
      {
        ...baseColumn,
        table: columnTable._id,
        name: 'isHidden',
        type: 'boolean',
        isSystem: true,
      },
    ]);
    await db
      .collection('enfyra_column')
      .updateMany({}, { $set: { isHidden: true } });
    await db.collection('enfyra_setting').updateOne(
      {},
      {
        $set: {
          isInit: false,
          userExtensionField: 'preserved',
        },
      },
    );

    server = await bootServer(
      buildDatabaseUri(
        'mongodb',
        {
          host: mongoHost,
          port: mongoPort,
          user: mongoUser,
          password: mongoPassword,
          database: databaseName,
        },
        mongoAuthDatabase,
      ),
      port,
    );
    const retriedSetting = await db.collection('enfyra_setting').findOne({});
    assert.equal(retriedSetting?.isInit, true);
    assert.equal(retriedSetting?.userExtensionField, 'preserved');
    assert.equal(
      await db.collection('enfyra_column').countDocuments({
        table: settingTable._id,
        name: 'userExtensionField',
        isSystem: false,
      }),
      1,
    );
    assert.equal(
      await db.collection('enfyra_column').countDocuments({
        table: columnTable._id,
        name: 'isHidden',
      }),
      0,
    );
    assert.equal(
      await db
        .collection('enfyra_column')
        .countDocuments({ isHidden: { $exists: true } }),
      0,
    );
    await stopServer(server);
    server = null;

    const settingIndexes = await db
      .collection('enfyra_setting')
      .listIndexes()
      .toArray();
    const targetIndex = settingIndexes.find((index) => index.name !== '_id_');
    assert.ok(targetIndex?.name);
    await db.collection('enfyra_setting').dropIndex(targetIndex.name);
    await db
      .collection('enfyra_setting')
      .createIndex({ corruptedIndexField: 1 }, { name: targetIndex.name });
    await db
      .collection('enfyra_setting')
      .updateOne({}, { $set: { isInit: false } });
    await assert.rejects(
      bootServer(
        buildDatabaseUri(
          'mongodb',
          {
            host: mongoHost,
            port: mongoPort,
            user: mongoUser,
            password: mongoPassword,
            database: databaseName,
          },
          mongoAuthDatabase,
        ),
        port,
      ),
      /index|conflict/i,
    );
    assert.equal(
      (await db.collection('enfyra_setting').findOne({}))?.isInit,
      false,
    );
  } finally {
    if (server) await stopServer(server);
    await client.db(databaseName).dropDatabase();
    await client.close();
  }
}

async function main(): Promise<void> {
  await runSql('postgres');
  console.log('PostgreSQL snapshot migration E2E passed');
  await runSqlRenameConflict('postgres');
  console.log('PostgreSQL conflict and retry E2E passed');
  await runSqlColumnContract('postgres');
  console.log('PostgreSQL comprehensive column update E2E passed');
  await runSqlRelationContract('postgres');
  console.log('PostgreSQL comprehensive relation update E2E passed');
  await runSqlFailureGuards('postgres');
  console.log('PostgreSQL pre-mutation failure guards E2E passed');
  await runSqlPartialJunctionResume('postgres');
  console.log('PostgreSQL partial junction resume E2E passed');
  await runSqlBoot('postgres', 18105);
  console.log('PostgreSQL full bootstrap E2E passed');
  await runSql('mysql');
  console.log('MySQL snapshot migration E2E passed');
  await runSqlRenameConflict('mysql');
  console.log('MySQL conflict and retry E2E passed');
  await runSqlColumnContract('mysql');
  console.log('MySQL comprehensive column update E2E passed');
  await runSqlRelationContract('mysql');
  console.log('MySQL comprehensive relation update E2E passed');
  await runSqlFailureGuards('mysql');
  console.log('MySQL pre-mutation failure guards E2E passed');
  await runSqlPartialJunctionResume('mysql');
  console.log('MySQL partial junction resume E2E passed');
  await runSqlBoot('mysql', 18106);
  console.log('MySQL full bootstrap E2E passed');
  await runMongo();
  console.log('MongoDB snapshot migration E2E passed');
  await runMongoRenameConflict();
  console.log('MongoDB conflict and retry E2E passed');
  await runMongoBoot(18107);
  console.log('MongoDB full bootstrap E2E passed');
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
