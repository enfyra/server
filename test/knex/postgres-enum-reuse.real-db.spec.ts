import { randomUUID } from 'node:crypto';
import knex, { type Knex } from 'knex';
import { beforeAll, beforeEach, afterEach, describe, expect, it } from 'vitest';
import { createTable } from '../../src/engines/knex/utils/provision/table-builder';
import { syncTable } from '../../src/engines/knex/utils/provision/sync-table';
import { getPostgresEnumTypeName } from '../../src/engines/knex/utils/sql-enum.util';
import { SqlSchemaMigrationService } from '../../src/engines/knex/services/sql-schema-migration.service';
import { SystemMetadataHealingService } from '../../src/engines/bootstrap/services/schema-healing/system-metadata-healing.service';
import type { KnexTableSchema } from '../../src/shared/types/database-init.types';

const tableName = 'enfyra_auth_header';
const typeName = getPostgresEnumTypeName(tableName, 'scheme');
const schema: KnexTableSchema = {
  tableName,
  definition: {
    name: tableName,
    columns: [
      {
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
      },
      {
        name: 'scheme',
        type: 'enum',
        options: ['raw', 'bearer'],
        isNullable: false,
        defaultValue: 'raw',
      },
    ],
    relations: [],
  },
  junctionTables: [],
};

async function enumState(db: Knex) {
  const result = await db.raw(
    `SELECT types.oid::text AS oid, types.typtype AS kind,
            array_agg(labels.enumlabel::text ORDER BY labels.enumsortorder) AS labels
       FROM pg_type types
       JOIN pg_namespace namespace ON namespace.oid = types.typnamespace
       LEFT JOIN pg_enum labels ON labels.enumtypid = types.oid
      WHERE namespace.nspname = current_schema() AND types.typname = ?
      GROUP BY types.oid`,
    [typeName],
  );
  return result.rows[0];
}

describe('PostgreSQL retained enum creation boundaries', () => {
  let connection: string;
  let namespace: string;
  let admin: Knex;
  let db: Knex;
  let created: boolean;

  beforeAll(() => {
    connection = process.env.POSTGRES_TEST_URI!;
    if (!connection) throw new Error('POSTGRES_TEST_URI is required');
    if (!['localhost', '127.0.0.1'].includes(new URL(connection).hostname)) {
      throw new Error('POSTGRES_TEST_URI must select a local database');
    }
  });

  beforeEach(async () => {
    namespace = `enum_reuse_${randomUUID().replaceAll('-', '')}`;
    created = false;
    admin = knex({ client: 'pg', connection, pool: { min: 0, max: 1 } });
    db = knex({
      client: 'pg',
      connection,
      searchPath: [namespace],
      pool: { min: 0, max: 1 },
      acquireConnectionTimeout: 5000,
    });
    await admin.raw('CREATE SCHEMA ??', [namespace]);
    created = true;
    await db.raw('SET statement_timeout = 5000');
  });

  afterEach(async () => {
    try {
      if (db) await db.destroy();
      if (created) await admin.raw('DROP SCHEMA ?? CASCADE', [namespace]);
    } finally {
      if (admin) await admin.destroy();
    }
  });

  it('reuses a retained matching enum during table creation without replacing it', async () => {
    await db.raw("CREATE TYPE ?? AS ENUM ('raw', 'bearer')", [typeName]);
    await db.schema.createTable('retained_enum_consumer', (table) =>
      table.specificType('scheme', `"${typeName}"`),
    );
    await db('retained_enum_consumer').insert({ scheme: 'bearer' });
    const before = await enumState(db);

    await db.transaction((tx) => createTable(tx, schema, 'postgres', [schema]));

    expect(await enumState(db)).toEqual(before);
    expect(await db('retained_enum_consumer').pluck('scheme')).toEqual([
      'bearer',
    ]);
    await db(tableName).insert({});
    await db(tableName).insert({ scheme: 'bearer' });
    expect(await db(tableName).orderBy('id').pluck('scheme')).toEqual([
      'raw',
      'bearer',
    ]);
    await expect(
      db(tableName).insert({ scheme: 'unknown' }),
    ).rejects.toMatchObject({ code: '22P02' });
    await expect(db(tableName).insert({ scheme: null })).rejects.toMatchObject({
      code: '23502',
    });
  });

  it('reuses a retained enum when additive sync restores a missing column', async () => {
    await db.raw("CREATE TYPE ?? AS ENUM ('raw', 'bearer')", [typeName]);
    await db.schema.createTable(tableName, (table) => {
      table.increments('id');
      table.timestamp('createdAt');
      table.timestamp('updatedAt');
    });
    await db(tableName).insert({ id: 7 });
    const before = await enumState(db);

    await db.transaction((tx) =>
      syncTable(tx, schema, [schema], { additiveOnly: true }),
    );
    await db.transaction((tx) =>
      syncTable(tx, schema, [schema], { additiveOnly: true }),
    );

    expect(await enumState(db)).toEqual(before);
    expect(await db(tableName).select('id', 'scheme')).toEqual([
      { id: 7, scheme: 'raw' },
    ]);
  });

  it.each([
    { label: 'different labels', labels: ['raw', 'other'] },
    { label: 'different label order', labels: ['bearer', 'raw'] },
  ])(
    'rejects a retained type with $label before creating a table',
    async ({ labels }) => {
      const values = labels
        .map((value) => `'${value.replace(/'/g, "''")}'`)
        .join(', ');
      await db.raw(`CREATE TYPE ?? AS ENUM (${values})`, [typeName]);
      const before = await enumState(db);

      await expect(
        createTable(db, schema, 'postgres', [schema]),
      ).rejects.toThrow(/already exists with incompatible options/);

      expect(await db.schema.hasTable(tableName)).toBe(false);
      expect(await enumState(db)).toEqual(before);
    },
  );

  it('rejects a non-enum type with the deterministic name', async () => {
    await db.raw('CREATE DOMAIN ?? AS text', [typeName]);
    const before = await enumState(db);

    await expect(createTable(db, schema, 'postgres', [schema])).rejects.toThrow(
      /already exists with incompatible options/,
    );

    expect(await db.schema.hasTable(tableName)).toBe(false);
    expect(await enumState(db)).toEqual(before);
  });

  it('reuses a retained matching enum through the runtime table creator', async () => {
    await db.raw("CREATE TYPE ?? AS ENUM ('raw', 'bearer')", [typeName]);
    const before = await enumState(db);
    const service = new SqlSchemaMigrationService({
      knexService: { getKnex: () => db },
      queryBuilderService: { getDatabaseType: () => 'postgres' },
    } as unknown as ConstructorParameters<typeof SqlSchemaMigrationService>[0]);

    await service.createTable(schema.definition);
    await service.createTable(schema.definition);

    expect(await enumState(db)).toEqual(before);
    await db(tableName).insert({});
    expect(await db(tableName).pluck('scheme')).toEqual(['raw']);
  });

  it.each(['snapshot', 'persisted'] as const)(
    'reuses a retained enum through %s missing-column healing',
    async (source) => {
      await db.raw("CREATE TYPE ?? AS ENUM ('raw', 'bearer')", [typeName]);
      await db.schema.createTable(tableName, (table) => table.increments('id'));
      await db(tableName).insert({ id: 7 });
      const before = await enumState(db);
      const service = new SystemMetadataHealingService({
        queryBuilderService: {
          getKnex: () => db,
          getDatabaseType: () => 'postgres',
        },
        systemCoreTableResolver: {
          getNames: async () => ({
            table: 'enfyra_table',
            column: 'enfyra_column',
          }),
        },
      } as unknown as ConstructorParameters<
        typeof SystemMetadataHealingService
      >[0]);
      const snapshot = {
        [tableName]: { ...schema.definition, isSystem: true },
      };
      if (source === 'persisted') {
        await db.schema.createTable('enfyra_table', (table) => {
          table.increments('id');
          table.string('name');
          table.boolean('isSystem');
        });
        await db.schema.createTable('enfyra_column', (table) => {
          table.integer('tableId');
          table.string('name');
          table.string('type');
          table.text('options');
          table.boolean('isNullable');
          table.string('defaultValue');
        });
        await db('enfyra_table').insert({
          id: 1,
          name: tableName,
          isSystem: true,
        });
        await db('enfyra_column').insert({
          tableId: 1,
          name: 'scheme',
          type: 'enum',
          options: JSON.stringify({ values: ['raw', 'bearer'] }),
          isNullable: false,
          defaultValue: 'raw',
        });
      }
      const repair = () =>
        source === 'snapshot'
          ? service.repairSqlSystemPhysicalColumnsFromSnapshot(snapshot)
          : service.repairSqlSystemPhysicalColumns();

      expect(await repair()).toBe(1);
      expect(await repair()).toBe(0);
      expect(await enumState(db)).toEqual(before);
      expect(await db(tableName).select('id', 'scheme')).toEqual([
        { id: 7, scheme: 'raw' },
      ]);
    },
  );

  it('preflights every retained enum before creating another type or adding columns', async () => {
    await db.raw("CREATE TYPE ?? AS ENUM ('raw', 'wrong')", [typeName]);
    await db.schema.createTable(tableName, (table) => table.increments('id'));
    await db(tableName).insert({ id: 9 });
    const conflicting = structuredClone(schema);
    conflicting.definition.columns.splice(1, 0, {
      name: 'other',
      type: 'enum',
      options: ['new'],
      isNullable: true,
    });
    const before = await enumState(db);

    await expect(
      syncTable(db, conflicting, [conflicting], { additiveOnly: true }),
    ).rejects.toThrow(/incompatible options/);

    expect(await db.schema.hasColumn(tableName, 'other')).toBe(false);
    expect(await db.schema.hasColumn(tableName, 'scheme')).toBe(false);
    expect(
      await db.raw('SELECT to_regtype(?) AS type', [
        getPostgresEnumTypeName(tableName, 'other'),
      ]),
    ).toMatchObject({ rows: [{ type: null }] });
    expect(await enumState(db)).toEqual(before);
    expect(await db(tableName).pluck('id')).toEqual([9]);
  });

  it('creates a missing enum inside the owning transaction and rolls it back with the table', async () => {
    await expect(
      db.transaction(async (tx) => {
        await createTable(tx, schema, 'postgres', [schema]);
        expect((await enumState(tx)).labels).toEqual(['raw', 'bearer']);
        throw new Error('rollback fixture');
      }),
    ).rejects.toThrow('rollback fixture');

    expect(await enumState(db)).toBeUndefined();
    expect(await db.schema.hasTable(tableName)).toBe(false);
    await db.transaction((tx) => createTable(tx, schema, 'postgres', [schema]));
    expect((await enumState(db)).labels).toEqual(['raw', 'bearer']);
  });
});
