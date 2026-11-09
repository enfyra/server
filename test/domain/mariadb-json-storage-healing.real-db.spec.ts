import { randomUUID } from 'node:crypto';
import { knex, type Knex } from 'knex';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SqlSchemaHealingService } from '../../src/engines/bootstrap/services/schema-healing/sql-schema-healing.service';

const uri = process.env.MARIADB_TEST_URI;

describe.skipIf(!uri)('MariaDB JSON storage healing', () => {
  const database = `enfyra_json_heal_${randomUUID().replaceAll('-', '')}`;
  let admin: Knex;
  let db: Knex;
  let created = false;

  beforeAll(async () => {
    const connection = new URL(uri!);
    if (
      !['127.0.0.1', 'localhost', '::1', '[::1]'].includes(connection.hostname)
    ) {
      throw new Error('MARIADB_TEST_URI must use a local database');
    }
    admin = knex({
      client: 'mysql2',
      connection: uri,
      pool: { min: 0, max: 1 },
    });
    await admin.raw('CREATE DATABASE ??', [database]);
    created = true;
    connection.pathname = `/${database}`;
    db = knex({
      client: 'mysql2',
      connection: connection.href,
      pool: { min: 0, max: 1 },
    });
    await db.raw(
      'CREATE TABLE enfyra_table (id INT PRIMARY KEY, name VARCHAR(255))',
    );
    await db.raw(
      'CREATE TABLE enfyra_column (tableId INT, name VARCHAR(255), type VARCHAR(255))',
    );
    await db.raw(
      'CREATE TABLE posts (id INT PRIMARY KEY, payload JSON NULL, CONSTRAINT payload_length CHECK (CHAR_LENGTH(payload) < 100))',
    );
    await db('enfyra_table').insert({ id: 1, name: 'posts' });
    await db('enfyra_column').insert({
      tableId: 1,
      name: 'payload',
      type: 'simple-json',
    });
    await db('posts').insert({
      id: 1,
      payload: JSON.stringify({ kept: 'yes' }),
    });
  });

  afterAll(async () => {
    await db?.destroy();
    if (created) await admin.raw('DROP DATABASE ??', [database]);
    await admin?.destroy();
  });

  it('converts the JSON alias to unconstrained text, preserves data and other checks, and is idempotent', async () => {
    const service = new SqlSchemaHealingService({
      queryBuilderService: {
        getKnex: () => db,
        getDatabaseType: () => 'mysql',
      } as never,
      metadataCacheService: {} as never,
      systemCoreTableResolver: {
        getNames: async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        }),
      } as never,
      log: () => {},
      warn: () => {},
    });
    const checks = async () =>
      (
        await db.raw(`
      SELECT tc.CONSTRAINT_NAME AS name, cc.CHECK_CLAUSE AS clause
      FROM information_schema.TABLE_CONSTRAINTS tc
      JOIN information_schema.CHECK_CONSTRAINTS cc
        ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
      WHERE tc.CONSTRAINT_SCHEMA = DATABASE() AND tc.TABLE_NAME = 'posts' AND tc.CONSTRAINT_TYPE = 'CHECK'
    `)
      )[0] as Array<{ name: string; clause: string }>;

    expect(
      (await checks()).some((check) => /json_valid/i.test(check.clause)),
    ).toBe(true);
    expect(await service.repairSqlJsonColumns()).toBe(1);
    const after = await checks();
    expect(after.some((check) => /json_valid/i.test(check.clause))).toBe(false);
    expect(after.some((check) => check.name === 'payload_length')).toBe(true);
    const value = (await db('posts').where({ id: 1 }).first('payload')).payload;
    expect(typeof value).toBe('string');
    expect(JSON.parse(value)).toEqual({ kept: 'yes' });
    await expect(
      db('posts').where({ id: 1 }).update({ payload: 'plain text' }),
    ).resolves.toBe(1);
    await expect(
      db('posts')
        .where({ id: 1 })
        .update({ payload: 'x'.repeat(100) }),
    ).rejects.toMatchObject({
      errno: 4025,
      sqlState: '23000',
      message: expect.stringContaining('payload_length'),
    });
    expect(await service.repairSqlJsonColumns()).toBe(0);
  });
});
