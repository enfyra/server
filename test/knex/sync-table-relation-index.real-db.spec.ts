import { randomUUID } from 'node:crypto';
import knex from 'knex';
import { describe, expect, it } from 'vitest';
import type { KnexTableSchema } from '../../src/shared/types/database-init.types';
import { syncTable } from '../../src/engines/knex/utils/provision/sync-table';
import {
  compareSchemas,
  getCurrentDatabaseSchema,
} from '../../src/engines/knex/utils/provision/schema-comparison';

describe('PostgreSQL relation upgrade on a real database', () => {
  it.each(['many-to-one', 'one-to-one'] as const)(
    'adds %s and converges on a second sync without losing existing data',
    async (type) => {
      const connection = process.env.POSTGRES_TEST_URI;
      if (!connection) throw new Error('POSTGRES_TEST_URI is required');
      const schemaName = `relation_index_test_${randomUUID().replaceAll('-', '')}`;
      const admin = knex({
        client: 'pg',
        connection,
        pool: { min: 0, max: 1 },
      });
      const db = knex({
        client: 'pg',
        connection,
        searchPath: [schemaName],
        pool: { min: 0, max: 1 },
      });
      let created = false;
      try {
        await admin.raw('create schema ??', [schemaName]);
        created = true;
        await db.schema.createTable('enfyra_menu', (table) => {
          table.increments('id');
        });
        await db.schema.createTable('enfyra_setting', (table) => {
          table.increments('id');
          table.string('enfyraVersion').notNullable();
          table.timestamp('createdAt');
          table.timestamp('updatedAt');
        });
        await db('enfyra_menu').insert({ id: 1 });
        await db('enfyra_setting').insert({ id: 1, enfyraVersion: '2.3.1' });
        const setting: KnexTableSchema = {
          tableName: 'enfyra_setting',
          definition: {
            name: 'enfyra_setting',
            columns: [
              { name: 'id', type: 'integer', isPrimary: true },
              { name: 'enfyraVersion', type: 'string', isNullable: false },
            ],
            relations: [
              {
                propertyName: 'defaultPage',
                type,
                targetTable: 'enfyra_menu',
                isNullable: true,
                onDelete: 'RESTRICT',
              },
            ],
          },
          junctionTables: [],
        };
        const menu: KnexTableSchema = {
          tableName: 'enfyra_menu',
          definition: {
            name: 'enfyra_menu',
            columns: [{ name: 'id', type: 'integer', isPrimary: true }],
          },
          junctionTables: [],
        };
        await db.transaction(async (tx) => {
          await syncTable(tx, setting, [setting, menu], { additiveOnly: true });
        });
        await db('enfyra_setting')
          .where({ id: 1 })
          .update({ defaultPageId: 1 });
        const beforeRetry = await getCurrentDatabaseSchema(
          db,
          setting.tableName,
        );
        expect(beforeRetry.foreignKeys).toEqual([
          {
            column: 'defaultPageId',
            references: 'id',
            referencesTable: 'enfyra_menu',
          },
        ]);
        const relationIndexes = beforeRetry.indexes.filter((index) =>
          index.columns.includes('defaultPageId'),
        );
        expect(relationIndexes).toEqual(
          type === 'many-to-one'
            ? [
                {
                  name: 'idx_enfyra_setting_defaultPageId',
                  columns: ['defaultPageId', 'id'],
                },
              ]
            : [],
        );
        expect(beforeRetry.uniques).toEqual(
          type === 'one-to-one'
            ? [
                {
                  name: 'uq_enfyra_setting_defaultPageId',
                  columns: ['defaultPageId'],
                },
              ]
            : [],
        );
        await db.transaction(async (tx) => {
          await syncTable(tx, setting, [setting, menu], { additiveOnly: true });
        });
        const afterRetry = await getCurrentDatabaseSchema(
          db,
          setting.tableName,
        );
        expect(afterRetry).toEqual(beforeRetry);
        expect(
          Object.values(compareSchemas(setting, afterRetry, 'pg')).flat(),
        ).toEqual([]);
        expect(
          await db('enfyra_setting').select(
            'id',
            'enfyraVersion',
            'defaultPageId',
          ),
        ).toEqual([{ id: 1, enfyraVersion: '2.3.1', defaultPageId: 1 }]);
        await expect(
          db('enfyra_menu').where({ id: 1 }).delete(),
        ).rejects.toMatchObject({ code: '23001' });
      } finally {
        await db.destroy();
        if (created) await admin.raw('drop schema ?? cascade', [schemaName]);
        await admin.destroy();
      }
    },
    20000,
  );
});
