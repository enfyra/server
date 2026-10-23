import type { Knex } from 'knex';
import { describe, expect, it, vi } from 'vitest';
import {
  compareSchemas,
  getCurrentDatabaseSchema,
} from '../../src/engines/knex/utils/provision/schema-comparison';
import type { KnexTableSchema } from '../../src/shared/types/database-init.types';

const schema: KnexTableSchema = {
  tableName: 'catalog_probe',
  definition: {
    name: 'catalog_probe',
    columns: [
      { name: 'id', type: 'int', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'tableId', type: 'int' },
    ],
    uniques: [['name', 'tableId']],
    indexes: [['tableId']],
    relations: [],
  },
  junctionTables: [],
};

describe('MySQL-compatible catalog flags', () => {
  it.each([
    [0, 1],
    ['0', '1'],
  ])(
    'retains catalog uniques and indexes for %j/%j',
    async (unique, nonUnique) => {
      const db = {
        client: { config: { client: 'mysql2' } },
        raw: vi
          .fn()
          .mockResolvedValueOnce([[]])
          .mockResolvedValueOnce([[]])
          .mockResolvedValueOnce([
            [
              {
                indexName: 'uq_catalog_probe_name_tableId',
                columns: 'name,tableId',
                isNonUnique: unique,
              },
              {
                indexName: 'idx_catalog_probe_tableId',
                columns: 'tableId,id',
                isNonUnique: nonUnique,
              },
              {
                indexName: 'idx_catalog_probe_createdAt',
                columns: 'createdAt,id',
                isNonUnique: nonUnique,
              },
              {
                indexName: 'idx_catalog_probe_updatedAt',
                columns: 'updatedAt,id',
                isNonUnique: nonUnique,
              },
            ],
          ]),
      } as unknown as Knex;
      const current = await getCurrentDatabaseSchema(db, schema.tableName);
      expect(current.uniques).toEqual([
        { name: 'uq_catalog_probe_name_tableId', columns: ['name', 'tableId'] },
      ]);
      expect(current.indexes).toHaveLength(3);
      const diff = compareSchemas(schema, current, 'mysql2');
      expect(diff.uniquesToAdd).toEqual([]);
      expect(diff.indexesToAdd).toEqual([]);
    },
  );
});
