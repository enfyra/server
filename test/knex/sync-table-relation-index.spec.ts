import type { Knex } from 'knex';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  KnexTableSchema,
  RelationDef,
} from '../../src/shared/types/database-init.types';
import { compareSchemas } from '../../src/engines/knex/utils/provision/schema-comparison';
import {
  applyIndexAndUniqueMigrations,
  applyRelationMigrations,
} from '../../src/engines/knex/utils/provision/sync-table';

function makeSchemaExecutor(client: string) {
  const indexes = new Set<string>();
  const column = {
    unsigned: vi.fn().mockReturnThis(),
    nullable: vi.fn().mockReturnThis(),
    notNullable: vi.fn().mockReturnThis(),
  };
  const foreignKey = {
    references: vi.fn().mockReturnThis(),
    inTable: vi.fn().mockReturnThis(),
    onDelete: vi.fn().mockReturnThis(),
    onUpdate: vi.fn().mockReturnThis(),
  };
  const table = {
    integer: vi.fn().mockReturnValue(column),
    foreign: vi.fn().mockReturnValue(foreignKey),
    unique: vi.fn(),
    index: vi.fn((_columns: string[], name: string) => {
      if (indexes.has(name))
        throw new Error(`relation "${name}" already exists`);
      indexes.add(name);
    }),
  };
  const knex = {
    client: { config: { client } },
    schema: {
      hasColumn: vi.fn().mockResolvedValue(false),
      alterTable: vi.fn(
        async (_name: string, build: (builder: typeof table) => void) => {
          build(table);
        },
      ),
    },
  } as unknown as Knex;
  return { knex, table, foreignKey };
}

afterEach(() => vi.restoreAllMocks());

describe.each(['pg', 'mysql2'])('relation index sync (%s)', (client) => {
  it.each(['many-to-one', 'one-to-one'] as const)(
    'adds a %s relation through one canonical constraint phase',
    async (type) => {
      vi.spyOn(console, 'log').mockImplementation(() => {});
      const relation: RelationDef = {
        propertyName: 'defaultPage',
        type,
        targetTable: 'enfyra_menu',
        isNullable: true,
        onDelete: 'RESTRICT',
      };
      const schema: KnexTableSchema = {
        tableName: 'enfyra_setting',
        definition: {
          name: 'enfyra_setting',
          columns: [{ name: 'id', type: 'integer', isPrimary: true }],
          relations: [relation],
        },
        junctionTables: [],
      };
      const target: KnexTableSchema = {
        tableName: 'enfyra_menu',
        definition: {
          name: 'enfyra_menu',
          columns: [{ name: 'id', type: 'integer', isPrimary: true }],
        },
        junctionTables: [],
      };
      const diff = compareSchemas(
        schema,
        {
          columns: [],
          foreignKeys: [],
          uniques: [],
          indexes: [
            { columns: ['createdAt', 'id'] },
            { columns: ['updatedAt', 'id'] },
          ],
        },
        client,
      );
      expect(diff.relationsToAdd).toEqual([relation]);
      const { knex, table, foreignKey } = makeSchemaExecutor(client);

      await applyRelationMigrations(knex, schema.tableName, diff, [
        schema,
        target,
      ]);
      await applyIndexAndUniqueMigrations(
        knex,
        schema.tableName,
        diff,
        schema.definition,
      );

      expect(table.integer).toHaveBeenCalledWith('defaultPageId');
      expect(foreignKey.onDelete).toHaveBeenCalledWith('RESTRICT');
      expect(foreignKey.onUpdate).toHaveBeenCalledWith('CASCADE');
      if (type === 'many-to-one') {
        expect(diff.indexesToAdd).toEqual([['defaultPageId', 'id']]);
        expect(table.index).toHaveBeenCalledExactlyOnceWith(
          ['defaultPageId', 'id'],
          'idx_enfyra_setting_defaultPageId',
        );
        expect(table.unique).not.toHaveBeenCalled();
      } else {
        expect(diff.indexesToAdd).toEqual([]);
        expect(table.index).not.toHaveBeenCalled();
        expect(table.unique).toHaveBeenCalledExactlyOnceWith(
          ['defaultPageId'],
          'uq_enfyra_setting_defaultPageId',
        );
      }
    },
  );
});
