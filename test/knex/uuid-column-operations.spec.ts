import { describe, expect, it, vi } from 'vitest';
import { SqlSchemaMigrationService } from '../../src/engines/knex/services/sql-schema-migration.service';
import { addColumnToTable } from '../../src/engines/knex/utils/migration/column-operations';
import { isTypeCompatible } from '../../src/engines/knex/utils/provision/schema-comparison';

function createTableBuilder() {
  const column = {
    primary: vi.fn(),
    notNullable: vi.fn(),
    nullable: vi.fn(),
    defaultTo: vi.fn(),
  };
  return {
    column,
    table: {
      uuid: vi.fn(() => column),
      string: vi.fn(() => column),
    } as any,
  };
}

describe('SQL UUID physical mapping', () => {
  it('creates custom UUID columns as native UUID on PostgreSQL', () => {
    const { table } = createTableBuilder();

    addColumnToTable(table, { name: 'externalId', type: 'uuid' }, 'postgres');

    expect(table.uuid).toHaveBeenCalledWith('externalId');
    expect(table.string).not.toHaveBeenCalled();
  });

  it('creates custom UUID columns as Knex UUIDs on MySQL', () => {
    const { table } = createTableBuilder();

    addColumnToTable(table, { name: 'externalId', type: 'uuid' }, 'mysql');

    expect(table.uuid).toHaveBeenCalledWith('externalId');
    expect(table.string).not.toHaveBeenCalled();
  });

  it('accepts the MySQL UUID representation without accepting it on PostgreSQL', () => {
    expect(isTypeCompatible('uuid', 'varchar', 'mysql2')).toBe(true);
    expect(isTypeCompatible('uuid', 'varchar', 'pg')).toBe(false);
  });

  it('classifies a MySQL CHAR(36) primary key as UUID for runtime foreign keys', async () => {
    const service = new SqlSchemaMigrationService({
      knexService: {
        getKnex: () => ({
          raw: vi.fn().mockResolvedValue([
            [
              {
                DATA_TYPE: 'char',
                COLUMN_TYPE: 'char(36)',
                CHARACTER_MAXIMUM_LENGTH: 36,
              },
            ],
          ]),
        }),
      },
      metadataCacheService: { lookupTableByName: vi.fn() },
      queryBuilderService: { getDatabaseType: () => 'mysql' },
      migrationJournalService: {},
      sqlSchemaDiffService: {},
    } as any);

    await expect((service as any).getPrimaryKeyType('accounts')).resolves.toBe(
      'uuid',
    );
  });

  it('creates runtime UUID foreign keys with Knex UUID mapping', () => {
    const column = {};
    const table = {
      uuid: vi.fn(() => column),
      string: vi.fn(() => column),
    };
    const service = new SqlSchemaMigrationService({} as any);

    expect(
      (service as any).createFKColumn(table, 'accountId', 'uuid'),
    ).toBe(column);
    expect(table.uuid).toHaveBeenCalledWith('accountId');
    expect(table.string).not.toHaveBeenCalled();
  });
});
