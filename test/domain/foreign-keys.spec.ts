import { beforeEach, describe, expect, it, vi } from 'vitest';
import { addForeignKeys } from '../../src/engines/knex/utils/provision/foreign-keys';
import { buildSqlForeignKeyContracts } from '../../src/engines/knex/utils/sql-physical-schema-contract';

vi.mock('../../src/engines/knex/utils/sql-physical-schema-contract', () => ({
  buildSqlForeignKeyContracts: vi.fn(),
}));

const foreignKeyContract = {
  tableName: 'enfyra_guard',
  columnName: 'tableId',
  targetTable: 'enfyra_table',
  targetColumn: 'id',
  constraintName: 'enfyra_guard_tableId_foreign',
  onDelete: 'CASCADE',
};

function makeKnex(options: {
  existingConstraints?: string[];
  hasColumn?: boolean;
}) {
  return {
    client: { config: { client: 'pg' } },
    raw: vi.fn().mockResolvedValue({
      rows: (options.existingConstraints ?? []).map((name) => ({ name })),
    }),
    schema: {
      hasColumn: vi.fn().mockResolvedValue(options.hasColumn ?? true),
      alterTable: vi.fn(),
    },
  };
}

describe('addForeignKeys', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'log').mockImplementation(() => undefined);
    vi.mocked(buildSqlForeignKeyContracts).mockReturnValue([
      foreignKeyContract as any,
    ]);
  });

  it('deduplicates PostgreSQL constraint names case-insensitively', async () => {
    const knex = makeKnex({
      existingConstraints: ['enfyra_guard_tableid_foreign'],
    });

    await addForeignKeys(
      knex as any,
      [{ tableName: 'enfyra_guard', definition: { relations: [{}] } } as any],
      'postgres',
    );

    expect(knex.schema.hasColumn).not.toHaveBeenCalled();
    expect(knex.schema.alterTable).not.toHaveBeenCalled();
  });

  it('defers a foreign key whose relation column is not created yet', async () => {
    const knex = makeKnex({ hasColumn: false });

    await addForeignKeys(
      knex as any,
      [{ tableName: 'enfyra_guard', definition: { relations: [{}] } } as any],
      'postgres',
    );

    expect(knex.schema.hasColumn).toHaveBeenCalledWith(
      'enfyra_guard',
      'tableId',
    );
    expect(knex.schema.alterTable).not.toHaveBeenCalled();
  });
});
