import { describe, expect, it, vi } from 'vitest';
import { applySqlColumnModifications } from '../../src/shared/utils/provision-schema-migration';
import { getCurrentDatabaseSchema } from '../../src/engines/knex/utils/provision/schema-comparison';
import { SqlSchemaHealingService } from '../../src/engines/bootstrap/services/schema-healing/sql-schema-healing.service';

vi.mock('../../src/shared/utils/provision-schema-migration', () => ({
  applySqlColumnModifications: vi.fn(),
}));
vi.mock('../../src/engines/knex/utils/provision/schema-comparison', () => ({
  getCurrentDatabaseSchema: vi.fn(),
}));

function service(physicalType: string, dbType = 'mysql') {
  const rows = [{ tableName: 'posts', columnName: 'payload' }];
  const knex: any = () => ({
    join: () => ({
      where: () => ({ select: async () => rows }),
    }),
  });
  knex.schema = { hasTable: async () => true, hasColumn: async () => true };
  knex.client = { config: { client: dbType === 'mysql' ? 'mysql2' : 'pg' } };
  vi.mocked(getCurrentDatabaseSchema).mockResolvedValue({
    columns: [
      {
        name: 'payload',
        type: physicalType,
        isNullable: true,
        defaultValue: null,
      },
    ],
  } as never);
  return new SqlSchemaHealingService({
    queryBuilderService: {
      getKnex: () => knex,
      getDatabaseType: () => dbType,
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
}

describe('SQL JSON storage healing', () => {
  it('promotes a metadata-declared JSON column from TEXT to large text', async () => {
    vi.mocked(applySqlColumnModifications).mockClear();
    await expect(service('text').repairSqlJsonColumns()).resolves.toBe(1);
    expect(applySqlColumnModifications).toHaveBeenCalledWith(
      expect.anything(),
      'posts',
      [expect.objectContaining({ to: { name: 'payload', type: 'simple-json' } })],
      'mysql2',
    );
  });

  it('leaves an already large-text column alone', async () => {
    vi.mocked(applySqlColumnModifications).mockClear();
    await expect(service('longtext').repairSqlJsonColumns()).resolves.toBe(0);
    expect(applySqlColumnModifications).not.toHaveBeenCalled();
  });

  it('converges a legacy native JSON column onto the text contract', async () => {
    vi.mocked(applySqlColumnModifications).mockClear();
    await expect(service('json').repairSqlJsonColumns()).resolves.toBe(1);
    expect(applySqlColumnModifications).toHaveBeenCalledWith(
      expect.anything(),
      'posts',
      [expect.objectContaining({ from: expect.objectContaining({ type: 'json' }) })],
      'mysql2',
    );
  });

  it('does nothing on PostgreSQL, where text is already unbounded', async () => {
    vi.mocked(applySqlColumnModifications).mockClear();
    await expect(service('text', 'postgres').repairSqlJsonColumns()).resolves.toBe(0);
    expect(applySqlColumnModifications).not.toHaveBeenCalled();
  });
});
