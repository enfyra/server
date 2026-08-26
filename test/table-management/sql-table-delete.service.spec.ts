import { describe, expect, it, vi } from 'vitest';
import { SqlTableDeleteService } from '../../src/modules/table-management/services/sql-table-delete.service';

describe('SqlTableDeleteService', () => {
  it('keeps metadata intact when physical schema cleanup fails', async () => {
    const metadataDeletes = {
      table: vi.fn(),
      relation: vi.fn(),
    };
    const tableQuery = {
      where: vi.fn(() => tableQuery),
      first: vi.fn().mockResolvedValue({ id: 7, name: 'trial_table' }),
      delete: metadataDeletes.table,
    };
    const relationQuery = {
      where: vi.fn(() => relationQuery),
      orWhere: vi.fn(() => relationQuery),
      select: vi.fn().mockResolvedValue([]),
      delete: metadataDeletes.relation,
    };
    const trx = Object.assign(
      (table: string) =>
        table === 'enfyra_table' ? tableQuery : relationQuery,
      {
        raw: vi.fn().mockResolvedValue({ rows: [] }),
        schema: { alterTable: vi.fn() },
        isCompleted: vi.fn().mockReturnValue(false),
        rollback: vi.fn().mockResolvedValue(undefined),
      },
    );
    const knex = {
      transaction: vi.fn(async (callback) => callback(trx)),
    };
    const service = new SqlTableDeleteService({
      queryBuilderService: {
        getKnex: () => knex,
        getDatabaseType: () => 'postgres',
      },
      sqlSchemaMigrationService: {
        dropTable: vi
          .fn()
          .mockRejectedValue(new Error('physical cleanup failed')),
      },
      metadataCacheService: {},
      runtimeRegistryService: {},
      loggingService: { error: vi.fn() },
      schemaMigrationLockService: {},
      policyService: {
        checkSchemaMigration: vi.fn().mockResolvedValue({ allow: true }),
      },
      tableManagementValidationService: {},
      sqlTableMetadataBuilderService: {},
      sqlTableMetadataWriterService: {},
    } as any);
    vi.spyOn(service as any, 'runWithSchemaLock').mockImplementation(
      async (_context, callback) => callback(),
    );

    await expect(service.delete(7)).rejects.toThrow('physical cleanup failed');

    expect(metadataDeletes.relation).not.toHaveBeenCalled();
    expect(metadataDeletes.table).not.toHaveBeenCalled();
  });

  it('removes incoming MySQL foreign keys and their columns before metadata', async () => {
    const events: string[] = [];
    const tableQuery = {
      where: vi.fn(() => tableQuery),
      first: vi.fn().mockResolvedValue({ id: 7, name: 'trial_table' }),
      delete: vi.fn(() => events.push('metadata-table')),
    };
    const relationQuery = {
      where: vi.fn(() => relationQuery),
      orWhere: vi.fn(() => relationQuery),
      select: vi.fn().mockResolvedValue([]),
      delete: vi.fn(() => events.push('metadata-relation')),
    };
    const trx = Object.assign(
      (table: string) =>
        table === 'enfyra_table' ? tableQuery : relationQuery,
      {
        raw: vi.fn(async (sql: string) => {
          if (sql.includes('INFORMATION_SCHEMA.KEY_COLUMN_USAGE')) {
            return [
              [
                {
                  table_name: 'child_table',
                  constraint_name: 'fk_child_parent',
                  column_name: 'parentId',
                },
              ],
            ];
          }
          events.push('foreign-key');
          return [[]];
        }),
        schema: {
          alterTable: vi.fn(async (_tableName, callback) => {
            callback({
              dropColumn: vi.fn(() => events.push('foreign-key-column')),
            });
          }),
        },
        isCompleted: vi.fn().mockReturnValue(false),
        rollback: vi.fn().mockResolvedValue(undefined),
      },
    );
    const service = new SqlTableDeleteService({
      queryBuilderService: {
        getKnex: () => ({
          transaction: async (callback: any) => callback(trx),
        }),
        getDatabaseType: () => 'mysql',
      },
      sqlSchemaMigrationService: {
        dropTable: vi.fn(async () => events.push('drop-table')),
      },
      metadataCacheService: {},
      runtimeRegistryService: {},
      loggingService: { error: vi.fn() },
      schemaMigrationLockService: {},
      policyService: {
        checkSchemaMigration: vi.fn().mockResolvedValue({ allow: true }),
      },
      tableManagementValidationService: {},
      sqlTableMetadataBuilderService: {},
      sqlTableMetadataWriterService: {},
    } as any);
    vi.spyOn(service as any, 'runWithSchemaLock').mockImplementation(
      async (_context, callback) => callback(),
    );

    await service.delete(7);

    expect(trx.raw).toHaveBeenCalledWith(
      'ALTER TABLE `child_table` DROP FOREIGN KEY `fk_child_parent`',
    );
    expect(events).toEqual([
      'foreign-key',
      'foreign-key-column',
      'drop-table',
      'metadata-relation',
      'metadata-table',
    ]);
  });
});
