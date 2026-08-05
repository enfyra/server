import { describe, expect, it, vi } from 'vitest';
import { SqlSchemaHealingService } from '../../src/engines/bootstrap/services/schema-healing/sql-schema-healing.service';
import { getSqlJunctionPhysicalNames } from '../../src/modules/table-management/utils/sql-junction-naming.util';

describe('SqlSchemaHealingService junction overlap healing', () => {
  it('merges rows into the canonical junction and removes the legacy table', async () => {
    const sourceTable = 'enfyra_field_permission';
    const targetTable = 'enfyra_user';
    const propertyName = 'allowedUsers';
    const canonical = getSqlJunctionPhysicalNames({
      sourceTable,
      propertyName,
      targetTable,
    });
    const legacyTable = `${sourceTable}_${propertyName}_${targetTable}`;
    const legacyRow = {
      [`${sourceTable}Id`]: 12,
      [`${targetTable}Id`]: 34,
    };
    const relation = {
      id: 'relation-1',
      type: 'many-to-many',
      mappedById: null,
      sourceTableName: sourceTable,
      targetTableName: targetTable,
      propertyName,
      junctionTableName: legacyTable,
      junctionSourceColumn: `${sourceTable}Id`,
      junctionTargetColumn: `${targetTable}Id`,
    };
    const insertIgnore = vi.fn().mockResolvedValue(undefined);
    const insert = vi.fn().mockReturnValue({
      onConflict: vi.fn().mockReturnValue({ ignore: insertIgnore }),
    });
    const dropTable = vi.fn().mockResolvedValue(undefined);
    const hasTable = vi
      .fn()
      .mockImplementation(async (tableName: string) =>
        [
          sourceTable,
          targetTable,
          canonical.junctionTableName,
          legacyTable,
        ].includes(tableName),
      );
    const hasColumn = vi
      .fn()
      .mockImplementation(
        async (tableName: string, columnName: string) =>
          tableName === canonical.junctionTableName &&
          [
            canonical.junctionSourceColumn,
            canonical.junctionTargetColumn,
          ].includes(columnName),
      );
    const metadataQuery = {
      leftJoin: vi.fn().mockReturnThis(),
      select: vi.fn().mockResolvedValue([relation]),
    };
    const knex: any = vi.fn((tableName: string) => {
      if (tableName === 'enfyra_relation as r') return metadataQuery;
      if (tableName === legacyTable) {
        return {
          columnInfo: vi.fn().mockResolvedValue({
            [`${sourceTable}Id`]: {},
            [`${targetTable}Id`]: {},
          }),
          select: vi.fn().mockResolvedValue([legacyRow]),
        };
      }
      if (tableName === canonical.junctionTableName) return { insert };
      return { where: vi.fn().mockReturnValue({ update: vi.fn() }) };
    });
    knex.schema = {
      hasTable,
      hasColumn,
      dropTable,
      alterTable: vi.fn(),
    };

    const service = new SqlSchemaHealingService({
      queryBuilderService: {
        getKnex: vi.fn().mockReturnValue(knex),
      } as any,
      metadataCacheService: {} as any,
      systemCoreTableResolver: {
        getNames: vi.fn().mockResolvedValue({
          relation: 'enfyra_relation',
          table: 'enfyra_table',
        }),
      } as any,
      log: vi.fn(),
      warn: vi.fn(),
    });

    await service.healSqlJunctionContracts({});

    expect(insert).toHaveBeenCalledWith([
      {
        sourceId: legacyRow[`${sourceTable}Id`],
        targetId: legacyRow[`${targetTable}Id`],
      },
    ]);
    expect(insertIgnore).toHaveBeenCalledOnce();
    expect(dropTable).toHaveBeenCalledWith(legacyTable);
  });
});
