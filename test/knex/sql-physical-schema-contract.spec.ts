import { describe, expect, it } from 'vitest';
import {
  buildSqlForeignKeyContracts,
  buildSqlIndexContracts,
  buildSqlJunctionTableContract,
  buildSqlUniqueContracts,
  resolveSqlRelationOnDelete,
} from '../../src/engines/knex';

describe('SQL physical schema contract', () => {
  const table = {
    name: 'route_definition',
    columns: [
      { name: 'id', type: 'int', isPrimary: true },
      { name: 'mainTableId', type: 'int' },
      { name: 'scheduledAt', type: 'datetime' },
    ],
    relations: [
      {
        propertyName: 'mainTable',
        type: 'many-to-one',
        targetTable: 'table_definition',
        onDelete: 'CASCADE',
        isNullable: false,
      },
    ],
    uniques: [['mainTable']],
    indexes: [['mainTable'], ['scheduledAt']],
  } as any;

  it('uses explicit onDelete before nullable fallback', () => {
    expect(
      resolveSqlRelationOnDelete({ onDelete: 'CASCADE', isNullable: false }),
    ).toBe('CASCADE');
    expect(resolveSqlRelationOnDelete({ isNullable: false })).toBe('RESTRICT');
    expect(resolveSqlRelationOnDelete({ isNullable: true })).toBe('SET NULL');
  });

  it('resolves relation FKs and unique groups to physical columns', () => {
    expect(buildSqlForeignKeyContracts('route_definition', table.relations)).toEqual([
      {
        tableName: 'route_definition',
        propertyName: 'mainTable',
        columnName: 'mainTableId',
        targetTable: 'table_definition',
        targetColumn: 'id',
        onDelete: 'CASCADE',
        onUpdate: 'CASCADE',
        nullable: false,
      },
    ]);

    expect(buildSqlUniqueContracts('route_definition', table)).toEqual([
      {
        name: 'uq_route_definition_mainTableId',
        logicalColumns: ['mainTable'],
        physicalColumns: ['mainTableId'],
      },
    ]);
  });

  it('appends id tie-breakers to non-unique physical indexes once', () => {
    expect(buildSqlIndexContracts('route_definition', table)).toEqual([
      {
        name: 'idx_route_definition_mainTableId',
        logicalColumns: ['mainTable'],
        physicalColumns: ['mainTableId', 'id'],
        source: 'metadata',
      },
      {
        name: 'idx_route_definition_scheduledAt',
        logicalColumns: ['scheduledAt'],
        physicalColumns: ['scheduledAt', 'id'],
        source: 'metadata',
      },
      {
        name: 'idx_route_definition_createdAt',
        logicalColumns: ['createdAt'],
        physicalColumns: ['createdAt', 'id'],
        source: 'system-timestamp',
      },
      {
        name: 'idx_route_definition_updatedAt',
        logicalColumns: ['updatedAt'],
        physicalColumns: ['updatedAt', 'id'],
        source: 'system-timestamp',
      },
    ]);
  });

  it('centralizes junction table names, indexes, and FK actions', () => {
    const contract = buildSqlJunctionTableContract({
      tableName: 'route_definition_availableMethods_method_definition',
      sourceTable: 'route_definition',
      targetTable: 'method_definition',
      sourceColumn: 'routeDefinitionId',
      targetColumn: 'methodDefinitionId',
      sourcePropertyName: 'availableMethods',
    });

    expect(contract).toMatchObject({
      primaryKeyName: 'route_definition_availableMethods_method_definition_pk',
      sourceIndexName: 'route_definition_availableMethods_src_idx',
      targetIndexName: 'route_definition_availableMethods_tgt_idx',
      reverseIndexName: 'route_definition_availableMethods_rev_idx',
      onDelete: 'CASCADE',
      onUpdate: 'CASCADE',
    });
    expect(contract.sourceForeignKeyName).toMatch(/^j_[0-9a-f]{8}_src_fk$/);
    expect(contract.targetForeignKeyName).toMatch(/^j_[0-9a-f]{8}_tgt_fk$/);
  });
});
