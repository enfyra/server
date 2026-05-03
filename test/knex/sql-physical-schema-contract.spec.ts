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
        constraintName: 'route_definition_mainTableId_foreign',
        targetTable: 'table_definition',
        targetColumn: 'id',
        onDelete: 'CASCADE',
        onUpdate: 'CASCADE',
        nullable: false,
      },
    ]);

    expect(
      buildSqlForeignKeyContracts('order_definition', [
        {
          propertyName: 'customer',
          type: 'many-to-one',
          targetTable: 'account_definition',
          foreignKeyColumn: 'customer_uuid',
          referencedColumn: 'uuid',
          constraintName: 'orders_customer_uuid_fkey',
        },
      ] as any),
    ).toEqual([
      {
        tableName: 'order_definition',
        propertyName: 'customer',
        columnName: 'customer_uuid',
        constraintName: 'orders_customer_uuid_fkey',
        targetTable: 'account_definition',
        targetColumn: 'uuid',
        onDelete: 'SET NULL',
        onUpdate: 'CASCADE',
        nullable: true,
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

  it('does not generate FK contracts or relation-FK indexes for inverse one-to-one relations', () => {
    const inverseTable = {
      name: 'room_definition',
      columns: [{ name: 'id', type: 'int', isPrimary: true }],
      relations: [
        {
          propertyName: 'course',
          type: 'one-to-one',
          targetTable: 'course_definition',
          mappedBy: 'room',
          mappedById: 10,
        },
      ],
      uniques: [['course']],
      indexes: [['course']],
    } as any;

    expect(buildSqlForeignKeyContracts('room_definition', inverseTable.relations)).toEqual([]);
    expect(buildSqlUniqueContracts('room_definition', inverseTable)).toEqual([
      {
        name: 'uq_room_definition_course',
        logicalColumns: ['course'],
        physicalColumns: ['course'],
      },
    ]);
    expect(
      buildSqlIndexContracts('room_definition', inverseTable).filter(
        (idx) => idx.source === 'relation-fk',
      ),
    ).toEqual([]);
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
      sourceIndexName: 'idx_route_definition_availableMethods_src',
      targetIndexName: 'idx_route_definition_availableMethods_tgt',
      reverseIndexName: 'idx_route_definition_availableMethods_rev',
      onDelete: 'CASCADE',
      onUpdate: 'CASCADE',
    });
    expect(contract.sourceForeignKeyName).toMatch(/^j_[0-9a-f]{8}_src_fk$/);
    expect(contract.targetForeignKeyName).toMatch(/^j_[0-9a-f]{8}_tgt_fk$/);
  });

  it('keeps generated junction identifiers short for long table names', () => {
    const contract = buildSqlJunctionTableContract({
      tableName: 'j_7f2d405c_e2e_flow_c_students_e2e_flow_s',
      sourceTable: 'e2e_flow_course_1777787795190',
      targetTable: 'e2e_flow_student_1777787795190',
      sourceColumn: 'sourceId',
      targetColumn: 'targetId',
      sourcePropertyName: 'students',
    });

    expect(contract.sourceIndexName.length).toBeLessThanOrEqual(63);
    expect(contract.targetIndexName.length).toBeLessThanOrEqual(63);
    expect(contract.reverseIndexName.length).toBeLessThanOrEqual(63);
  });
});
